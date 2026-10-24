use super::*;
mod own832_diagnostics;
#[cfg(test)]
mod native_retirement_479;
#[cfg(test)]
mod own823c_tests;
#[cfg(test)]
use harness_contract::agent::AgenticExecutionFocus;
use harness_contract::agent_action::{
    AgentActorBinding, AgentActorKind, AgentInviteInput, ArtifactCommitInput, TaskClaimInput,
    TaskOneShotDeliverable, TaskPublishInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput,
    TeamCreateInput,
};

#[cfg(test)]
pub(crate) mod diagnostic_1120 {
    use serde_json::{json, Value};
    use std::{
        cell::RefCell,
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
    };
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    static FAILED: AtomicBool = AtomicBool::new(false);
    thread_local! {
        static CONTEXT: RefCell<Vec<Value>> = const { RefCell::new(Vec::new()) };
        static DIRECTORY: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }
    pub(crate) fn record(kind: &str, payload: Value) {
        let directory = DIRECTORY
            .with(|slot| slot.borrow().clone())
            .or_else(|| std::env::var_os("COWD_DIAG_1120_DIR").map(PathBuf::from));
        let Some(directory) = directory else {
            return;
        };
        let result = (|| -> std::io::Result<()> {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if directory.symlink_metadata()?.file_type().is_symlink()
                    || directory.metadata()?.permissions().mode() & 0o777 != 0o700
                {
                    return Err(std::io::Error::other("observer directory is not private"));
                }
            }
            let sequence = SEQUENCE.fetch_add(1, Ordering::SeqCst);
            let context = CONTEXT
                .try_with(|slot| slot.borrow().last().cloned())
                .ok()
                .flatten();
            let mut event = json!({"schema":1,"pid":std::process::id(),"sequence":sequence,
                "kind":kind,"context":context,"payload":payload});
            let mut bytes = serde_json::to_vec(&event)?;
            if bytes.len() > 65536 {
                event["payload"] = json!({"oversize":true,"bytes":bytes.len(),
                    "digest":crate::agentic::review_evidence::proof_value_digest(&event["payload"])});
                bytes = serde_json::to_vec(&event)?;
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(directory.join(format!(
                "observer-{}-{sequence:08}.json",
                std::process::id()
            )))?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            std::fs::File::open(directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            FAILED.store(true, Ordering::SeqCst);
            eprintln!("OBS1120_WRITE_FAILURE kind={kind}");
        }
    }
    pub(crate) struct Frame;
    impl Frame {
        pub(crate) fn enter(mut context: Value) -> Self {
            context["call_id"] = json!(SEQUENCE.fetch_add(1, Ordering::SeqCst));
            CONTEXT.with(|slot| slot.borrow_mut().push(context));
            record("validator.enter", json!({}));
            Self
        }
    }
    impl Drop for Frame {
        fn drop(&mut self) {
            record(
                "validator.exit",
                json!({"unwinding":std::thread::panicking()}),
            );
            let _ = CONTEXT.try_with(|slot| slot.borrow_mut().pop());
        }
    }
    pub(crate) struct Boundary {
        stage: &'static str,
        identity: Value,
        ended: bool,
    }
    impl Boundary {
        pub(crate) fn enter(stage: &'static str, identity: Value) -> Self {
            record("cleanup.begin", json!({"stage":stage,"identity":identity}));
            Self {
                stage,
                identity,
                ended: false,
            }
        }
        pub(crate) fn end(mut self) {
            self.ended = true;
            record(
                "cleanup.end",
                json!({"stage":self.stage,"identity":self.identity}),
            );
        }
    }
    impl Drop for Boundary {
        fn drop(&mut self) {
            if !self.ended {
                record(
                    "cleanup.dropped",
                    json!({"stage":self.stage,"identity":self.identity,
                    "unwinding":std::thread::panicking()}),
                );
            }
        }
    }
    // Explicit diagnostic admission gate: never a default-suite success with
    // missing external evidence. The owning runner executes this exact test
    // with --ignored and verifies the files after this process has exited.
    #[test]
    #[ignore = "requires the diagnostic owner's private COWD_DIAG_1120_DIR"]
    fn external_records_survive_original_scope_unwind() {
        let directory = std::env::var_os("COWD_DIAG_1120_DIR")
            .map(PathBuf::from)
            .expect("diagnostic admission supplies a private collector");
        DIRECTORY.with(|slot| *slot.borrow_mut() = Some(directory.clone()));
        let caught = std::panic::catch_unwind(|| {
            let _frame = Frame::enter(json!({"callsite":"observer_contract","identity":"Applied",
                "action_id":"independent-literal-action"}));
            let _boundary = Boundary::enter("contract_unwind", json!({"run":"original-run"}));
            record(
                "contract.before_panic",
                json!({"expected":"durable before unwind"}),
            );
            panic!("1120 controlled observer unwind only");
        });
        DIRECTORY.with(|slot| *slot.borrow_mut() = None);
        Boundary::enter("contract_return", json!({"run":"original-return"})).end();
        assert!(caught.is_err());
        assert!(!FAILED.load(Ordering::SeqCst));
        let events = std::fs::read_dir(directory)
            .unwrap()
            .map(|p| {
                serde_json::from_slice::<Value>(&std::fs::read(p.unwrap().path()).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        assert!(events.iter().any(|e| e["kind"] == "contract.before_panic"));
        assert!(events.iter().any(|e| e["kind"] == "cleanup.dropped"
            && e["payload"]["unwinding"] == true
            && e["context"]["action_id"] == "independent-literal-action"));
        assert!(events
            .iter()
            .any(|e| e["kind"] == "validator.exit" && e["payload"]["unwinding"] == true));
    }
}

pub(super) fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            objective_id: "objective-dispatch".to_string(),
            program_id: "program-dispatch".to_string(),
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            root_execution_id: None,
            // No user topology constraint in this fixture; the ceiling must
            // therefore not restrict the teams these tests build.
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "dispatch objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: "root:session-dispatch".to_string(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        },
        expected_revision: None,
        action,
    }
}

pub(super) fn agent(
    action_id: &str,
    team_id: &str,
    agent_id: &str,
    execution_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.actor_id = agent_id.to_string();
    envelope.actor.kind = AgentActorKind::Agent;
    envelope.actor.execution_id = Some(execution_id.to_string());
    envelope.actor.team_id = Some(team_id.to_string());
    envelope.actor.agent_id = Some(agent_id.to_string());
    envelope
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DelegatedEffectCase {
    None,
    Review,
    PendingReview,
    Withdraw,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PhysicalDrainTiming {
    BeforeGoal,
    AfterGoal,
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct NativeTaskReviewCase {
    pub objective: Option<String>,
    pub acceptance: Option<String>,
    pub criterion_source: harness_contract::agent_action::CriterionSource,
    pub unresolved: Vec<String>,
    pub complete_goal: bool,
    pub commit_report: bool,
}

#[cfg(test)]
type NativeReviewFuture = std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>;

#[cfg(test)]
tokio::task_local! {
    static NATIVE_REVIEW_ASYNC_PROBE: std::cell::RefCell<Option<Box<dyn FnOnce(
        Arc<RuntimeServices>, AgentActionEnvelope, crate::agentic::review_evidence::IndependentReview,
    ) -> NativeReviewFuture + Send>>>;
    static NATIVE_TASK_SETUP: std::cell::RefCell<Option<Box<dyn FnOnce(
        Arc<RuntimeServices>, AgentActionEnvelope, AgentActionEnvelope, TaskSubmitInput,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskSubmitInput> + Send + 'static>> + Send>>>;
    static NATIVE_TASK_CASE: NativeTaskReviewCase;
    static NATIVE_TASK_ASYNC_PROBE: std::cell::RefCell<Option<Box<dyn FnOnce(
        Arc<RuntimeServices>, AgentActionEnvelope, crate::agentic::review_evidence::IndependentReview,
    ) -> NativeReviewFuture + Send>>>;
    static NATIVE_PROBE_HOST: Arc<ReviewReadHost>;
    static NATIVE_REWORK_WIRE: NativeReworkWire;
    static NATIVE_A_PROBE: std::cell::RefCell<Option<Box<dyn FnOnce(
        &Arc<RuntimeServices>, &crate::AgenticProgramProjection,
        crate::execution_core::goal::PreparedProgramConclusion, &AgentActionEnvelope,
    ) + Send>>>;
}

#[cfg(test)]
pub(crate) async fn with_native_goal_before_a(
    probe: impl FnOnce(
            &Arc<RuntimeServices>,
            &crate::AgenticProgramProjection,
            crate::execution_core::goal::PreparedProgramConclusion,
            &AgentActionEnvelope,
        ) + Send
        + 'static,
) {
    run_native_a_probe(R1ProofScenario::Mechanical, probe).await;
}

#[cfg(test)]
pub(crate) async fn with_native_approval_before_a(
    probe: impl FnOnce(
            &Arc<RuntimeServices>,
            &crate::AgenticProgramProjection,
            crate::execution_core::goal::PreparedProgramConclusion,
            &AgentActionEnvelope,
        ) + Send
        + 'static,
) {
    run_native_a_probe(R1ProofScenario::MechanicalApproval, probe).await;
}

#[cfg(test)]
pub(crate) fn with_native_conclusion_append_hook<Output>(
    hook: impl FnOnce(&crate::AppendTransactionRequest) + 'static,
    commit: impl FnOnce() -> Output,
) -> Output {
    crate::agentic::action_service::with_conclusion_append_hook(hook, commit)
}

#[cfg(test)]
pub(crate) async fn with_native_cross_team_before_a(
    probe: impl FnOnce(
            &Arc<RuntimeServices>,
            &crate::AgenticProgramProjection,
            crate::execution_core::goal::PreparedProgramConclusion,
            &AgentActionEnvelope,
        ) + Send
        + 'static,
) {
    run_native_a_probe(R1ProofScenario::MechanicalCrossTeam, probe).await;
}

#[cfg(test)]
pub(crate) async fn with_native_extra_before_a(
    probe: impl FnOnce(
            &Arc<RuntimeServices>,
            &crate::AgenticProgramProjection,
            crate::execution_core::goal::PreparedProgramConclusion,
            &AgentActionEnvelope,
        ) + Send
        + 'static,
) {
    run_native_a_probe(R1ProofScenario::MechanicalExtra, probe).await;
}

#[cfg(test)]
async fn run_native_a_probe(
    scenario: R1ProofScenario,
    probe: impl FnOnce(
            &Arc<RuntimeServices>,
            &crate::AgenticProgramProjection,
            crate::execution_core::goal::PreparedProgramConclusion,
            &AgentActionEnvelope,
        ) + Send
        + 'static,
) {
    NATIVE_A_PROBE
        .scope(std::cell::RefCell::new(Some(Box::new(probe))), async {
            let host = Arc::new(ReviewReadHost::default());
            let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
            Box::pin(exercise_reviewed_goal_proof_scenario(
                services,
                host,
                false,
                false,
                DelegatedEffectCase::Review,
                false,
                PhysicalDrainTiming::BeforeGoal,
                scenario,
            ))
            .await;
        })
        .await;
}

#[cfg(test)]
pub(crate) async fn with_native_task_review_async(
    case: NativeTaskReviewCase,
    probe: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            crate::agentic::review_evidence::IndependentReview,
        ) -> NativeReviewFuture
        + Send
        + 'static,
) {
    with_native_task_review_setup(case, |_, _, _, input| Box::pin(async move { input }), probe)
        .await;
}

#[cfg(test)]
pub(crate) async fn with_native_task_review_setup(
    case: NativeTaskReviewCase,
    setup: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            AgentActionEnvelope,
            TaskSubmitInput,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TaskSubmitInput> + Send + 'static>,
        > + Send
        + 'static,
    probe: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            crate::agentic::review_evidence::IndependentReview,
        ) -> NativeReviewFuture
        + Send
        + 'static,
) {
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    let complete_goal = case.complete_goal;
    NATIVE_PROBE_HOST
        .scope(
            host.clone(),
            NATIVE_TASK_CASE.scope(
                case,
                NATIVE_TASK_SETUP.scope(
                    std::cell::RefCell::new(Some(Box::new(setup))),
                    NATIVE_TASK_ASYNC_PROBE.scope(
                        std::cell::RefCell::new(Some(Box::new(probe))),
                        async {
                            Box::pin(exercise_reviewed_goal_proof_scenario(
                                services.clone(),
                                host,
                                false,
                                false,
                                DelegatedEffectCase::Review,
                                false,
                                PhysicalDrainTiming::BeforeGoal,
                                R1ProofScenario::TaskReviewAdmission,
                            ))
                            .await;
                            if complete_goal {
                                let goal = services
                                    .goal_store()
                                    .get("goal:root-agentic-execution")
                                    .unwrap()
                                    .unwrap();
                                let program = services
                                    .agent_action_service()
                                    .project(
                                        &goal
                                            .execution_binding
                                            .as_ref()
                                            .unwrap()
                                            .agentic_program_id,
                                    )
                                    .unwrap();
                                assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
                                assert!(program.completion_request.is_some());
                                assert_eq!(
                                    goal.completion,
                                    harness_contract::goal::GoalCompletion::Satisfied
                                );
                            }
                        },
                    ),
                ),
            ),
        )
        .await;
}

#[cfg(test)]
pub(crate) async fn with_native_review_async(
    probe: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            crate::agentic::review_evidence::IndependentReview,
        ) -> NativeReviewFuture
        + Send
        + 'static,
) {
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    NATIVE_PROBE_HOST
        .scope(
            host.clone(),
            NATIVE_REVIEW_ASYNC_PROBE.scope(
                std::cell::RefCell::new(Some(Box::new(probe))),
                async {
                    Box::pin(exercise_reviewed_goal_proof_scenario(
                        services,
                        host,
                        false,
                        false,
                        DelegatedEffectCase::Review,
                        false,
                        PhysicalDrainTiming::BeforeGoal,
                        R1ProofScenario::ReviewAdmission,
                    ))
                    .await;
                },
            ),
        )
        .await;
}

#[cfg(test)]
pub(crate) fn native_review_activity(
    services: &Arc<RuntimeServices>,
    envelope: &AgentActionEnvelope,
) -> (usize, usize, usize) {
    let calls = NATIVE_PROBE_HOST.with(|host| host.calls.lock().unwrap().len());
    let reads = services
        .event_store()
        .list_stream(&format!("session:{}", envelope.actor.session_id))
        .unwrap()
        .iter()
        .filter(|event| {
            event.kind.contains("read")
                || event.kind.contains("delivery")
                || event.kind == "tool.invocation.completed"
        })
        .count();
    let graph = services
        .graph_state_store()
        .load(envelope.actor.execution_id.as_ref().unwrap())
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(
        &graph
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::AgentTask)
            .unwrap()
            .payload_ref,
    )
    .unwrap();
    let source = format!(
        "execution-agent-receipts:{}:{}:{}",
        packet.graph_id(),
        packet.node_id(),
        packet.attempt
    );
    let process_reads = services
        .event_store()
        .list_stream(&source)
        .unwrap()
        .iter()
        .filter(|event| {
            event.kind == "execution.agent_tool.receipt" && event.payload["effect_kind"] == "read"
        })
        .count();
    (calls, reads, process_reads)
}

#[cfg(test)]
#[derive(Clone)]
struct NativeReworkWire {
    packet: AgentTaskPacket,
    replies: Arc<std::sync::Mutex<std::collections::VecDeque<serde_json::Value>>>,
    requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    pause:
        Arc<std::sync::Mutex<Option<(String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
}

#[cfg(test)]
pub(crate) async fn with_reworked_native_task_review(
    services: Arc<RuntimeServices>,
    rework: AgentActionEnvelope,
    before_submit: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            TaskSubmitInput,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = TaskSubmitInput> + Send + 'static>,
        > + Send
        + 'static,
    probe: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            crate::agentic::review_evidence::IndependentReview,
        ) -> NativeReviewFuture
        + Send
        + 'static,
) {
    let fixture = ReviewedFixtureServices(services.clone());
    let wire = NATIVE_REWORK_WIRE.with(Clone::clone);
    let graph = services
        .graph_state_store()
        .load(rework.actor.execution_id.as_ref().unwrap())
        .unwrap();
    let node = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::AgentTask)
        .unwrap();
    let previous = RealProcessSession {
        services: services.clone(),
        packet: serde_json::from_str(&node.payload_ref).unwrap(),
    };
    previous.finish().await;
    wait_process_graph_terminal(&fixture, &previous).await;
    let task_ref = match &rework.action {
        AgentAction::TaskReview(input) => {
            assert_eq!(input.decision, TaskReviewDecision::Rework);
            input.task_ref.clone()
        }
        _ => unreachable!(),
    };
    let current = services
        .agent_action_service()
        .project(&rework.actor.program_id)
        .unwrap();
    assert_eq!(
        current.tasks[&task_ref].status,
        crate::AgenticTaskStatus::Rework
    );
    assert_eq!(current.tasks[&task_ref].claim_generation, 1);
    let reply = |call: &str, name: &str, input: serde_json::Value, content: Option<&str>| {
        serde_json::json!({
            "role":"assistant", "content":content,
            "tool_calls":[{"index":0,"id":call,"type":"function","function":{"name":name,"arguments":input.to_string()}}],
        })
    };
    let arrived = Arc::new(tokio::sync::Notify::new());
    let resume = Arc::new(tokio::sync::Notify::new());
    *wire.pause.lock().unwrap() = Some((
        "native-round2-submit".into(),
        arrived.clone(),
        resume.clone(),
    ));
    {
        let mut replies = wire.replies.lock().unwrap();
        assert!(replies.is_empty());
        replies.push_back(reply("native-round2-claim", "task_claim",
            serde_json::json!({"task_ref":task_ref,"reason":"Correct the original Task after independent Rework"}), None));
        replies.push_back(reply(
            "native-round2-read",
            "read_file",
            serde_json::json!({"path":"review-effect.txt","complete":true}),
            None,
        ));
        replies.push_back(reply("native-round2-artifact", "artifact_commit", serde_json::json!({
            "content_ref":"current_message_block:0","kind":"report","title":"Corrected report","relates_to":[task_ref],
        }), Some("Corrected report: one verifiable fact per file, independently inspect this new submission. I read the complete review-effect.txt in this new execution.")));
        replies.push_back(reply(
            "native-round2-submit",
            "task_submit",
            serde_json::json!({
                "task_ref":task_ref,"artifact_refs":[],"evidence_refs":[],"unresolved":[],
            }),
            None,
        ));
        replies.push_back(serde_json::json!({"role":"assistant","content":"The corrected original Task report is submitted with no unresolved items."}));
    }
    let dispatch = fixture
        .dispatch_agentic_followups(
            &rework,
            AgenticDispatchContext {
                session_id: rework.actor.session_id.clone(),
                turn_id: rework.actor.turn_id.clone(),
                model_lease: "r1-native-wire".into(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: vec!["workspace:.".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(dispatch.len(), 1);
    let graph = services
        .graph_state_store()
        .load(&dispatch[0].graph_id)
        .unwrap();
    let node = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::AgentTask)
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
    packet.validate_cohort_prompt_package().unwrap();
    let binding = packet.binding.as_ref().unwrap();
    binding.validate().unwrap();
    let original_binding = wire.packet.binding.as_ref().unwrap();
    assert_eq!(binding.definition_ref, original_binding.definition_ref);
    assert_eq!(binding.executor, original_binding.executor);
    assert_eq!(binding.release, original_binding.release);
    assert_eq!(
        binding.effective_capabilities,
        original_binding.effective_capabilities
    );
    assert_eq!(packet.permission_ceiling, wire.packet.permission_ceiling);
    assert_eq!(packet.resource_scopes, wire.packet.resource_scopes);
    assert_eq!(packet.allowed_tools, wire.packet.allowed_tools);
    for tool in ["read_file", "artifact_commit", "task_submit"] {
        assert!(packet.allowed_tools.iter().any(|allowed| allowed == tool));
    }
    assert!(!packet
        .allowed_tools
        .iter()
        .any(|tool| tool == "edit" || tool == "execute_code"));
    let original_graph = services
        .graph_state_store()
        .load(wire.packet.graph_id())
        .unwrap();
    let parent = graph.parent_execution.as_ref().unwrap();
    let original_parent = original_graph.parent_execution.as_ref().unwrap();
    assert_eq!(parent.execution_id, original_parent.execution_id);
    assert_eq!(parent.node_id, original_parent.node_id);
    assert_ne!(packet.graph_id(), wire.packet.graph_id());
    assert_ne!(packet.run_id(), wire.packet.run_id());
    assert_eq!(packet.attempt, 2);
    let scope = packet.agentic_binding.as_ref().unwrap();
    let original_scope = wire.packet.agentic_binding.as_ref().unwrap();
    assert_eq!(scope.program_id, original_scope.program_id);
    assert_eq!(scope.agent_id, original_scope.agent_id);
    assert_eq!(scope.team_id, original_scope.team_id);
    assert_eq!(scope.task_team_id, original_scope.task_team_id);
    assert!(
        matches!(&scope.focus, AgenticExecutionFocus::TaskExecute { task_ref: bound } if bound == &task_ref)
    );
    tokio::time::timeout(std::time::Duration::from_secs(30), arrived.notified())
        .await
        .unwrap();
    let actor = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: packet.graph_id().into(),
                node_id: packet.node_id().into(),
            },
            None,
        )
        .await
        .unwrap();
    let current = services
        .agent_action_service()
        .project(&actor.program_id)
        .unwrap();
    assert_eq!(current.tasks[&task_ref].claim_generation, 2);
    assert_eq!(
        current.tasks[&task_ref].claim_execution_id.as_deref(),
        Some(packet.graph_id())
    );
    let artifacts: Vec<String> = current
        .artifacts
        .iter()
        .filter(|(_, artifact)| {
            artifact.claim_execution_id.as_deref() == Some(packet.graph_id())
                && artifact.claim_generation == Some(2)
                && artifact.relates_to.contains(&task_ref)
                && artifact.committed_by == actor.actor_id
        })
        .map(|(reference, _)| reference.clone())
        .collect();
    assert_eq!(artifacts.len(), 1);
    let input = TaskSubmitInput {
        task_ref: task_ref.clone(),
        artifact_refs: artifacts,
        evidence_refs: Vec::new(),
        unresolved: Vec::new(),
        deliverable: None,
    };
    let worker = AgentActionEnvelope {
        action_id: "native-round2-submit-probe".into(),
        actor,
        expected_revision: None,
        action: AgentAction::TaskSubmit(input.clone()),
    };
    let input = before_submit(services.clone(), worker, input).await;
    wire.replies.lock().unwrap().front_mut().unwrap()["tool_calls"][0]["function"]["arguments"] =
        serde_json::to_string(&input).unwrap().into();
    resume.notify_one();
    wait_admitted_graph_terminal(&services, &packet).await;
    let run = services.agent_runtime().get(packet.agent_id()).unwrap();
    assert_eq!(
        run.backend,
        crate::agent_run_handle::AgentBackendKind::InProcess
    );
    assert_eq!(run.status, harness_contract::agent::AgentStatus::Completed);
    assert_eq!(
        services
            .graph_state_store()
            .load(packet.graph_id())
            .unwrap()
            .node_statuses[packet.node_id()],
        ExecutionNodeStatus::Completed
    );
    assert!(wire.replies.lock().unwrap().is_empty());
    let events = services
        .event_store()
        .list_stream(&format!("session:{}", packet.session_id()))
        .unwrap();
    for (call, tool) in [
        ("native-round2-claim", "task_claim"),
        ("native-round2-read", "read_file"),
        ("native-round2-artifact", "artifact_commit"),
        ("native-round2-submit", "task_submit"),
    ] {
        let completed: Vec<_> = events
            .iter()
            .filter(|event| {
                event.kind == "tool.invocation.completed"
                    && event.payload["tool_call_id"] == call
                    && event.payload["tool_name"] == tool
            })
            .collect();
        assert_eq!(completed.len(), 1);
        assert_eq!(
            completed[0]
                .activity_binding()
                .unwrap()
                .agent_run_id
                .as_deref(),
            Some(packet.run_id())
        );
    }
    let material = services
        .event_store()
        .list_stream(&format!("session:{}", packet.session_id()))
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
        .filter_map(|event| crate::context_evidence::decode_read_delivery(&event.payload).ok())
        .find(|material| material.tool_invocation_id == "native-round2-read")
        .expect("the new Native producer must retain its own complete physical source read");
    assert_native_task_material_wire(&services, &packet, &material, &wire.requests).await;
    let submission = services.event_store().list_stream(&format!("agentic-program:{}", rework.actor.program_id)).unwrap()
        .into_iter().filter_map(|event| serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone()).ok())
        .find(|envelope| envelope.actor.execution_id.as_deref() == Some(packet.graph_id())
            && matches!(&envelope.action, AgentAction::TaskSubmit(input) if input.task_ref == task_ref))
        .expect("the new Native producer must actually submit through its original ingress");
    let current = services
        .agent_action_service()
        .project(&rework.actor.program_id)
        .unwrap();
    assert_eq!(
        current.tasks[&task_ref].status,
        crate::AgenticTaskStatus::Submitted
    );
    assert_eq!(current.tasks[&task_ref].artifact_refs, input.artifact_refs);
    for reference in &input.artifact_refs {
        let artifact = &current.artifacts[reference];
        assert_eq!(artifact.claim_generation, Some(2));
        assert_eq!(
            artifact.claim_execution_id.as_deref(),
            Some(packet.graph_id())
        );
        assert_eq!(artifact.committed_by, submission.actor.actor_id);
    }
    with_current_native_task_review(services, submission, rework, probe).await;
}

#[cfg(test)]
async fn with_current_native_task_review(
    services: Arc<RuntimeServices>,
    submission: AgentActionEnvelope,
    previous_review: AgentActionEnvelope,
    probe: impl FnOnce(
            Arc<RuntimeServices>,
            AgentActionEnvelope,
            crate::agentic::review_evidence::IndependentReview,
        ) -> NativeReviewFuture
        + Send
        + 'static,
) {
    let fixture = ReviewedFixtureServices(services.clone());
    let task_ref = match &submission.action {
        AgentAction::TaskSubmit(input) => input.task_ref.clone(),
        _ => unreachable!(),
    };
    let dispatch = fixture
        .dispatch_agentic_followups(
            &submission,
            AgenticDispatchContext {
                session_id: submission.actor.session_id.clone(),
                turn_id: submission.actor.turn_id.clone(),
                model_lease: "test".into(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: vec!["workspace:.".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(dispatch.len(), 1);
    let verifier = wait_real_process(&fixture, &dispatch[0].graph_id).await;
    verifier.packet.validate_cohort_prompt_package().unwrap();
    verifier
        .packet
        .binding
        .as_ref()
        .unwrap()
        .validate()
        .unwrap();
    assert_eq!(verifier.packet.attempt, 2);
    assert!(
        matches!(&verifier.packet.agentic_binding.as_ref().unwrap().focus,
        AgenticExecutionFocus::TaskReview { task_ref: bound } if bound == &task_ref)
    );
    let current = services
        .agent_action_service()
        .project(&submission.actor.program_id)
        .unwrap();
    assert_eq!(
        current.tasks[&task_ref].status,
        crate::AgenticTaskStatus::Submitted
    );
    let contents: Vec<String> = current.tasks[&task_ref]
        .artifact_refs
        .iter()
        .map(|reference| current.artifacts[reference].content_ref.clone())
        .collect();
    for (position, content) in contents.iter().enumerate() {
        verifier
            .execute_tool(
                &format!("corrected-independent-read:{position}"),
                "evidence_retrieve",
                &serde_json::json!({"evidence_ref":content}).to_string(),
            )
            .await
            .unwrap();
    }
    let actor = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: verifier.packet.graph_id().into(),
                node_id: verifier.packet.node_id().into(),
            },
            None,
        )
        .await
        .unwrap();
    assert_ne!(actor.execution_id, previous_review.actor.execution_id);
    assert_ne!(actor.execution_id, submission.actor.execution_id);
    assert_eq!(current.tasks[&task_ref].claim_generation, 2);
    assert_eq!(current.tasks[&task_ref].review_generation, 1);
    let envelope = AgentActionEnvelope {
        action_id: "corrected-native-review".into(),
        actor,
        expected_revision: None,
        action: AgentAction::TaskReview(TaskReviewInput {
            task_ref: task_ref.clone(),
            decision: TaskReviewDecision::Accept,
            reason: "Independently read the corrected submitted report".into(),
            operator_requirement: None,
            evidence_refs: contents,
        }),
    };
    let donor = services
        .independent_result_review(
            &envelope.actor,
            &current,
            &current.tasks[&task_ref].artifact_refs,
            None,
        )
        .await
        .unwrap();
    assert_eq!(donor.verification.input_set.as_ref().unwrap().version, 2);
    probe(services, envelope, donor).await;
    verifier.finish().await;
    wait_process_graph_terminal(&fixture, &verifier).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum R1ProofScenario {
    BoundedProcess,
    BoundedProcessInputChanged,
    #[cfg(test)]
    Mechanical,
    #[cfg(test)]
    MechanicalApproval,
    #[cfg(test)]
    MechanicalExtra,
    #[cfg(test)]
    MechanicalCrossTeam,
    #[cfg(test)]
    ReviewAdmission,
    #[cfg(test)]
    TaskReviewAdmission,
    #[cfg(test)]
    ProgramPublication(ProgramPublicationCase),
    ReadDelivery(RealProcessProofCase),
    Unchanged,
    UnrelatedNote,
    ExecutionEpoch,
    AcceptancePolicy,
    LegacyReplay(bool),
    MaterialEnumerationRace,
    PartialDependency,
    TransitiveDependency,
    ReviewerInfluence,
    OptionalSharedCheckpoint,
    PhysicalOpaque,
    PhysicalLineage,
    PhysicalAfterA,
    OptionalNotRetiring,
    OptionalSourceRace,
    OptionalForgedB,
    OptionalForeignB,
    OptionalOverlap,
    OptionalMissingIdentity,
    OptionalHardlink,
    OptionalBroad,
    OptionalMixedDomain,
    OptionalForeignChild,
    OptionalWrongChildAttempt,
    OptionalMixedChild,
}

impl R1ProofScenario {
    fn bounded_process(self) -> bool {
        matches!(
            self,
            Self::BoundedProcess | Self::BoundedProcessInputChanged
        )
    }

    fn native_sources(self) -> bool {
        #[cfg(test)]
        if matches!(
            self,
            Self::ProgramPublication(_)
                | Self::Mechanical
                | Self::MechanicalApproval
                | Self::MechanicalExtra
                | Self::MechanicalCrossTeam
                | Self::ReviewAdmission
                | Self::TaskReviewAdmission
        ) {
            return true;
        }
        self == Self::ReadDelivery(RealProcessProofCase::NativeSources)
    }

    fn mechanical_cross_team(self) -> bool {
        #[cfg(test)]
        if self == Self::MechanicalCrossTeam {
            return true;
        }
        false
    }

    fn mechanical_extra(self) -> bool {
        #[cfg(test)]
        if self == Self::MechanicalExtra {
            return true;
        }
        false
    }

    fn mechanical_approval(self) -> bool {
        #[cfg(test)]
        if self == Self::MechanicalApproval {
            return true;
        }
        false
    }

    fn needs_scoped_review(self) -> bool {
        self.native_sources()
            || self.bounded_process()
            || matches!(self, Self::ReadDelivery(_))
            || self.optional_unknown()
            || matches!(
                self,
                Self::MaterialEnumerationRace
                    | Self::PartialDependency
                    | Self::TransitiveDependency
                    | Self::ReviewerInfluence
            )
    }
    fn invalid_child(self) -> bool {
        matches!(
            self,
            Self::OptionalForeignChild | Self::OptionalWrongChildAttempt | Self::OptionalMixedChild
        )
    }
    fn optional_unknown(self) -> bool {
        matches!(
            self,
            Self::OptionalSharedCheckpoint
                | Self::PhysicalOpaque
                | Self::PhysicalAfterA
                | Self::OptionalForeignChild
                | Self::OptionalWrongChildAttempt
                | Self::OptionalMixedChild
                | Self::OptionalNotRetiring
                | Self::OptionalSourceRace
                | Self::OptionalForgedB
                | Self::OptionalForeignB
                | Self::OptionalOverlap
                | Self::OptionalMissingIdentity
                | Self::OptionalHardlink
                | Self::OptionalBroad
                | Self::OptionalMixedDomain
        )
    }
}

fn original_packet_followup_context(
    packet: &AgentTaskPacket,
    parent: &ExecutionParentBinding,
    actor: &AgentActorBinding,
) -> Result<AgenticDispatchContext, String> {
    packet.assignment.validate().map_err(|error| error.to_string())?;
    let turn_id = packet.assignment.execution_identity.turn_id()
        .filter(|turn| !turn.is_empty())
        .ok_or_else(|| "original followup packet has no turn identity".to_string())?;
    let binding = packet.agentic_binding.as_ref()
        .ok_or_else(|| "original followup packet has no Program binding".to_string())?;
    if packet.graph_id() != parent.execution_id || packet.node_id() != parent.node_id
        || actor.kind != AgentActorKind::Agent
        || actor.execution_id.as_deref() != Some(packet.graph_id())
        || actor.session_id != packet.session_id() || actor.turn_id != turn_id
        || actor.program_id != binding.program_id
        || actor.actor_id != binding.agent_id
        || actor.agent_id.as_deref() != Some(binding.agent_id.as_str())
        || actor.team_id.as_deref() != Some(binding.team_id.as_str())
    {
        return Err("original followup packet/parent/actor identity mismatch".into());
    }
    Ok(AgenticDispatchContext {
        session_id: packet.session_id().into(),
        turn_id: turn_id.into(),
        model_lease: packet.model_lease.clone(),
        permission_ceiling: packet.permission_ceiling,
        resource_scopes: packet.resource_scopes.clone(),
    })
}

struct ReviewedFixtureServices(Arc<RuntimeServices>);

impl std::ops::Deref for ReviewedFixtureServices {
    type Target = Arc<RuntimeServices>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReviewedFixtureServices {
    fn submit_agent_action<'fixture>(
        &'fixture self,
        envelope: &'fixture AgentActionEnvelope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<harness_contract::agent_action::AgentActionObservation, String>,
                > + Send
                + 'fixture,
        >,
    > {
        Box::pin(async move {
            let mut envelope = envelope.clone();
            if let AgentAction::AgentInvite(input) = &mut envelope.action {
                if input.definition_ref.is_none() {
                    input.definition_ref = Some(REAL_PROCESS_DEFINITION.into());
                }
            }
            if envelope.actor.kind == AgentActorKind::Agent {
                if let Some(execution_id) = envelope.actor.execution_id.as_deref() {
                    if let Ok(graph) = self.graph_state_store().load(execution_id) {
                        if let Some(node) = graph
                            .nodes
                            .iter()
                            .find(|node| node.kind == ExecutionNodeKind::AgentTask)
                        {
                            let parent = ExecutionParentBinding {
                                execution_id: graph.id.clone(),
                                node_id: node.id.clone(),
                            };
                            if self
                                .0
                                .resolve_agent_action_actor(&parent, None)
                                .await
                                .as_ref()
                                .ok()
                                == Some(&envelope.actor)
                            {
                                let packet: AgentTaskPacket =
                                    serde_json::from_str(&node.payload_ref)
                                        .map_err(|error| error.to_string())?;
                                let busy = self.workspace_root().join(".cowd/fixtures/busy").join(
                                    crate::context_evidence::read_sha256(graph.id.as_bytes())
                                        .trim_start_matches("sha256:"),
                                );
                                let process = packet.binding.as_ref().is_some_and(|binding|
                                    matches!(binding.executor, harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }));
                                if process
                                    && !busy.exists()
                                    && crate::agent_runtime::confirmed_run_exit(
                                        self.event_store(),
                                        &packet,
                                    )?
                                    .is_none()
                                {
                                    return RealProcessSession {
                                        services: self.0.clone(),
                                        packet,
                                    }
                                    .action(&envelope)
                                    .await;
                                }
                            }
                        }
                    }
                }
            }
            self.0.submit_agent_action(&envelope).await
        })
    }

    fn dispatch_agentic_followups<'fixture>(
        &'fixture self,
        envelope: &'fixture AgentActionEnvelope,
        context: AgenticDispatchContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<AgenticDispatchReceipt>, String>>
                + Send
                + 'fixture,
        >,
    > {
        Box::pin(self.0.dispatch_agentic_followups(envelope, context))
    }
}

#[cfg(test)]
pub(crate) async fn exercise_bounded_process_reviewed_goal_283() {
    let workspace = tempfile::tempdir().unwrap();
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services.clone(),
        host,
        false,
        false,
        DelegatedEffectCase::Review,
        false,
        PhysicalDrainTiming::AfterGoal,
        R1ProofScenario::BoundedProcess,
    ))
    .await;
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    assert!(goal.terminal.is_some());
    let program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
    assert!(program.completion_request.is_some());
    services.shutdown_maintenance().await;
}

#[cfg(test)]
pub(crate) async fn exercise_bounded_process_input_changed_before_final_290() {
    let workspace = tempfile::tempdir().unwrap();
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services.clone(),
        host,
        false,
        false,
        DelegatedEffectCase::Review,
        false,
        PhysicalDrainTiming::AfterGoal,
        R1ProofScenario::BoundedProcessInputChanged,
    ))
    .await;
    services
        .reconcile_agentic_completion_request("program-dispatch")
        .unwrap();
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Open
    );
    assert!(goal.terminal.is_none());
    let program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_eq!(program.status, crate::AgenticProgramStatus::Open);
    assert!(program.objective_verdict.is_none());
    services.shutdown_maintenance().await;
}

#[inline(never)]
pub(super) fn exercise_reviewed_goal_delivery_with_drain(
    services: Arc<RuntimeServices>,
    read_host: Arc<ReviewReadHost>,
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
    direct_text: bool,
    drain_timing: PhysicalDrainTiming,
) -> std::pin::Pin<Box<impl std::future::Future<Output = ()>>> {
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services,
        read_host,
        with_disclosure,
        with_effect,
        delegated_case,
        direct_text,
        drain_timing,
        R1ProofScenario::Unchanged,
    ))
}

async fn read_bounded_body_pages_283(
    services: &ReviewedFixtureServices,
    reader: &RealProcessSession,
    call: &str,
    body: &harness_contract::context::ArtifactRef,
    incomplete_review: Option<&AgentActionEnvelope>,
) -> Vec<String> {
    let mut input = serde_json::json!({"reference":body.selector,"limit":8});
    let mut chunks = std::collections::BTreeMap::new();
    let mut references = Vec::new();
    for page in 0..64 {
        let call_id = format!("{call}:{page}");
        let output = reader
            .execute_tool(&call_id, "read_ref", &input.to_string())
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(value["available"], true);
        let material = assert_process_delivery(services, &reader.packet, &call_id);
        assert!(!material.body_omitted);
        assert_eq!(material.acquisition.tool_name, "read_ref");
        assert_eq!(material.acquisition.body.source_ref, body.selector);
        assert_eq!(material.acquisition.body.source_bytes, body.bytes);
        let index = services
            .event_store()
            .event_by_idempotency_key(
                &material.acquisition.index_stream,
                &material.acquisition.index_key,
            )
            .unwrap()
            .unwrap();
        references.push(format!(
            "tool://{}#{}",
            index.stream_id,
            index.payload["sequence"].as_u64().unwrap()
        ));
        for chunk in value["chunks"].as_array().unwrap() {
            assert!(chunks
                .insert(
                    chunk["index"].as_u64().unwrap(),
                    chunk["content"].as_str().unwrap().to_string()
                )
                .is_none());
        }
        if value["next_request"].is_null() {
            assert_eq!(chunks.len() as u64, value["total_chunks"].as_u64().unwrap());
            assert!(chunks.keys().copied().eq(0..chunks.len() as u64));
            let delivered = chunks.into_values().collect::<String>();
            let original = services
                .artifact_store()
                .read(body, &body.visibility_scope, None)
                .await
                .unwrap();
            assert_eq!(delivered.as_bytes(), original.as_slice());
            assert_eq!(
                crate::context_evidence::read_sha256(delivered.as_bytes())
                    .trim_start_matches("sha256:"),
                body.sha256.trim_start_matches("sha256:")
            );
            return references;
        }
        if page == 0 {
            if let Some(envelope) = incomplete_review {
                let denied = services.submit_agent_action(envelope).await.unwrap_err();
                assert!(
                    denied.contains("review_requires_complete_process_body"),
                    "{denied}"
                );
            }
        }
        input = value["next_request"].clone();
        assert_eq!(input["reference"], body.selector);
    }
    panic!("bounded body read exceeded its declared page budget")
}

async fn run_bounded_process_versions_283(
    services: &ReviewedFixtureServices,
    host: &Arc<ReviewReadHost>,
    writer: &RealProcessSession,
) -> Vec<harness_contract::context::ToolExecutionFact> {
    use harness_contract::context::{EvidenceTargetIdentity, ToolExecutionTerminal};
    assert!(writer
        .packet
        .allowed_tools
        .iter()
        .any(|name| name == "execute_code"));
    assert!(writer
        .packet
        .resource_scopes
        .iter()
        .any(|scope| scope == "workspace:." || scope == "read:."));
    let mut runs = Vec::new();
    for (version, value, expected, successful) in
        [(0, 1, 1, true), (1, 2, 1, false), (2, 2, 2, true)]
    {
        let suffix = if version == 0 {
            "b'complete UTF8 stream'"
        } else {
            "bytes([255])"
        };
        for (path, content) in [
            ("bounded_source.py", format!("VALUE = {value}\n")),
            ("bounded_test.py", format!("import os\nfrom bounded_source import VALUE\nassert VALUE == {expected}\nos.write(1, b'proof-283-' * 2500 + {suffix})\n")),
        ] {
            writer.execute_tool(&format!("bounded-edit:{version}:{path}"), "edit",
                &serde_json::json!({"path":path,"content":content}).to_string()).await.unwrap();
            assert_eq!(std::fs::read_to_string(services.workspace_root().join(path)).unwrap(), content);
        }
        let call_id = format!("bounded-execute:{version}");
        let input = serde_json::json!({"language":"python",
            "code":"exec(open('/project/bounded_test.py').read())",
            "input_files":["bounded_source.py","bounded_test.py"],"timeout_ms":5000})
        .to_string();
        writer.approve_bounded_execute_code_848(&call_id, &input);
        writer
            .execute_tool(&call_id, "execute_code", &input)
            .await
            .unwrap();
        let requests = host
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.tool_use_id == call_id && request.tool_name == "execute_code")
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(
            request.parent_execution.as_ref().unwrap().execution_id,
            writer.packet.graph_id()
        );
        assert_eq!(
            request.parent_execution_attempt,
            Some(writer.packet.attempt)
        );
        assert_eq!(
            request
                .step_binding
                .as_ref()
                .unwrap()
                .snapshot
                .identity
                .actor_ref
                .as_deref(),
            Some(writer.packet.agent_id())
        );
        let effect = &request.compiled_invocation.as_ref().unwrap().effect;
        let original = services
            .commit_service()
            .completed_tool_effect(request, effect)
            .unwrap()
            .unwrap();
        let facts = original
            .observed_evidence
            .iter()
            .filter_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution.clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(facts.len(), 1);
        let fact = facts.into_iter().next().unwrap();
        assert!(
            matches!(&fact.terminal, ToolExecutionTerminal::Settled { exit_code:Some(code), closure, .. }
            if (*code == 0) == successful && closure.all_children_reaped && closure.init_reaped
                && closure.network_disabled && closure.project_read_only && closure.status_channel_protected)
        );
        assert!(fact.capture_complete);
        assert_eq!(fact.admission.inputs.len(), 2);
        let source = services
            .artifact_store()
            .read(
                &fact.admission.source_bundle,
                &fact.admission.source_bundle.visibility_scope,
                None,
            )
            .await
            .unwrap();
        assert_eq!(source.len() as u64, fact.admission.source_bundle.bytes);
        let body = services
            .artifact_store()
            .read(&fact.body, &fact.body.visibility_scope, None)
            .await
            .unwrap();
        assert_eq!(body.len() as u64, fact.body.bytes);
        let body_value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if version == 0 {
            use base64::Engine;
            let raw = base64::engine::general_purpose::STANDARD
                .decode(body_value["stdout_base64"].as_str().unwrap())
                .unwrap();
            assert_eq!(body_value["stdout_utf8"].as_str().unwrap().as_bytes(), raw);
        } else if successful {
            assert!(body_value.get("stdout_utf8").is_none());
        }
        if successful {
            assert!(fact.stdout_bytes > 16 * 1024);
        }
        let binding = writer.packet.agentic_binding.as_ref().unwrap();
        let harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref } =
            &binding.focus
        else {
            panic!("bounded producer must retain its actual execution Task");
        };
        let program = services
            .agent_action_service()
            .project(&binding.program_id)
            .unwrap();
        assert_eq!(
            program.tasks[task_ref].status,
            crate::AgenticTaskStatus::Claimed,
            "a terminal exit code cannot submit or accept the producer's Task"
        );
        assert!(program.objective_verdict.is_none());
        let event_count = services
            .event_store()
            .stream_event_count(&format!("execution-effect:{}", request.idempotency_key))
            .unwrap();
        writer
            .execute_tool(&call_id, "execute_code", &input)
            .await
            .unwrap();
        assert_eq!(
            host.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.tool_use_id == call_id)
                .count(),
            1
        );
        assert_eq!(
            services
                .event_store()
                .stream_event_count(&format!("execution-effect:{}", request.idempotency_key))
                .unwrap(),
            event_count
        );
        runs.push(fact);
    }
    assert_ne!(runs[0].admission.inputs, runs[2].admission.inputs);
    runs
}

/// Read the confirmed physical run exit, tolerating the documented
/// stale-view race on a live agent run stream.
///
/// The agent run stream can advance while the exit is read. That is a stale
/// view of a live stream, not an untrusted one, and the product reports it as
/// retryable for exactly that reason. Unwrapping it made whole scenarios
/// fail on a concurrent append that has nothing to do with what they assert.
fn confirmed_run_exit_with_retry(
    store: &crate::RuntimeEventStore,
    packet: &harness_contract::agent::AgentTaskPacket,
) -> Option<(
    crate::agent_runtime::RunExitEvidence,
    Vec<crate::ExpectedStreamRevision>,
)> {
    let mut attempt = 0;
    loop {
        match crate::agent_runtime::confirmed_run_exit(store, packet) {
            Ok(exit) => return exit,
            Err(error)
                if error == "Agent exit source changed during authentication" && attempt < 400 =>
            {
                std::thread::sleep(std::time::Duration::from_millis(5));
                attempt += 1;
            }
            Err(error) => panic!("{error}"),
        }
    }
}

#[inline(never)]
fn boxed_fixture_stage<Make, Stage>(make: Make) -> std::pin::Pin<Box<Stage>>
where
    Make: FnOnce() -> Stage,
    Stage: std::future::Future,
{
    Box::pin(make())
}

pub(super) async fn exercise_reviewed_goal_proof_scenario(
    services: Arc<RuntimeServices>,
    read_host: Arc<ReviewReadHost>,
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
    direct_text: bool,
    drain_timing: PhysicalDrainTiming,
    proof_scenario: R1ProofScenario,
) {
    let services = ReviewedFixtureServices(services);
    let _cleanup = RealFixtureCleanup {
        services: services.0.clone(),
        host: read_host.clone(),
    };
    let probes_final_b = proof_scenario.native_sources()
        || matches!(
            proof_scenario,
            R1ProofScenario::ReadDelivery(RealProcessProofCase::NativeSources)
                | R1ProofScenario::OptionalSharedCheckpoint
                | R1ProofScenario::OptionalForgedB
                | R1ProofScenario::OptionalForeignB
                | R1ProofScenario::PhysicalAfterA
        );
    let fixture_objective_id = if probes_final_b {
        harness_contract::agent_action::root_objective_id("session-dispatch", "turn-dispatch")
    } else {
        "objective-dispatch".to_string()
    };
    let fixture_program_id = if probes_final_b {
        harness_contract::agent_action::program_id_for_objective(&fixture_objective_id)
    } else {
        "program-dispatch".to_string()
    };

    let delegated_effect = delegated_case != DelegatedEffectCase::None;
    let effect_host = Arc::clone(&read_host);
    read_host.services.set(Arc::downgrade(&services)).unwrap();
    install_real_process_fixture(&services, &read_host).await;
    let root_mission_id = "mission:auto:dispatch-regression";
    services
        .mission_runtime()
        .create_mission(root_mission_id, "root Agent-first Program", Vec::new())
        .expect("create non-default root mission");
    std::fs::create_dir_all(services.workspace_root().join("crates/runtime"))
        .expect("create bounded test workspace");
    std::fs::write(
        services.workspace_root().join("crates/runtime/Cargo.toml"),
        "[package]\nname = \"runtime-test\"\n",
    )
    .expect("write bounded test manifest");
    let mut root_graph = ExecutionGraph::new("root Agent-first Program");
    root_graph.id = "root-agentic-execution".to_string();
    root_graph.lineage = Some(ExecutionGraphLineage {
        session_id: "session-dispatch".to_string(),
        turn_id: "turn-dispatch".to_string(),
        root_task_id: "task-root-dispatch".to_string(),
        task_id: "task-root-dispatch".to_string(),
        generation: 1,
    });
    let mut root_node = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "payload:root",
    );
    root_node.id = "root-agentic-execution:model".to_string();
    root_node.idempotency_key = "root-agentic-model".to_string();
    root_graph.nodes.push(root_node);
    services
        .commit_service()
        .register_graph(root_graph)
        .expect("register root graph");
    let bind_root = |mut envelope: AgentActionEnvelope| {
        envelope.actor.objective_id = fixture_objective_id.clone();
        envelope.actor.program_id = fixture_program_id.clone();
        envelope.actor.root_execution_id = Some("root-agentic-execution".to_string());
        if proof_scenario.native_sources() {
            let policy = services
                .session_execution_policy(&envelope.actor.session_id)
                .expect("Native physical fixture requires its installed Session policy");
            assert_eq!(
                policy.autonomy_profile,
                harness_contract::policy::AutonomyProfileId::Autonomous
            );
            assert!(policy.permission_mode.permits(PermissionMode::ReadOnly));
            assert_eq!(
                envelope.actor.permission_ceiling,
                Some(PermissionMode::ReadOnly)
            );
            assert!(envelope.actor.resource_scopes.is_empty());
            envelope.actor.resource_scopes = vec!["workspace:.".into()];
        }
        envelope
    };
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let task_production_stage = boxed_fixture_stage(|| async {
        include!("reviewed_goal_fixture/task_production_stage.rs")
    });
    let Some((
        action_service,
        team_ref,
        task,
        task_ref,
        producer_session,
        packet,
        actor,
        content,
        mut pending_effect,
        delegated_write_content,
        verified_delegated_content,
        writer,
        bounded_runs,
        mut review_evidence_refs,
        artifact,
        reviewer,
        _probe,
    )) = task_production_stage.await
    else {
        return;
    };
    let task_review_stage =
        boxed_fixture_stage(|| async { include!("reviewed_goal_fixture/task_review_stage.rs") });
    let Some((reviewer_session, review_graph, _bridge)) = task_review_stage.await else {
        return;
    };
    let drain_workers = || async {
        services
            .cancel_execution_tree(
                &review_graph.id,
                "mechanical reviewer fixture finished after durable review",
            )
            .await
            .unwrap();
        services
            .cancel_execution_tree(
                packet.graph_id(),
                "mechanical producer fixture finished after durable submission",
            )
            .await
            .unwrap();
        wait_process_graph_terminal(&services, &producer_session).await;
        wait_process_graph_terminal(&services, &reviewer_session).await;
        for (execution_id, node_id) in [
            (packet.graph_id(), packet.node_id()),
            (review_graph.id.as_str(), review_graph.nodes[0].id.as_str()),
        ] {
            let original: AgentTaskPacket = serde_json::from_str(
                &services
                    .graph_state_store()
                    .load(execution_id)
                    .unwrap()
                    .nodes[0]
                    .payload_ref,
            )
            .unwrap();
            assert!(confirmed_run_exit_with_retry(services.event_store(), &original).is_some());
            assert_eq!(
                services
                    .graph_state_store()
                    .load(execution_id)
                    .unwrap()
                    .node_statuses[node_id],
                ExecutionNodeStatus::Cancelled
            );
        }
        if let Some(writer) = writer.as_ref() {
            let target = services.workspace_root().join("review-effect.txt");
            let bytes_before = std::fs::read(&target).unwrap();
            let calls_before = effect_host.calls.lock().unwrap().len();
            let error = writer
                .execute_tool(
                    "late-after-cancel",
                    "edit",
                    r#"{"path":"review-effect.txt","content":"cancelled write must not execute"}"#,
                )
                .await
                .unwrap_err();
            let expected_fence = if proof_scenario == R1ProofScenario::ExecutionEpoch {
                "ProcessJsonl packet policy revision is stale"
            } else {
                "tool_effect_authority"
            };
            assert!(error.to_string().contains(expected_fence), "{error}");
            assert_eq!(std::fs::read(&target).unwrap(), bytes_before);
            assert_eq!(effect_host.calls.lock().unwrap().len(), calls_before);
        }
    };
    if drain_timing == PhysicalDrainTiming::BeforeGoal {
        drain_workers().await;
    }
    use harness_contract::goal::{GoalCompletion, GoalContract};
    let goal_sources_stage =
        boxed_fixture_stage(|| async { include!("reviewed_goal_fixture/goal_sources_stage.rs") });
    let Some((mut scoped_goal_reviewer, mut optional_effect)) = goal_sources_stage.await else {
        return;
    };
    let bind_review = |envelope: AgentActionEnvelope, reviewer: Option<&R1ScopedReviewer>| {
        let mut envelope = bind_root(envelope);
        if let Some(reviewer) = reviewer {
            envelope.actor = reviewer.actor.clone();
        }
        envelope
    };
    let objective_admission_stage = boxed_fixture_stage(|| async {
        include!("reviewed_goal_fixture/objective_admission_stage.rs")
    });
    let Some((
        native_sources,
        reviewed_delivery_refs,
        transitive_inputs,
        bounded_selected_body_receipts,
        obligation_envelope,
        mut raced_material,
        _physical_history,
    )) = objective_admission_stage.await
    else {
        return;
    };
    let objective_commit_stage = boxed_fixture_stage(|| async {
        include!("reviewed_goal_fixture/objective_commit_stage.rs")
    });
    let Some((mut optional_effect,)) = objective_commit_stage.await else {
        return;
    };
    let goal_completion_stage = boxed_fixture_stage(|| async {
        include!("reviewed_goal_fixture/goal_completion_stage.rs")
    });
    let Some(()) = goal_completion_stage.await else {
        return;
    };
    let post_completion_stage = boxed_fixture_stage(|| async {
        include!("reviewed_goal_fixture/post_completion_stage.rs")
    });
    post_completion_stage.await;
}

// Hold only the named Task's producer/reviewer at the existing backend boundary. The
// real GraphRunner starts/cancels it; it never fabricates a model verdict or
// receipt. The test drives its real Process tool/action protocol below.
async fn exercise_legacy_review_replay(
    services: &Arc<RuntimeServices>,
    host: &Arc<ReviewReadHost>,
    unknown_version: bool,
    envelope: AgentActionEnvelope,
) {
    use crate::{
        AgentActionService, AppendTransactionRequest, ExpectedStreamRevision, RuntimeEventInput,
        RuntimeEventRef, RuntimeEventScope, RuntimeTransactionEventInput,
    };
    use harness_contract::agent_action::AgentActionStatus;
    use harness_contract::goal::ObjectiveReviewVerification;
    use std::collections::BTreeMap;

    let store = services.event_store();
    let actions = services.agent_action_service();
    let program = actions.project(&envelope.actor.program_id).unwrap();
    let goal_id = "goal:root-agentic-execution";
    let goal = services.goal_store().get(goal_id).unwrap().unwrap();
    assert!(goal.reviews.is_empty());
    assert!(!program.tasks.is_empty());
    assert!(program
        .tasks
        .values()
        .any(|task| { task.status == crate::AgenticTaskStatus::Accepted }));
    let AgentAction::ObjectiveReview(input) = &envelope.action else {
        panic!("historical fixture requires an Objective review");
    };
    let donor = services
        .independent_result_review(
            &envelope.actor,
            &program,
            &input.result_refs,
            Some(&input.criterion_ref),
        )
        .await
        .unwrap();
    let modern = &donor.verification;
    assert_eq!(modern.input_set.as_ref().unwrap().version, 2);
    assert!(modern.independence_required);
    assert!(modern.result_source_revisions.is_empty());
    assert!(!modern.reads.is_empty());
    assert!(modern
        .reads
        .iter()
        .all(|read| !read.receipt_refs.is_empty()));
    let modern_before = crate::agentic::review_evidence::validate_proof_inputs(
        store,
        &program,
        Some(&goal),
        modern,
        crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(services.artifact_store().as_ref()),
            history: services.session_history_reader().as_deref(),
        },
    )
    .unwrap();
    let historical_work = crate::agentic::review_evidence::work_manifest_digest(&program);
    let historical_effects = crate::agentic::review_evidence::effect_review_snapshot(
        store,
        &program,
        Some(&goal),
        &modern.effect_source_refs,
    )
    .unwrap();
    let mut historical_wire = serde_json::json!({
        "goal_spec_revision": modern.goal_spec_revision,
        "goal_spec_digest": modern.goal_spec_digest,
        "work_manifest_digest": historical_work,
        "effect_manifest_digest": historical_effects.digest,
        "effect_source_refs": modern.effect_source_refs,
        "reviewer_execution_id": modern.reviewer_execution_id,
        "producer_execution_ids": modern.producer_execution_ids,
        "reads": modern.reads,
    });
    assert!(historical_wire.get("input_set").is_none());
    if unknown_version {
        historical_wire = serde_json::to_value(modern).unwrap();
        historical_wire["input_set"]["version"] = serde_json::json!(u32::MAX);
    }
    let historical: ObjectiveReviewVerification =
        serde_json::from_value(historical_wire.clone()).unwrap();
    assert_eq!(historical.input_set.is_none(), !unknown_version);
    if unknown_version {
        assert_eq!(historical.input_set.as_ref().unwrap().version, u32::MAX);
    }
    assert!(historical.independence_required);
    assert_eq!(historical.reads, modern.reads);
    assert_eq!(
        historical.producer_execution_ids,
        modern.producer_execution_ids
    );
    let prepared = services
        .goal_store()
        .prepare_agentic_objective_action(&envelope, &donor.producers, Some(&historical))
        .unwrap();
    let program_stream = format!("agentic-program:{}", envelope.actor.program_id);
    let action_key = format!("agent-action:{}", envelope.action_id);
    assert!(store
        .event_by_idempotency_key(&program_stream, &action_key)
        .unwrap()
        .is_none());
    let mut expected = BTreeMap::new();
    for source in modern_before.sources.into_iter().chain([
        ExpectedStreamRevision {
            stream_id: program_stream.clone(),
            expected_revision: program.revision,
        },
        ExpectedStreamRevision {
            stream_id: prepared.stream_id.clone(),
            expected_revision: prepared.expected_stream_revision,
        },
    ]) {
        if let Some(previous) = expected.insert(source.stream_id, source.expected_revision) {
            assert_eq!(previous, source.expected_revision);
        }
    }
    let refs = [
        ("objective", envelope.actor.objective_id.clone()),
        ("program", envelope.actor.program_id.clone()),
        ("actor", envelope.actor.actor_id.clone()),
        ("session", envelope.actor.session_id.clone()),
        ("turn", envelope.actor.turn_id.clone()),
        (
            "execution_graph",
            envelope.actor.root_execution_id.clone().unwrap(),
        ),
    ]
    .into_iter()
    .map(|(kind, id)| RuntimeEventRef {
        kind: kind.into(),
        id,
    })
    .collect();
    store
        .append_transaction(AppendTransactionRequest {
            transaction_id: format!(
                "agent-action:{}:{}",
                envelope.actor.program_id, envelope.action_id,
            ),
            expected_streams: expected
                .into_iter()
                .map(|(stream_id, expected_revision)| ExpectedStreamRevision {
                    stream_id,
                    expected_revision,
                })
                .collect(),
            events: vec![
                RuntimeTransactionEventInput {
                    event: RuntimeEventInput {
                        stream_id: program_stream.clone(),
                        scope: RuntimeEventScope::Program,
                        kind: "agentic.action_applied".into(),
                        status: Some("applied".into()),
                        actor: Some(envelope.actor.actor_id.clone()),
                        refs,
                        payload: serde_json::json!({
                            "envelope": envelope,
                            "entity_ref": null,
                            "review_verification": historical_wire,
                        }),
                    },
                    idempotency_key: Some(action_key.clone()),
                    schema_version: 1,
                },
                prepared.event,
            ],
        })
        .unwrap();

    let historical_record = store
        .event_by_idempotency_key(&program_stream, &action_key)
        .unwrap()
        .unwrap();
    let decoded: ObjectiveReviewVerification =
        serde_json::from_value(historical_record.payload["review_verification"].clone()).unwrap();
    assert_eq!(decoded, historical);
    assert_eq!(decoded.input_set.is_none(), !unknown_version);
    let cold_goals = crate::execution_core::GoalStore::new(Arc::clone(store));
    let old_goal = cold_goals.get(goal_id).unwrap().unwrap();
    assert_eq!(old_goal.reviews.len(), 1);
    let old_review = &old_goal.reviews[0];
    assert_eq!(old_review.decision, "satisfied");
    assert_eq!(old_review.verification.as_ref(), Some(&historical));
    assert_eq!(old_review.criterion_ref, "reviewed-delivery");
    let first = services.submit_agent_action(&envelope).await.unwrap();
    assert_eq!(first.status, AgentActionStatus::Applied);
    assert!(first.duplicate);

    let note = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"Unrelated historical replay control",
        )
        .await
        .unwrap();
    let mut note_action = root(
        "r1-legacy-unrelated-note",
        AgentAction::ArtifactCommit(ArtifactCommitInput {
            content_ref: note.selector,
            kind: "note".into(),
            title: "Unrelated replay control".into(),
            relates_to: Vec::new(),
        }),
    );
    note_action.actor = envelope.actor.clone();
    let note_receipt = services.submit_agent_action(&note_action).await.unwrap();
    assert_eq!(note_receipt.status, AgentActionStatus::Applied);
    assert!(!note_receipt.duplicate);

    let previous = services
        .session_execution_policy("session-dispatch")
        .unwrap();
    let replacement = harness_contract::policy::SessionExecutionPolicy::from_profile(
        harness_contract::policy::AutonomyProfileId::Cautious,
        previous.revision + 1,
        harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    services
        .session_execution_policy_control("session-dispatch")
        .unwrap()
        .replace(replacement.clone())
        .unwrap();
    assert_ne!(previous.permission_mode, replacement.permission_mode);
    assert_eq!(
        services.session_execution_policy("session-dispatch"),
        Some(replacement)
    );

    let mut session = crate::Session::new();
    session.session_id = "session-dispatch".into();
    let runtime = crate::ConversationRuntime::new(
        session,
        NoReviewProvider,
        RootReviewReader(Arc::clone(host)),
        crate::PermissionPolicy::new(PermissionMode::DangerFullAccess),
        vec!["Inspect the same committed result".into()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(store));
    runtime
        .begin_turn_strategy_with_resource_snapshot("turn-dispatch", "调研外部资料并形成报告", None)
        .unwrap();
    runtime
        .bind_turn_strategy_execution("turn-dispatch", "root-agentic-execution")
        .unwrap();
    let call = crate::conversation::ModelToolCall {
        binding: None,
        id: "r1-legacy-policy-mutation-plan".into(),
        name: "edit".into(),
        input: r#"{"action":"write","path":"review-effect.txt","content":"must not execute"}"#
            .into(),
        depends_on: Vec::new(),
    };
    let request = crate::tool_dispatch::ToolRequest {
        tool_use_id: call.id.clone(),
        tool_name: call.name.clone(),
        input: call.input.clone(),
        depends_on: Vec::new(),
    };
    let invocations = crate::ToolExecutor::prepare_governed_invocations(
        runtime.tool_executor().as_ref(),
        std::slice::from_ref(&request),
    );
    let plan = crate::GovernedToolCompiler
        .compile(
            services.workspace_root(),
            std::slice::from_ref(&request),
            |name, input| {
                invocations.iter().find_map(|invocation| {
                    (invocation.intent.tool_name == name
                        && invocation.intent.normalized_input == *input)
                        .then(|| {
                            (
                                invocation.effect.clone(),
                                invocation.catalog_revision,
                                invocation.descriptor_set_hash.clone(),
                            )
                        })
                })
            },
        )
        .unwrap();
    let decision = runtime
        .retarget_active_turn_strategy_for_governed_plan(&plan, std::slice::from_ref(&call))
        .unwrap();
    assert_eq!(
        decision.pattern(),
        harness_contract::core::ExecutionPattern::Execute
    );
    assert!(decision
        .gates()
        .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
    println!(
        "R1_LEGACY_POLICY_CONTROL {}",
        serde_json::json!({
            "risk":decision.risk(),"modifiers":decision.modifiers(),"gates":decision.gates(),
        })
    );

    let current = actions.project(&envelope.actor.program_id).unwrap();
    assert_ne!(
        crate::agentic::review_evidence::work_manifest_digest(&current),
        historical.work_manifest_digest,
    );
    let modern_after = crate::agentic::review_evidence::proof_input_snapshot(
        store,
        &current,
        Some(&old_goal),
        modern,
        crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(services.artifact_store().as_ref()),
            history: services.session_history_reader().as_deref(),
        },
    )
    .unwrap();
    assert_ne!(
        modern_after.policy_digest,
        modern.input_set.as_ref().unwrap().acceptance_policy_digest,
    );
    assert!(crate::agentic::review_evidence::validate_proof_inputs(
        store,
        &current,
        Some(&old_goal),
        modern,
        crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(services.artifact_store().as_ref()),
            history: services.session_history_reader().as_deref()
        }
    )
    .err()
    .expect("current policy invalidates the modern control")
    .contains("stale v2 proof"));

    let cold_actions = AgentActionService::new(Arc::clone(store));
    let cursor = store.durable_commit_cursor().unwrap();
    let host_calls = host.calls.lock().unwrap().len();
    let program_revision = store.stream_revision(&program_stream).unwrap();
    let goal_projection = serde_json::to_value(cold_goals.projection(goal_id).unwrap()).unwrap();
    let cold = cold_actions.replay_if_applied(&envelope).unwrap().unwrap();
    let cold_internal = cold_actions
        .apply_verified_review(&envelope, &historical)
        .unwrap();
    assert!(cold_internal.duplicate);
    assert_eq!(cold_internal.receipt_id, cold.receipt_id);
    assert_eq!(cold_internal.changed_refs, cold.changed_refs);
    let (left, right) = tokio::join!(
        services.submit_agent_action(&envelope),
        services.submit_agent_action(&envelope),
    );
    for replay in [cold, left.unwrap(), right.unwrap()] {
        assert_eq!(replay.status, AgentActionStatus::Applied);
        assert!(replay.duplicate);
        assert_eq!(replay.receipt_id, first.receipt_id);
        assert_eq!(replay.action_id, envelope.action_id);
        assert_eq!(replay.changed_refs, first.changed_refs);
        assert_eq!(replay.revision, program_revision);
    }
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
    assert_eq!(host.calls.lock().unwrap().len(), host_calls);
    assert_eq!(cold_goals.get(goal_id).unwrap().unwrap(), old_goal);
    assert_eq!(
        serde_json::to_value(cold_goals.projection(goal_id).unwrap()).unwrap(),
        goal_projection,
    );
    assert_eq!(
        store
            .event_by_idempotency_key(&program_stream, &action_key)
            .unwrap()
            .unwrap(),
        historical_record,
    );
    assert_eq!(
        cold_actions.project(&envelope.actor.program_id).unwrap(),
        current
    );
    for conflict_kind in ["actor", "payload"] {
        let mut conflict = envelope.clone();
        if conflict_kind == "actor" {
            conflict.actor.actor_id.push_str(":foreign");
        } else if let AgentAction::ObjectiveReview(input) = &mut conflict.action {
            input.reason.push_str(":different");
        }
        let rejected = cold_actions.replay_if_applied(&conflict).unwrap().unwrap();
        assert_eq!(
            rejected.status,
            AgentActionStatus::Rejected,
            "same identity cannot alias {conflict_kind}"
        );
        assert_eq!(rejected.error.unwrap().code, "action_id_conflict");
    }
    let mut fresh = envelope.clone();
    fresh.action_id.push_str(":new-unsupported");
    let refused = cold_actions
        .apply_verified_review(&fresh, &historical)
        .unwrap();
    assert_eq!(refused.status, AgentActionStatus::Rejected);
    assert_eq!(refused.error.unwrap().code, "unsupported_review_inputs");
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
    assert_eq!(host.calls.lock().unwrap().len(), host_calls);
    assert_eq!(cold_goals.get(goal_id).unwrap().unwrap(), old_goal);
    assert_eq!(
        store.stream_revision(&program_stream).unwrap(),
        program_revision
    );
}

async fn assert_r1_optional_final_b(
    services: &ReviewedFixtureServices,
    host: &Arc<ReviewReadHost>,
    optional: &R1OptionalEffect,
    payload_ref: &str,
    scenario: R1ProofScenario,
) {
    use harness_contract::execution_graph::ExecutionNodeResult;

    let store = services.event_store();
    #[cfg(test)]
    let artifacts = host
        .cold_artifacts
        .get()
        .map(|(root, repository)| {
            Arc::new(
                crate::ArtifactStore::new(
                    root,
                    repository.clone(),
                    crate::ArtifactStoreConfig::default(),
                )
                .unwrap(),
            )
        })
        .unwrap_or_else(|| services.artifact_store().clone());
    #[cfg(not(test))]
    let artifacts = {
        let _ = host;
        services.artifact_store().clone()
    };
    let queries = Arc::new(std::sync::OnceLock::new());
    queries
        .set(services.session_query_port().unwrap())
        .unwrap_or_else(|_| panic!("fresh cold query slot"));
    let commits = crate::execution_core::graph::ExecutionCommitService::new(store.clone())
        .with_artifact_store(artifacts.clone())
        .with_session_query_slot(queries);
    let cold_actions =
        crate::AgentActionService::new(store.clone()).with_artifact_store(artifacts.clone());
    let cold_goals =
        crate::execution_core::GoalStore::new(store.clone()).with_artifact_store(artifacts.clone());
    let goal = cold_goals
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let binding = goal.execution_binding.as_ref().unwrap();
    let objective_id =
        harness_contract::agent_action::root_objective_id(&binding.session_id, &binding.turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    assert_eq!(binding.objective_id, objective_id);
    assert_eq!(binding.agentic_program_id, program_id);
    assert_eq!(optional.program_id, program_id);
    let program = services
        .agent_action_service()
        .project(&program_id)
        .unwrap();
    let verdict = program
        .objective_verdict
        .as_ref()
        .expect("real committed A");
    assert_eq!(cold_actions.project(&program_id).unwrap(), program);
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
    assert_eq!(verdict.goal_id, goal.id);
    assert_eq!(verdict.goal_revision, goal.revision);
    assert_eq!(
        goal.terminal.as_ref().unwrap().terminal_fence,
        verdict.terminal_fence
    );
    assert_eq!(
        program.tasks[&optional.task_ref].status,
        crate::AgenticTaskStatus::CancelRequested
    );
    assert!(program.tasks[&optional.task_ref]
        .pending_retirement
        .is_some());
    assert!(!goal.reviews.is_empty());
    assert!(goal.reviews.iter().all(|review| {
        review
            .verification
            .as_ref()
            .and_then(|proof| proof.input_set.as_ref())
            .is_some_and(|inputs| inputs.version == 2)
    }));
    optional.assert_pending(services);
    let mut held_before = store.list_stream(&optional.index).unwrap();
    let effect_stream = format!("execution-effect:{}", optional.request.idempotency_key);
    let mut effect_before = store.list_stream(&effect_stream).unwrap();
    let program_stream = format!("agentic-program:{program_id}");
    let goal_stream = format!("goal:{}", goal.id);
    let program_events = store.list_stream(&program_stream).unwrap();
    let goal_events = store.list_stream(&goal_stream).unwrap();
    let a_event = store
        .event_by_idempotency_key(
            &program_stream,
            &format!("program-conclusion:{}", verdict.authority_revision),
        )
        .unwrap()
        .or_else(|| {
            store
                .event_by_idempotency_key(
                    &program_stream,
                    &format!("agentic-objective-verdict:{}", verdict.terminal_fence),
                )
                .unwrap()
        })
        .expect("A must have the production commit owner's durable identity");
    assert_eq!(a_event.kind, "agentic.objective_verdict_bound");
    assert_eq!(
        a_event.payload["verdict"],
        serde_json::to_value(verdict).unwrap()
    );
    let mut graph = services
        .graph_state_store()
        .load(&binding.root_execution_id)
        .unwrap();
    let lineage = graph.lineage.as_ref().unwrap();
    assert_eq!(lineage.session_id, binding.session_id);
    assert_eq!(lineage.turn_id, binding.turn_id);
    assert_eq!(
        program.root_execution_id.as_deref(),
        Some(graph.id.as_str())
    );
    let node_id = "root-agentic-execution:model";
    graph = bind_review_fixture_root(services, &commits, graph, node_id);
    let mut proof_sources = crate::agentic::review_evidence::committed_proof_sources(
        store,
        &goal,
        crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(artifacts.as_ref()),
            history: services.session_history_reader().as_deref(),
        },
    )
    .expect("A proof is valid at the B boundary before the controlled change");
    if scenario == R1ProofScenario::PhysicalAfterA {
        optional.hold.release.notify_one();
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if store
                    .list_stream(&optional.index)
                    .unwrap()
                    .iter()
                    .any(|event| {
                        event.kind == "execution.agent_tool.receipt"
                            && event.payload["idempotency_key"] == optional.request.idempotency_key
                    })
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let before = store.stream_revision(&optional.index).unwrap();
        let calls_before = host.calls.lock().unwrap().len();
        let target = services.workspace_root().join("review-effect.txt");
        let bytes_before = std::fs::read(&target).unwrap();
        let receipts_before = store.list_stream(&optional.index).unwrap();
        let added = optional
            .process
            .execute_tool(
                "physical-after-a-real-new-host-write",
                "edit",
                r#"{"path":"review-effect.txt","content":"actual new Host effect after A"}"#,
            )
            .await;
        let error =
            added.expect_err("Verified Program and retiring Task cannot authorize a fresh write");
        assert!(
            error.to_string().contains("tool_effect_authority"),
            "{error}"
        );
        assert_eq!(store.stream_revision(&optional.index).unwrap(), before);
        assert_eq!(store.list_stream(&optional.index).unwrap(), receipts_before);
        assert_eq!(host.calls.lock().unwrap().len(), calls_before);
        assert_eq!(std::fs::read(&target).unwrap(), bytes_before);
        assert!(store
            .list_stream(&optional.index)
            .unwrap()
            .iter()
            .any(|event| event.kind == "execution.agent_tool.receipt"
                && event.payload["idempotency_key"] == optional.request.idempotency_key));
        proof_sources = crate::agentic::review_evidence::committed_proof_sources(
            store,
            &goal,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(artifacts.as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .expect("settled full-effect work must retain a valid relevant A proof");
    }
    if scenario == R1ProofScenario::PhysicalAfterA {
        held_before = store.list_stream(&optional.index).unwrap();
        effect_before = store.list_stream(&effect_stream).unwrap();
    }
    let request_id = "r1-final-b-commit-owner-only";
    let mut terminal = crate::SessionTerminalInput {
        terminal_id: format!("turn-terminal:{request_id}"),
        message_id: format!("assistant:{request_id}"),
        session_id: binding.session_id.clone(),
        execution_id: Some(graph.id.clone()),
        turn_id: Some(binding.turn_id.clone()),
        request_id: Some(request_id.into()),
        session_generation: Some(1),
        input_sequence: Some(1),
        input_claim_owner: Some("r1-b-only-test-owner".into()),
        input_claim_token: Some("r1-b-only-test-token".into()),
        input_claim_revision: Some(1),
        controlled_recovery_claim_fingerprints: Vec::new(),
        payload_ref: payload_ref.to_string(),
    };
    let terminal_stream = format!("session-terminal:{request_id}");
    r1_prepare_fixture_terminal_payload(
        services, &mut terminal, &format!("{request_id}:terminal"),
    );
    let mut terminal_event = crate::RuntimeTransactionEventInput {
        event: crate::RuntimeEventInput {
            stream_id: terminal_stream.clone(),
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".into(),
            status: Some("pending_delivery".into()),
            actor: Some("r1_b_commit_fixture".into()),
            refs: vec![
                crate::RuntimeEventRef {
                    kind: "execution_graph".into(),
                    id: graph.id.clone(),
                },
                crate::RuntimeEventRef {
                    kind: "session".into(),
                    id: binding.session_id.clone(),
                },
            ],
            payload: serde_json::to_value(&terminal).unwrap(),
        },
        idempotency_key: Some(format!("{request_id}:terminal")),
        schema_version: 1,
    };
    assert!(terminal_event
        .event
        .payload
        .get("objective_verdict")
        .is_none());
    let expected_rejection = match scenario {
        R1ProofScenario::OptionalForgedB => {
            terminal_event.event.payload["objective_verdict"] =
                serde_json::json!({"verdict": verdict});
            Some("Session terminal cannot supply its own Objective verdict")
        }
        R1ProofScenario::OptionalForeignB => {
            terminal_event.event.payload["session_id"] = serde_json::json!("foreign-session");
            Some("Session terminal does not match its Graph, Session and Turn")
        }
        R1ProofScenario::OptionalSharedCheckpoint => None,
        R1ProofScenario::PhysicalAfterA => None,
        _ => panic!("unexpected B-only fixture scenario"),
    };
    assert!(store
        .session_terminal(&terminal.terminal_id)
        .unwrap()
        .is_none());
    assert!(store.list_stream(&terminal_stream).unwrap().is_empty());
    let node_stream = format!("{}:node:{node_id}", graph.id);
    let protected = [
        graph.id.clone(),
        node_stream.clone(),
        terminal_stream.clone(),
    ]
    .into_iter()
    .map(|stream| {
        let events = store.list_stream(&stream).unwrap();
        (stream, events)
    })
    .collect::<Vec<_>>();
    let graph_before = graph.clone();
    let result = commits.transition_node(
        &graph,
        node_id,
        ExecutionNodeStatus::Completed,
        Some(ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(payload_ref.to_string()),
            wait_class: None,
            summary: Some("B commit-owner fixture, not a synthesized Session answer".into()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: crate::tool_invocation_now_ms(),
        }),
        vec![terminal_event],
    );
    if let Some(expected) = expected_rejection {
        let error = result
            .err()
            .expect("the actual B owner must reject forged or foreign authority");
        assert!(matches!(
            &error,
            crate::execution_core::graph::ExecutionCommitError::InvalidCommand(_)
        ));
        let reason = error.to_string();
        assert!(reason.contains(expected), "{reason}");
        for (stream, events) in &protected {
            assert_eq!(&store.list_stream(stream).unwrap(), events);
        }
        assert_eq!(
            services
                .graph_state_store()
                .load_current_async(graph.id.clone())
                .await
                .unwrap(),
            graph_before
        );
        assert!(store
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .is_none());
        println!(
            "R1_B_COMMIT_OWNER_REJECTED {}",
            serde_json::json!({
                "a_cursor": a_event.commit_cursor, "reason": reason,
                "half_write": false, "optional_receipt": false,
            })
        );
    } else {
        let receipt = result.expect(
            "original B gate must evaluate the complete held effect, including shared checkpoints",
        );
        let committed = store.list_stream(&terminal_stream).unwrap();
        assert_eq!(committed.len(), 1);
        let published = &committed[0];
        let authority = &published.payload["objective_verdict"];
        assert_eq!(authority["program_id"], program_id);
        assert_eq!(authority["a_commit_cursor"], a_event.commit_cursor);
        assert_eq!(authority["verdict"], serde_json::to_value(verdict).unwrap());
        let digests = crate::agentic::review_evidence::required_review_conditions(&goal)
            .iter()
            .map(|condition| {
                goal.reviews
                    .iter()
                    .rev()
                    .find(|review| &review.criterion_ref == condition)
                    .expect("every required condition must retain its latest review")
            })
            .map(|review| {
                review
                    .verification
                    .as_ref()
                    .and_then(|proof| proof.input_set.as_ref())
                    .expect("every required review must retain its proof input set")
                    .semantic_digest
                    .clone()
            })
            .collect::<std::collections::BTreeSet<_>>();
        let published_digests: Vec<String> =
            serde_json::from_value(authority["proof_input_digests"].clone()).unwrap();
        assert!(
            published_digests.windows(2).all(|pair| pair[0] < pair[1]),
            "B must publish strictly ordered digests without duplicates: {published_digests:?}"
        );
        assert_eq!(
            published_digests,
            digests.into_iter().collect::<Vec<_>>()
        );
        assert!(receipt.transaction.commit_cursor > a_event.commit_cursor);
        assert_eq!(published.commit_cursor, receipt.transaction.commit_cursor);
        assert_eq!(published.transaction_id, receipt.transaction.transaction_id);
        let outbox = store
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .unwrap();
        assert_eq!(outbox.commit_cursor, receipt.transaction.commit_cursor);
        assert_eq!(outbox.payload_ref, terminal.payload_ref);
        assert_eq!(outbox.status, "pending");
        assert!(outbox.materialized_at_ms.is_none());
        assert_eq!(
            receipt.graph.node_statuses[node_id],
            ExecutionNodeStatus::Completed
        );
        for stream in [&graph.id, &node_stream] {
            let events = store.list_stream(stream).unwrap();
            let last = events.last().unwrap();
            assert_eq!(last.commit_cursor, receipt.transaction.commit_cursor);
            assert_eq!(last.transaction_id, receipt.transaction.transaction_id);
        }
        let goal_revision = store.stream_revision(&goal_stream).unwrap();
        for source in proof_sources.into_iter().chain([
            crate::ExpectedStreamRevision {
                stream_id: program_stream.clone(),
                expected_revision: program.revision,
            },
            crate::ExpectedStreamRevision {
                stream_id: goal_stream.clone(),
                expected_revision: goal_revision,
            },
        ]) {
            assert!(receipt.transaction.stream_revisions.iter().any(|fence| {
                fence.stream_id == source.stream_id
                    && fence.expected_revision == source.expected_revision
            }));
        }
        println!(
            "R1_B_COMMIT_OWNER_SATISFIED {}",
            serde_json::json!({
                "a_cursor": a_event.commit_cursor, "b_cursor": receipt.transaction.commit_cursor,
                "optional_receipt": scenario == R1ProofScenario::PhysicalAfterA,
                "new_write_authority_rejected": scenario == R1ProofScenario::PhysicalAfterA,
                "session_materialized": false,
            })
        );
    }
    assert_eq!(store.list_stream(&program_stream).unwrap(), program_events);
    assert_eq!(store.list_stream(&goal_stream).unwrap(), goal_events);
    assert_eq!(
        services
            .agent_action_service()
            .project(&program_id)
            .unwrap(),
        program
    );
    assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
    assert_eq!(store.list_stream(&optional.index).unwrap(), held_before);
    assert_eq!(store.list_stream(&effect_stream).unwrap(), effect_before);
    if scenario != R1ProofScenario::PhysicalAfterA {
        optional.assert_pending(services);
    }
}

async fn publish_r1_material(
    services: &ReviewedFixtureServices,
    actor: &AgentActorBinding,
    action_id: &str,
    bytes: &[u8],
    relates_to: Vec<String>,
) -> (String, String) {
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: Some(format!("{action_id}.txt")),
            },
            bytes,
        )
        .await
        .unwrap();
    let observation = services
        .submit_agent_action(&AgentActionEnvelope {
            action_id: action_id.into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "note".into(),
                title: "Actual derived material".into(),
                relates_to,
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        observation.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{observation:?}"
    );
    (content.selector, observation.changed_refs[0].clone())
}

fn create_r1_transitive_decision(services: &ReviewedFixtureServices) -> String {
    use harness_contract::policy::*;
    let root = services
        .graph_state_store()
        .load("root-agentic-execution")
        .unwrap();
    let mut graph = ExecutionGraph::new("Derived input decision");
    graph.id = "r1-transitive-decision".into();
    graph.parent_execution = Some(ExecutionParentBinding {
        execution_id: root.id.clone(),
        node_id: root
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::InlineModel)
            .expect("approval input must belong to the original root model node")
            .id
            .clone(),
    });
    graph.lineage = root.lineage;
    let node = ExecutionNodeSpec::new(
        ExecutionNodeKind::Approval,
        "approval",
        "actual decision source",
    );
    let reference = crate::execution_core::graph::executors::graph_approval_id(&graph.id, &node.id);
    graph.nodes.push(node);
    services.commit_service().register_graph(graph).unwrap();
    let source = ApprovalSource {
        kind: ApprovalSourceKind::Session,
        session_id: Some("session-dispatch".into()),
        agent_id: None,
        team_id: None,
        mission_id: None,
        resource_ref: None,
        review_ref: None,
        application: None,
    };
    services
        .approval_queue()
        .submit_scoped(
            &reference,
            SubmitApprovalRequest {
                context: ApprovalContext::owned(
                    &source,
                    "derived input decision",
                    "root-agentic-execution",
                ),
                source,
                action: "derived input decision".into(),
                summary: "A real operator decision used by an auxiliary producer".into(),
                risk: harness_contract::core::TaskRisk::Medium,
                domain: ApprovalDomain::Execution,
                blocks_execution: true,
                evidence_refs: Vec::new(),
                timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
            },
        )
        .unwrap();
    services
        .approval_queue()
        .decide_surface_human(
            "operator:test",
            ApprovalDecisionCommand {
                approval_id: reference.clone(),
                approved: false,
                skip: false,
                reason: "An authentic denial is still a decision input".into(),
                scope: ApprovalGrantScope::Once,
                actor: ApprovalDecisionActor {
                    kind: ApprovalDecisionActorKind::Human,
                    actor_id: "authenticated-surface".into(),
                },
                evidence_refs: Vec::new(),
            },
        )
        .unwrap();
    reference
}

struct R1ScopedReviewer {
    actor: AgentActorBinding,
    packet: AgentTaskPacket,
    reader: RealProcessSession,
    task_ref: String,
    reason_ref: String,
}

impl R1ScopedReviewer {
    async fn finish(self, services: &ReviewedFixtureServices) {
        let task_ref = self.task_ref.clone();
        let withdrawn = AgentActionEnvelope {
            action_id: format!("r1-scoped-reviewer-finished:{}", self.packet.graph_id()),
            actor: self.actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: task_ref.clone(),
                reason_ref: self.reason_ref.clone(),
                evidence_refs: vec![self.reason_ref],
            }),
        };
        self.reader.retire_control(&withdrawn, &task_ref).await;
        services
            .dispatch_agentic_followups(
                &withdrawn,
                AgenticDispatchContext {
                    session_id: self.actor.session_id.clone(),
                    turn_id: self.actor.turn_id.clone(),
                    model_lease: "test".into(),
                    permission_ceiling: PermissionMode::ReadOnly,
                    resource_scopes: vec!["workspace:.".into()],
                },
            )
            .await
            .unwrap();
        wait_retired_process(services, &self.packet, &task_ref).await;
        let program = services
            .agent_action_service()
            .project(&self.actor.program_id)
            .unwrap();
        assert_eq!(
            program.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Withdrawn
        );
        assert!(program.tasks[&task_ref].active_attempts.is_empty());
    }
}

async fn start_r1_scoped_goal_reviewer(
    services: &ReviewedFixtureServices,
    root_actor: &AgentActorBinding,
    content_ref: &str,
    namespace: &str,
    read_target: bool,
) -> R1ScopedReviewer {
    start_r1_scoped_goal_worker(
        services,
        root_actor,
        content_ref,
        namespace,
        read_target,
        false,
    )
    .await
}

async fn start_r1_scoped_goal_worker(
    services: &ReviewedFixtureServices,
    root_actor: &AgentActorBinding,
    content_ref: &str,
    namespace: &str,
    read_target: bool,
    can_write: bool,
) -> R1ScopedReviewer {
    let envelope = |id: &str, action: AgentAction| AgentActionEnvelope {
        action_id: format!("{namespace}:{id}"),
        actor: root_actor.clone(),
        expected_revision: None,
        action,
    };
    let team = services
        .submit_agent_action(&envelope(
            "r1-scoped-review-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Independent physical Goal review".into(),
                mission: "Read actual admitted materials".into(),
                objective: None,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        team.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{team:?}"
    );
    let invite = services
        .submit_agent_action(&envelope(
            "r1-scoped-review-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.changed_refs[0].clone(),
                role: "Independent reviewer".into(),
                mission: "Verify the existing committed work without producing a report".into(),
                required_capabilities: if can_write {
                    vec!["read".into(), "write".into()]
                } else {
                    vec!["read".into()]
                },
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        invite.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{invite:?}"
    );
    let publication = envelope(
        "r1-scoped-review-task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team.changed_refs[0].clone(),
            title: "Assess the actual Goal evidence".into(),
            objective: "Independently read the result and actual affected file".into(),
            acceptance: "Submit actual Goal review or refuse; no fabricated report".into(),
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
            required_capabilities: if can_write {
                vec!["read".into(), "write".into()]
            } else {
                vec!["read".into()]
            },
            depends_on: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: harness_contract::agent_action::TaskPurpose::Exploration,
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    );
    let published = services.submit_agent_action(&publication).await.unwrap();
    assert_eq!(
        published.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{published:?}"
    );
    let task_ref = published.changed_refs[0].clone();
    let receipts = services
        .dispatch_agentic_followups(
            &publication,
            AgenticDispatchContext {
                session_id: root_actor.session_id.clone(),
                turn_id: root_actor.turn_id.clone(),
                model_lease: "test".into(),
                permission_ceiling: if can_write {
                    PermissionMode::WorkspaceWrite
                } else {
                    PermissionMode::ReadOnly
                },
                resource_scopes: vec!["workspace:.".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    wait_real_process(services, &receipts[0].graph_id).await;
    let graph = services
        .graph_state_store()
        .load(&receipts[0].graph_id)
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
    let actor = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: graph.id.clone(),
                node_id: graph.nodes[0].id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    let claim = services
        .submit_agent_action(&AgentActionEnvelope {
            action_id: format!("{namespace}:r1-scoped-review-claim"),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_ref.clone(),
                reason: None,
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        claim.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{claim:?}"
    );
    let reader = RealProcessSession::prepare(
        services,
        &packet,
        &crate::agent_model_selector::AgentModelSelection {
            model: "test".into(),
            provider: "test".into(),
            registry_revision: 0,
        },
    )
    .unwrap();
    reader
        .execute_tool(
            "r1-scoped-result-read",
            "evidence_retrieve",
            &serde_json::json!({
                "evidence_ref":content_ref,
            })
            .to_string(),
        )
        .await
        .unwrap();
    if read_target {
        let output = reader
            .execute_tool(
                "r1-scoped-target-read",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        let actual: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(actual["type"], "text");
        assert_eq!(actual["truncated"], false);
        assert_eq!(actual["file"]["startLine"], 1);
        assert_eq!(actual["file"]["numLines"], actual["file"]["totalLines"]);
    }
    R1ScopedReviewer {
        actor,
        packet,
        reader,
        task_ref,
        reason_ref: content_ref.into(),
    }
}

struct R1OptionalEffect {
    program_id: String,
    process: RealProcessSession,
    packet: AgentTaskPacket,
    task_ref: String,
    graph_id: String,
    index: String,
    request: crate::RuntimeToolExecutionRequest,
    effect_status: crate::RuntimeToolExecutionStatus,
    hold: Arc<ReviewWriteHold>,
    worker: tokio::task::JoinHandle<Result<(), String>>,
    retirement: Option<AgentActionEnvelope>,
}

impl R1OptionalEffect {
    fn assert_pending(&self, services: &ReviewedFixtureServices) {
        let events = services.event_store().list_stream(&self.index).unwrap();
        let intent = events
            .iter()
            .find(|event| {
                event.kind == "execution.agent_tool.intent"
                    && event.payload["idempotency_key"] == self.request.idempotency_key
            })
            .expect("actual pending effect intent");
        assert!(!events
            .iter()
            .any(|event| event.kind == "execution.agent_tool.receipt"
                && event.payload["idempotency_key"] == self.request.idempotency_key));
        let primary = services
            .event_store()
            .list_stream(&format!(
                "execution-effect:{}",
                self.request.idempotency_key
            ))
            .unwrap();
        let primary = primary
            .iter()
            .find(|event| event.kind == "execution.effect.intent")
            .unwrap();
        assert_eq!(primary.commit_cursor, intent.commit_cursor);
        assert_eq!(
            primary.payload["resolved_effect_scope"],
            intent.payload["resolved_effect_scope"]
        );
        assert!(!intent.payload["resolved_effect_scope"].is_null());
        let scope: harness_contract::goal::ProofEffectScope =
            serde_json::from_value(intent.payload["resolved_effect_scope"].clone()).unwrap();
        let descriptor = &self.request.compiled_invocation.as_ref().unwrap().effect;
        assert_eq!(scope.version, 2);
        scope.validate_purpose_mapping(descriptor).unwrap();
        assert_eq!(
            primary.payload["effect"],
            serde_json::to_value(descriptor).unwrap()
        );
        assert_eq!(scope.scope_purposes.len(), descriptor.scopes.len());
        if self.effect_status == crate::RuntimeToolExecutionStatus::Failed
            && self.request.tool_name == "edit"
        {
            let input: serde_json::Value = serde_json::from_str(&self.request.input).unwrap();
            let target = input["path"].as_str().unwrap();
            let directory = scope
                .targets
                .iter()
                .find(|target| {
                    target.declared_scope.operation
                        == harness_contract::policy::PermissionOperation::Write
                        && target.declared_scope.target.as_deref() == input["path"].as_str()
                })
                .unwrap();
            assert_eq!(
                directory.path.object_kind,
                harness_contract::context::WorkspaceObjectKind::Directory
            );
            assert!(!directory.exact);
            assert!(!directory.alias_checked);
            assert!(directory.physical_identity.is_none());
            assert_eq!(
                std::fs::read_to_string(
                    services.workspace_root().join(target).join("original.txt")
                )
                .unwrap(),
                "original directory member"
            );
        }
        assert!(
            !self.worker.is_finished(),
            "the physical ToolHost call really remains pending"
        );
    }

    async fn finish(self, services: &ReviewedFixtureServices) {
        self.finish_after_retirement(services, None).await;
    }

    async fn finish_after_retirement(
        self,
        services: &ReviewedFixtureServices,
        retirement: Option<AgentActionEnvelope>,
    ) {
        self.finish_after_receipt(services, retirement, None, || {}).await;
    }

    async fn finish_rejected_child(
        self,
        services: &ReviewedFixtureServices,
        scenario: R1ProofScenario,
    ) {
        assert!(scenario.invalid_child());
        self.finish_after_receipt(services, None, Some(scenario), || {}).await;
    }

    async fn finish_after_receipt<AfterReceipt: FnOnce() + Send + 'static>(
        self,
        services: &ReviewedFixtureServices,
        retirement: Option<AgentActionEnvelope>,
        rejected_child: Option<R1ProofScenario>,
        after_receipt: AfterReceipt,
    ) {
        let retirement = retirement.or_else(|| self.retirement.clone());
        let projection = services
            .agent_action_service()
            .project(&self.program_id)
            .unwrap();
        if projection.status == crate::AgenticProgramStatus::Verified {
            let before_task = serde_json::to_value(&projection.tasks[&self.task_ref]).unwrap();
            for kind in [AgentActorKind::Root, AgentActorKind::Agent] {
                let mut actor = root(
                    "r1-untrusted-retirement",
                    AgentAction::TaskClaim(TaskClaimInput {
                        task_ref: self.task_ref.clone(),
                        reason: None,
                    }),
                )
                .actor;
                actor.objective_id = projection.objective_id.clone();
                actor.program_id = projection.program_id.clone();
                actor.root_execution_id = projection.root_execution_id.clone();
                actor.kind = kind;
                if kind == AgentActorKind::Agent {
                    let binding = self.packet.agentic_binding.as_ref().unwrap();
                    actor.actor_id = binding.agent_id.clone();
                    actor.agent_id = Some(binding.agent_id.clone());
                    actor.team_id = Some(binding.team_id.clone());
                    actor.execution_id = Some(self.graph_id.clone());
                }
                let rejected = services
                    .submit_agent_action(&AgentActionEnvelope {
                        action_id: format!("r1-untrusted-retirement:{kind:?}:{}", self.graph_id),
                        actor,
                        expected_revision: None,
                        action: AgentAction::TaskAttemptFail(
                            harness_contract::agent_action::TaskAttemptFailInput {
                                task_ref: self.task_ref.clone(),
                                execution_id: self.graph_id.clone(),
                                mode: harness_contract::agent_action::AgentAttemptMode::Execute,
                                reason: "A business caller cannot forge Runtime retirement".into(),
                                retryable: true,
                            },
                        ),
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    rejected.status,
                    harness_contract::agent_action::AgentActionStatus::Rejected,
                    "{rejected:?}"
                );
                assert_eq!(rejected.error.as_ref().unwrap().code, "program_terminal");
                self.assert_pending(services);
                assert_eq!(
                    serde_json::to_value(
                        &services
                            .agent_action_service()
                            .project(&self.program_id)
                            .unwrap()
                            .tasks[&self.task_ref]
                    )
                    .unwrap(),
                    before_task
                );
            }
            let premature = services
                .settle_abandoned_agentic_attempt(
                    &self.packet,
                    "the original Runtime callback raced before actual physical settlement",
                )
                .await;
            assert!(
                premature.is_err(),
                "a Supervisor label alone cannot settle a physically pending attempt"
            );
            self.assert_pending(services);
            assert_eq!(
                serde_json::to_value(
                    &services
                        .agent_action_service()
                        .project(&self.program_id)
                        .unwrap()
                        .tasks[&self.task_ref]
                )
                .unwrap(),
                before_task
            );
            println!(
                "R1_RUNTIME_RETIREMENT_NEGATIVES {}",
                serde_json::json!({
                    "task":self.task_ref,"execution":self.graph_id,"root_and_agent_refused":true,
                    "actual_supervisor_before_physical_exit_refused":true,"same_runtime_id_must_remain_retryable":true,
                })
            );
        }
        let receipt_probe =
            services.commit_service().receipt_commit_probe_for_test(
                &self.request, after_receipt,
            );
        self.hold.release.notify_one();
        let completed = tokio::time::timeout(std::time::Duration::from_secs(30), self.worker)
            .await
            .expect("release the actual optional call")
            .expect("optional call task did not panic");
        if self.effect_status == crate::RuntimeToolExecutionStatus::Executed {
            completed.expect("record the actually completed physical effect");
        } else {
            assert!(
                completed.is_err(),
                "cleanup must retain the actual failed Tools operation, not fabricate success"
            );
        }
        let events = services.event_store().list_stream(&self.index).unwrap();
        assert!(events
            .iter()
            .any(|event| event.kind == "execution.agent_tool.receipt"
                && event.payload["idempotency_key"] == self.request.idempotency_key
                && event.payload["outcome"]["status"]
                    == serde_json::to_value(&self.effect_status).unwrap()));
        receipt_probe.require_fired();
        let retirement_projection = services
            .agent_action_service()
            .project(&self.program_id)
            .unwrap();
        let retiring = &retirement_projection.tasks[&self.task_ref];
        assert!(
            retiring.status == crate::AgenticTaskStatus::Withdrawn
                || retiring.status == crate::AgenticTaskStatus::CancelRequested
                    && retiring.pending_retirement.is_some(),
            "the original semantic withdrawal must own optional cancellation: {retiring:?}"
        );
        if let Some(envelope) = retirement.as_ref() {
            let receipts = services
                .dispatch_agentic_followups(
                    envelope,
                    AgenticDispatchContext {
                        session_id: envelope.actor.session_id.clone(),
                        turn_id: envelope.actor.turn_id.clone(),
                        model_lease: "test".into(),
                        permission_ceiling: PermissionMode::ReadOnly,
                        resource_scopes: vec!["workspace:.".into()],
                    },
                )
                .await
                .unwrap();
            assert!(
                receipts.is_empty(),
                "retirement followups must cancel the original run, not admit replacement work"
            );
        }
        if self.effect_status == crate::RuntimeToolExecutionStatus::Failed
            && self.request.tool_name == "edit"
        {
            let input: serde_json::Value = serde_json::from_str(&self.request.input).unwrap();
            assert_eq!(
                std::fs::read_to_string(
                    services
                        .workspace_root()
                        .join(input["path"].as_str().unwrap())
                        .join("original.txt")
                )
                .unwrap(),
                "original directory member"
            );
        }
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let mut pending = vec![self.graph_id.clone()];
                let mut visited = std::collections::BTreeSet::new();
                let mut all_terminal = true;
                while let Some(graph_id) = pending.pop() {
                    if !visited.insert(graph_id.clone()) {
                        continue;
                    }
                    let graph = services.graph_state_store().load(&graph_id).unwrap();
                    all_terminal &= graph
                        .node_statuses
                        .values()
                        .all(|status| status.is_terminal());
                    pending.extend(
                        services
                            .graph_state_store()
                            .child_links(&graph_id)
                            .unwrap()
                            .into_iter()
                            .map(|link| link.child_execution_id),
                    );
                }
                if all_terminal
                    && confirmed_run_exit_with_retry(services.event_store(), &self.packet,)
                    .is_some()
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|elapsed| {
            let mut pending = vec![self.graph_id.clone()];
            let mut visited = std::collections::BTreeSet::new();
            let mut graphs = Vec::new();
            while let Some(graph_id) = pending.pop() {
                if !visited.insert(graph_id.clone()) {
                    continue;
                }
                let graph = services.graph_state_store().load(&graph_id);
                graphs.push(format!("{graph_id}: {graph:?}"));
                match services.graph_state_store().child_links(&graph_id) {
                    Ok(children) => pending.extend(
                        children.into_iter().map(|child| child.child_execution_id),
                    ),
                    Err(error) => graphs.push(format!("child_links {graph_id}: {error}")),
                }
            }
            let exit = crate::agent_runtime::confirmed_run_exit(
                services.event_store(), &self.packet,
            );
            let lifecycle = services.event_store()
                .list_stream(&format!("agent:{}", self.packet.agent_id()));
            panic!(
                "the original Process wait and blocking driver must finish before attempt settlement: {elapsed:?}; {}; exact_exit={exit:?}; graphs={graphs:?}; agent_lifecycle={lifecycle:?}",
                self.process.failure_diagnostic(),
            );
        });
        self.process.wait_exit().await;
        if rejected_child.is_some() || self.effect_status == crate::RuntimeToolExecutionStatus::Failed {
            let before = services.agent_action_service().project(&self.program_id).unwrap();
            let error = services.retry_agentic_run_exit(&self.graph_id).await
                .expect_err("an adversarial original child must remain unsettled after physical reap");
            assert!(error.contains("runtime_retirement_not_settled"), "{error}");
            let reason = match rejected_child {
                Some(R1ProofScenario::OptionalWrongChildAttempt) => "runtime retirement tool child request mismatch",
                Some(R1ProofScenario::OptionalForeignChild | R1ProofScenario::OptionalMixedChild) =>
                    "runtime retirement non-Native conversation child",
                None => "runtime retirement effect receipt is not known settled",
                Some(_) => unreachable!(),
            };
            assert!(error.contains(reason), "{error}");
            let after = services.agent_action_service().project(&self.program_id).unwrap();
            assert_eq!(after.revision, before.revision);
            let task = &after.tasks[&self.task_ref];
            assert_eq!(task.status, crate::AgenticTaskStatus::CancelRequested);
            assert!(task.pending_retirement.is_some());
            assert!(!task.active_attempts.is_empty());
            assert!(after.objective_verdict.is_none());
            assert!(!process_control_directory(services, &self.packet).exists());
            assert!(confirmed_run_exit_with_retry(services.event_store(), &self.packet).is_some());
            println!("R1_REAPED_INVALID_CHILD_REMAINS_UNSETTLED {}", serde_json::json!({
                "task":self.task_ref,"graph":self.graph_id,"reason":reason,
                "physical_exit":true,"task_withdrawn":false,"goal_satisfied":false,
            }));
            return;
        }
        let settled = services.retry_agentic_run_exit(&self.graph_id).await
            .unwrap_or_else(|error| panic!(
                "original Task settlement owner rejected after exact physical exit: {error}; {}",
                self.process.failure_diagnostic(),
            ));
        assert!(settled,
            "original Task settlement owner still awaits exact exit or effects after authenticated exit");
        let program = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                let program = services
                    .agent_action_service()
                    .project(&self.program_id)
                    .unwrap();
                let task = &program.tasks[&self.task_ref];
                let graph = services.graph_state_store().load(&self.graph_id).unwrap();
                if task.status == crate::AgenticTaskStatus::Withdrawn
                    && task.active_attempts.is_empty()
                    && graph
                        .node_statuses
                        .values()
                        .all(|status| status.is_terminal())
                    && confirmed_run_exit_with_retry(services.event_store(), &self.packet,)
                    .is_some()
                {
                    break program;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|elapsed| {
            panic!(
                "original graph observation, Task retirement and physical cancellation must all settle: {elapsed:?}; program={}; task={}; {}",
                self.program_id,
                self.task_ref,
                self.process.failure_diagnostic(),
            );
        });
        assert_eq!(
            program.tasks[&self.task_ref].status,
            crate::AgenticTaskStatus::Withdrawn
        );
        assert!(program.tasks[&self.task_ref].active_attempts.is_empty());
    }
}

#[cfg(test)]
#[test]
fn optional_directory_scope_preserves_broadness_without_root_alias() {
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir(workspace.path().join("directory")).unwrap();
    std::fs::write(workspace.path().join("directory/input.txt"), "original").unwrap();
    let host = tools::ToolHost::builtin("directory-scope-control", workspace.path());
    let lease = host.pin_snapshot();
    let root = serde_json::json!({"action":"write", "path":".", "content":"not an admitted File"});
    let root_effect = lease.describe_effect("edit", &root);
    assert_eq!(
        root_effect.effect_kind,
        harness_contract::tool::ToolEffectKind::Unknown
    );
    assert!(!crate::tool_host::delegated_tool_effect_is_bounded(
        &root_effect
    ));
    let request = crate::bound_tool_batch::scope_admission_tests::bound_request(
        &host,
        "edit",
        serde_json::json!({"action":"write", "path":"directory", "content":"not a regular File"}),
    );
    let descriptor = &request.compiled_invocation.as_ref().unwrap().effect;
    assert_eq!(
        descriptor.effect_kind,
        harness_contract::tool::ToolEffectKind::Write
    );
    assert!(crate::tool_host::delegated_tool_effect_is_bounded(
        descriptor
    ));
    let resolver =
        crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
    let scope = crate::bound_tool_batch::admitted_effect_scope(
        &request,
        descriptor,
        Some(lease),
        &resolver,
    )
    .unwrap();
    scope.validate_purpose_mapping(descriptor).unwrap();
    let target = scope
        .targets
        .iter()
        .find(|target| {
            target.declared_scope.operation == harness_contract::policy::PermissionOperation::Write
                && target.declared_scope.target.as_deref() == Some("directory")
        })
        .unwrap();
    assert_eq!(
        target.path.object_kind,
        harness_contract::context::WorkspaceObjectKind::Directory
    );
    assert!(!target.exact && !target.alias_checked && target.physical_identity.is_none());
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("directory/input.txt")).unwrap(),
        "original"
    );
}

#[cfg(test)]
#[tokio::test]
async fn persistent_scope_positive_control_uses_existing_file_in_independent_zone() {
    use crate::execution_core::graph::{
        ScopeLockError, ScopeLockManager, ScopeLockMode, ScopeLockRequest, ScopedResource,
    };
    use std::time::Duration;
    let workspace = tempfile::tempdir().unwrap();
    for path in ["out/a", "out/disjoint", "independent-probe/control.txt"] {
        let target = workspace.path().join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "original").unwrap();
    }
    let resolver =
        crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
    let manager = ScopeLockManager::persistent(workspace.path().join("locks")).unwrap();
    let request_for = |path: &str| {
        let identity = resolver.resolve_existing(path).unwrap();
        assert!(resolver
            .physical_file_identity_at_admission(&identity)
            .unwrap()
            .is_some());
        ScopeLockRequest {
            scope: ScopedResource::workspace_object(identity),
            mode: ScopeLockMode::Write,
        }
    };
    let held = manager.acquire([request_for("out/a")], None).await.unwrap();
    assert!(matches!(
        manager
            .acquire(
                [request_for("out/disjoint")],
                Some(Duration::from_millis(30))
            )
            .await,
        Err(ScopeLockError::TimedOut { .. })
    ));
    let independent = manager
        .acquire(
            [request_for("independent-probe/control.txt")],
            Some(Duration::from_millis(100)),
        )
        .await
        .unwrap();
    assert!(matches!(
        manager
            .acquire([request_for("out/a")], Some(Duration::from_millis(30)))
            .await,
        Err(ScopeLockError::TimedOut { .. })
    ));
    drop(independent);
    drop(held);
    let successor = manager
        .acquire([request_for("out/a")], Some(Duration::from_millis(100)))
        .await
        .unwrap();
    drop(successor);
    assert_eq!(manager.active_lease_count().unwrap(), 0);
    for path in ["out/a", "out/disjoint", "independent-probe/control.txt"] {
        assert_eq!(
            std::fs::read_to_string(workspace.path().join(path)).unwrap(),
            "original"
        );
    }
}

async fn start_r1_optional_effect(
    services: &ReviewedFixtureServices,
    host: &Arc<ReviewReadHost>,
    root_actor: &AgentActorBinding,
    reason_ref: &str,
    scenario: R1ProofScenario,
) -> R1OptionalEffect {
    let opaque_definition = if scenario == R1ProofScenario::PhysicalOpaque {
        Some(register_opaque_process_variant(services))
    } else {
        None
    };
    let envelope = |id: &str, action: AgentAction| AgentActionEnvelope {
        action_id: id.into(),
        actor: root_actor.clone(),
        expected_revision: None,
        action,
    };
    let team = services
        .submit_agent_action(&envelope(
            "r1-optional-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Additional authorized exploration".into(),
                mission: "An explicitly non-delivery physical operation".into(),
                objective: None,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        team.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let invited = services
        .submit_agent_action(&envelope(
            "r1-optional-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.changed_refs[0].clone(),
                role: "Exploration worker".into(),
                mission: "Use only the admitted private workspace".into(),
                required_capabilities: if scenario == R1ProofScenario::OptionalMixedDomain {
                    vec!["write".into(), "network".into()]
                } else {
                    vec!["write".into()]
                },
                existing_agent_ref: None,
                definition_ref: opaque_definition.clone(),
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        invited.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let publication = envelope(
        "r1-optional-publish",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team.changed_refs[0].clone(),
            title: "Additional operation".into(),
            objective: "Record a bounded workspace observation".into(),
            acceptance: "Retire explicitly; no required business result".into(),
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
            required_capabilities: if scenario == R1ProofScenario::OptionalMixedDomain {
                vec!["write".into(), "network".into()]
            } else {
                vec!["write".into()]
            },
            depends_on: Vec::new(),
            obligation_refs: Vec::new(),
            purpose: harness_contract::agent_action::TaskPurpose::Exploration,
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    );
    let published = services.submit_agent_action(&publication).await.unwrap();
    assert_eq!(
        published.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let task_ref = published.changed_refs[0].clone();
    let receipts = services
        .dispatch_agentic_followups(
            &publication,
            AgenticDispatchContext {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                model_lease: "test".into(),
                permission_ceiling: PermissionMode::DangerFullAccess,
                resource_scopes: vec!["workspace:.".into(), "network:*".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    let process = wait_real_process_definition(
        services,
        &receipts[0].graph_id,
        opaque_definition
            .as_deref()
            .unwrap_or(REAL_PROCESS_DEFINITION),
    )
    .await;
    let graph = services
        .graph_state_store()
        .load(&receipts[0].graph_id)
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
    let actor = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: graph.id.clone(),
                node_id: graph.nodes[0].id.clone(),
            },
            None,
        )
        .await
        .unwrap();
    let claim = services
        .submit_agent_action(&AgentActionEnvelope {
            action_id: "r1-optional-claim".into(),
            actor,
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_ref.clone(),
                reason: None,
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        claim.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let path = if scenario == R1ProofScenario::OptionalOverlap {
        "review-effect.txt"
    } else if scenario == R1ProofScenario::OptionalBroad {
        "r1-optional-directory"
    } else {
        "r1-optional-target.txt"
    };
    let target = services.workspace_root().join(path);
    if scenario == R1ProofScenario::PhysicalOpaque {
        let direct = process.command(serde_json::json!({"direct_probe":{
            "kind":"write", "path":"out/physical-sentinel.txt", "value":"opaque child direct write",
        }})).await.unwrap();
        assert_eq!(direct["ok"], true);
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("out/physical-sentinel.txt"))
                .unwrap(),
            "opaque child direct write"
        );
        assert!(!crate::agent_runtime::authenticated_run_lifecycle(
            services.event_store(),
            &packet
        )
        .unwrap()
        .no_direct_effects());
    }
    if scenario == R1ProofScenario::OptionalHardlink {
        std::fs::hard_link(services.workspace_root().join("review-effect.txt"), &target).unwrap();
    } else if scenario == R1ProofScenario::OptionalBroad {
        std::fs::create_dir(&target).unwrap();
        std::fs::write(target.join("original.txt"), "original directory member").unwrap();
    } else if scenario != R1ProofScenario::OptionalMissingIdentity
        && scenario != R1ProofScenario::OptionalOverlap
    {
        std::fs::write(&target, "the optional target existed at effect admission").unwrap();
    }
    let hold = Arc::new(ReviewWriteHold::default());
    host.held_write.set(Arc::clone(&hold)).unwrap();
    let writer = Arc::new(
        RealProcessSession::prepare(
            services,
            &packet,
            &crate::agent_model_selector::AgentModelSelection {
                model: "test".into(),
                provider: "test".into(),
                registry_revision: 0,
            },
        )
        .unwrap(),
    );
    let effect_tool = if scenario == R1ProofScenario::OptionalMixedDomain {
        "tool_orchestrate"
    } else {
        "edit"
    };
    let write_path = path;
    let write_input = serde_json::json!({
        "action":"write", "path":write_path,
        "content":"an actual optional physical write whose receipt is pending",
    });
    let effect_input = if scenario == R1ProofScenario::OptionalMixedDomain {
        serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":write_input},
            {"id":"network","op":"call","tool":"web_fetch",
                "input":{"url":"https://example.invalid/r1-no-network-dispatched"},"depends_on":["write"]}
        ]})
    } else {
        write_input
    };
    if scenario == R1ProofScenario::OptionalMixedDomain {
        let descriptor = host
            .catalog
            .get()
            .unwrap()
            .pin_snapshot()
            .describe_effect(effect_tool, &effect_input);
        assert!(
            descriptor.scopes.iter().any(|scope|scope.resource == harness_contract::policy::PermissionResource::File)
                && descriptor.scopes.iter().any(|scope|scope.resource == harness_contract::policy::PermissionResource::Network),
            "Original pinned ToolHost cannot authenticate the requested mixed file/network effect; composite resolver scope support needs its owner, not a fabricated fixture scope: {descriptor:?}"
        );
        assert!(
            packet.allowed_tools.iter().any(|tool| tool == effect_tool),
            "Mixed-domain tool must be in the immutable admitted inventory"
        );
    }
    if scenario == R1ProofScenario::OptionalBroad {
        let lease = host.catalog.get().unwrap().pin_snapshot();
        let original_root =
            serde_json::json!({"action":"write", "path":".", "content":"must not execute"});
        let original_root_effect = lease.describe_effect("edit", &original_root);
        assert_eq!(
            original_root_effect.effect_kind,
            harness_contract::tool::ToolEffectKind::Unknown
        );
        assert!(!crate::tool_host::delegated_tool_effect_is_bounded(
            &original_root_effect
        ));
        let calls = host.calls.lock().unwrap().len();
        let original_index = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let original_head = services
            .event_store()
            .stream_revision(&original_index)
            .unwrap();
        let root_denied = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            writer.execute_tool(
                "optional-root-alias-denied",
                "edit",
                &original_root.to_string(),
            ),
        )
        .await
        .expect("the original root alias must be refused before a physical Host wait")
        .unwrap_err();
        assert!(
            root_denied.contains("ProcessJsonl tool authorization denied:")
                || root_denied.contains("outside the Agent network/File operation resource lease"),
            "{root_denied}"
        );
        assert_eq!(host.calls.lock().unwrap().len(), calls);
        assert_eq!(
            services
                .event_store()
                .stream_revision(&original_index)
                .unwrap(),
            original_head
        );
        let directory_effect = lease.describe_effect("edit", &effect_input);
        assert_eq!(
            directory_effect.effect_kind,
            harness_contract::tool::ToolEffectKind::Write
        );
        assert!(crate::tool_host::delegated_tool_effect_is_bounded(
            &directory_effect
        ));
        assert!(directory_effect.scopes.iter().any(|scope| scope.resource
            == harness_contract::policy::PermissionResource::File
            && scope.operation == harness_contract::policy::PermissionOperation::Write
            && scope.target.as_deref() == Some(path)));
    }
    let worker_effect_input = effect_input.clone();
    let mut worker = tokio::spawn(async move {
        writer
            .execute_tool(
                "r1-optional-held-write",
                effect_tool,
                &worker_effect_input.to_string(),
            )
            .await
            .map(|_| ())
    });
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        tokio::select! {
            _ = hold.entered.notified() => {},
            result = &mut worker => panic!("optional write completed before original Host hold: {result:?}"),
        }
    })
        .await
        .unwrap_or_else(|error| {
            let graph = services.graph_state_store().load(packet.graph_id());
            let children = services.graph_state_store().child_links(packet.graph_id());
            panic!("optional effect never reached original Host hold: broad={}; elapsed={error:?}; worker_finished={}; graph={graph:?}; children={children:?}",
                scenario == R1ProofScenario::OptionalBroad, worker.is_finished());
        });
    let requests = host
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|call| {
            call.tool_name == effect_tool
                && call.parent_execution.as_ref().is_some_and(|parent| {
                    parent.execution_id == graph.id && parent.node_id == packet.node_id()
                })
        })
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        requests.len(),
        1,
        "one actual pending ToolHost request for the immutable optional packet"
    );
    let request = requests.into_iter().next().unwrap();
    let effect = host
        .catalog
        .get()
        .unwrap()
        .pin_snapshot()
        .describe_effect(effect_tool, &effect_input);
    assert!(effect.scopes.iter().any(|scope|
        scope.resource == harness_contract::policy::PermissionResource::File
            && scope.operation == harness_contract::policy::PermissionOperation::Write
            && scope.target.as_deref() == Some(".cowd/checkpoints")),
        "held edit retains its shared checkpoint effect; distinct target names do not prove disjointness");
    let actual_input: serde_json::Value = serde_json::from_str(&request.input).unwrap();
    let actual_write = if effect_tool == "tool_orchestrate" {
        &actual_input["steps"][0]["input"]
    } else {
        &actual_input
    };
    assert_eq!(actual_write["path"], write_path);
    assert_eq!(
        actual_write["content"],
        "an actual optional physical write whose receipt is pending"
    );
    println!(
        "R1_ACTUAL_OPTIONAL_REQUEST {}",
        serde_json::json!({
            "caller_id":"r1-optional-held-write","host_id":request.tool_use_id,
            "parent":request.parent_execution,"effect_key":request.idempotency_key,
        })
    );
    let index = format!(
        "execution-agent-receipts:{}:{}:{}",
        packet.graph_id(),
        packet.node_id(),
        packet.attempt
    );
    let retirement = if scenario != R1ProofScenario::OptionalNotRetiring {
        let envelope = envelope(
            "r1-optional-explicit-retirement",
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: task_ref.clone(),
                reason_ref: reason_ref.into(),
                evidence_refs: vec![reason_ref.into()],
            }),
        );
        let observation = services.submit_agent_action(&envelope).await.unwrap();
        assert_eq!(
            observation.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{observation:?}"
        );
        Some(envelope)
    } else {
        None
    };
    let fixture = R1OptionalEffect {
        retirement,
        program_id: root_actor.program_id.clone(),
        process,
        packet,
        task_ref,
        graph_id: graph.id,
        index,
        effect_status: if effect_tool == "tool_orchestrate"
            || scenario == R1ProofScenario::OptionalBroad
        {
            crate::RuntimeToolExecutionStatus::Failed
        } else {
            crate::RuntimeToolExecutionStatus::Executed
        },
        request,
        hold,
        worker,
    };
    fixture.assert_pending(services);
    fixture
}

// Test adapter performs real ArtifactStore reads. Runtime owns authorization,
// tool execution receipts and review decisions; this host never writes a proof.
#[derive(Default)]
pub(super) struct ReviewReadHost {
    #[cfg(test)]
    cold_artifacts:
        std::sync::OnceLock<(std::path::PathBuf, Arc<crate::EphemeralArtifactRepository>)>,
    pub(super) services: std::sync::OnceLock<std::sync::Weak<RuntimeServices>>,
    pub(super) calls: std::sync::Mutex<Vec<crate::RuntimeToolExecutionRequest>>,
    held_write: std::sync::OnceLock<Arc<ReviewWriteHold>>,
    catalog: std::sync::OnceLock<tools::ToolHost>,
    read_hold: std::sync::Mutex<Option<Arc<ReviewWriteHold>>>,
}

#[derive(Debug, Default)]
struct ReviewWriteHold {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    released: std::sync::atomic::AtomicBool,
}
impl ReviewReadHost {
    async fn publish_content(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<String, String> {
        let services = self.services.get().unwrap().upgrade().unwrap();
        let input: harness_contract::content_publication::ArtifactPublishInput =
            serde_json::from_str(&request.input).map_err(|error| error.to_string())?;
        let session = request
            .session_id
            .as_deref()
            .ok_or("publication requires original Session")?;
        let harness_contract::content_publication::ArtifactPublishInput::File {
            path,
            sha256,
            media_type,
        } = input
        else {
            return Err("fixture publication requires an explicitly selected actual file".into());
        };
        let lease = self.catalog.get().unwrap().pin_snapshot();
        let selected = lease
            .path_policy()
            .resolve(&path)
            .map_err(|error| error.to_string())?;
        let root_owned = match &request.parent_execution {
            Some(parent) => services
                .graph_state_store()
                .load(&parent.execution_id)
                .is_ok_and(|graph| {
                    graph.nodes.iter().any(|node| {
                        node.id == parent.node_id && node.kind == ExecutionNodeKind::InlineModel
                    })
                }),
            None => {
                let intent = services
                    .event_store()
                    .event_by_idempotency_key(
                        &format!("execution-effect:{}", request.idempotency_key),
                        &format!(
                            "{}:{}",
                            request.idempotency_key,
                            if request.compiled_invocation.as_ref()
                                .ok_or("root publication requires its original invocation")?
                                .effect.effect_kind == harness_contract::tool::ToolEffectKind::Read
                            {
                                "read-intent"
                            } else {
                                "intent"
                            },
                        ),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("root publication requires original Runtime intent")?;
                let owner: harness_contract::projection::RuntimeActivityBinding =
                    serde_json::from_value(intent.payload["root_effect_owner"]["binding"].clone())
                        .map_err(|error| error.to_string())?;
                let node = owner
                    .node_id
                    .as_deref()
                    .ok_or("root publication has no original node")?;
                let original = services
                    .event_store()
                    .event_by_idempotency_key(
                        &format!("{}:node:{node}", owner.root_execution_id),
                        &format!("{}:{node}:{}", owner.root_execution_id, owner.revision),
                    )
                    .map_err(|error| error.to_string())?
                    .ok_or("root publication original activity missing")?;
                owner.agent_run_id.is_none()
                    && owner.session_id == session
                    && original.activity_binding().as_ref() == Some(&owner)
                    && original.payload["to"] == "running"
                    && original.commit_cursor < intent.commit_cursor
                    && intent.payload["replay"]["parent_execution"].is_null()
                    && intent.payload["root_effect_context"]["execution_id"]
                        == owner.root_execution_id
                    && intent.payload["root_effect_context"]["session_id"] == session
                    && intent.payload["input_sha256"]
                        == crate::context_evidence::read_sha256(request.input.as_bytes())
                    && request
                        .compiled_invocation
                        .as_ref()
                        .is_some_and(|invocation| {
                            intent.payload["effect"]
                                == serde_json::to_value(&invocation.effect).unwrap()
                        })
                    && request.step_binding.as_ref().is_some_and(|binding| {
                        binding.snapshot.identity.session_id == owner.session_id
                            && binding.snapshot.identity.turn_id == owner.turn_id
                            && binding.snapshot.identity.execution_id.as_deref()
                                == Some(owner.root_execution_id.as_str())
                            && intent.payload["replay"]["origin"]
                                == serde_json::to_value(&binding.origin).unwrap()
                            && binding.origin.call_id == request.tool_use_id
                    })
            }
        };
        if !root_owned
            && !request.authorized_scopes.iter().any(|scope| {
                scope.split_once(':').is_some_and(|(kind, root)| {
                    matches!(kind, "read" | "workspace" | "write")
                        && lease
                            .path_policy()
                            .resolve(root)
                            .is_ok_and(|root| selected.starts_with(root))
                })
            })
        {
            return Err("publication file is outside actual delegated scopes".into());
        }
        let bytes = tools::file_ops::snapshot_file(lease.path_policy(), &path, &sha256)
            .map_err(|error| error.to_string())?;
        if bytes.is_empty() {
            return Err("publication requires nonempty authored content".into());
        }
        let artifact = services
            .publish_authorized_content(&bytes, &sha256, &media_type, session)
            .await?;
        Ok(serde_json::json!({
            "content_ref":artifact.selector, "sha256":artifact.sha256, "bytes":artifact.bytes,
            "media_type":artifact.media_type, "read_request":{"evidence_ref":artifact.selector},
            "commit_input":{"content_ref":artifact.selector}, "status":"published",
        })
        .to_string())
    }

    async fn tool_output(&self, name: &str, input: &str, scopes: &[String]) -> String {
        if matches!(name, "evidence_retrieve" | "read_ref") {
            return self.read(input, scopes).await;
        }
        let services = self.services.get().unwrap().upgrade().unwrap();
        let input: serde_json::Value = serde_json::from_str(input).unwrap();
        let path = input["path"].as_str().unwrap();
        let path = if path == "." {
            "r1-optional-target.txt"
        } else {
            path
        };
        assert!(matches!(
            path,
            "review-effect.txt"
                | "unrelated-review.txt"
                | "r1-optional-target.txt"
                | "out/a"
                | "bounded_source.py"
                | "bounded_test.py"
        ));
        let path = services.workspace_root().join(path);
        match name {
            "read_file" => serde_json::to_string(
                &tools::file_ops::read_file(
                    &tools::path_policy::WorkspacePathPolicy::new(services.workspace_root()),
                    path.to_str().unwrap(),
                    input
                        .get("offset")
                        .and_then(serde_json::Value::as_u64)
                        .map(|value| usize::try_from(value).unwrap()),
                    input
                        .get("limit")
                        .and_then(serde_json::Value::as_u64)
                        .map(|value| usize::try_from(value).unwrap()),
                    input
                        .get("complete")
                        .and_then(serde_json::Value::as_bool)
                        .unwrap_or(false),
                )
                .unwrap(),
            )
            .unwrap(),
            _ => panic!("unexpected review tool {name}"),
        }
    }

    async fn read(&self, input: &str, scopes: &[String]) -> String {
        let services = self.services.get().unwrap().upgrade().unwrap();
        let input: serde_json::Value = serde_json::from_str(input).unwrap();
        let selector = input
            .get("evidence_ref")
            .or_else(|| input.get("reference"))
            .and_then(serde_json::Value::as_str)
            .unwrap();
        let artifact = if selector.starts_with("approval:v1:") {
            services
                .approval_result_content("session-dispatch", selector)
                .await
                .unwrap()
                .0
        } else if let Some(id) = selector.strip_prefix("tool://") {
            let access = services
                .session_evidence_access("session-dispatch", id)
                .await
                .unwrap()
                .unwrap();
            services
                .artifact_store()
                .resolve(&access.retrieval_selector)
                .unwrap()
        } else {
            services.artifact_store().resolve(selector).unwrap()
        };
        assert!(scopes.contains(&artifact.visibility_scope));
        let bytes = services
            .artifact_store()
            .read(&artifact, &artifact.visibility_scope, None)
            .await
            .unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let chunks = text.chars().collect::<Vec<_>>().chunks(1500)
            .enumerate().map(|(index, chunk)| serde_json::json!({"index":index,"content":chunk.iter().collect::<String>()})).collect::<Vec<_>>();
        let start = input
            .get("cursor")
            .and_then(serde_json::Value::as_str)
            .map(|cursor| {
                let (digest, start) = cursor
                    .rsplit_once(':')
                    .expect("fixture cursor binds original hash and chunk offset");
                assert_eq!(digest, artifact.sha256);
                start.parse::<usize>().unwrap()
            })
            .unwrap_or(0);
        let limit = input
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(16)
            .clamp(1, 16) as usize;
        assert!(start <= chunks.len());
        let end = start.saturating_add(limit).min(chunks.len());
        let next = (end < chunks.len()).then(|| format!("{}:{end}", artifact.sha256));
        let next_request = next.as_ref().map(|cursor| {
            if input.get("reference").is_some() {
                serde_json::json!({"reference":selector,"cursor":cursor,"limit":limit})
            } else {
                serde_json::json!({"evidence_ref":selector,"cursor":cursor,"limit":limit})
            }
        });
        serde_json::json!({"kind":"evidence_retrieve","available":true,"evidence_ref":selector,
            "sha256":artifact.sha256,"bytes":artifact.bytes,"encoding":"utf8",
            "total_chunks":chunks.len(),"chunks":&chunks[start..end],"coverage":"sequential_content",
            "truncated":end < chunks.len(),"next_cursor":next,"next_request":next_request})
        .to_string()
    }
}
#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for ReviewReadHost {
    fn bounded_process_authority(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<crate::tool_host::BoundedProcessAuthority, crate::ToolError> {
        let services = self
            .services
            .get()
            .and_then(std::sync::Weak::upgrade)
            .ok_or_else(|| crate::ToolError::new("bounded fixture original Runtime unavailable"))?;
        crate::tool_host::BoundedProcessAuthority::bind(services, request.clone())
    }

    fn bounded_process_lease(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, crate::ToolError> {
        let binding = request
            .step_binding
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("bounded fixture requires original Step"))?;
        let step = binding
            .resolve(self.catalog.get().map(tools::ToolHost::pin_snapshot))
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        let input = step
            .validate_call(
                &binding.origin,
                request
                    .session_id
                    .as_deref()
                    .ok_or_else(|| crate::ToolError::new("missing Session"))?,
                &request.tool_use_id,
                &request.tool_name,
                &request.input,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        let authorization = request
            .authorization
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("missing original authorization"))?;
        step.lease()
            .validate_authorization(authorization, &request.tool_name, &input)
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        self.calls.lock().unwrap().push(request.clone());
        Ok(step.lease().clone())
    }

    fn bounded_process_plane(&self) -> Option<Arc<crate::ToolExecutionPlane>> {
        self.services
            .get()?
            .upgrade()
            .map(|services| Arc::clone(services.tool_execution_plane()))
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        self.catalog.get().map(tools::ToolHost::pin_snapshot)
    }
    fn delegated_tool_definitions(&self, names: &[String]) -> Vec<crate::ProviderToolDefinition> {
        self.catalog
            .get()
            .unwrap()
            .pin_snapshot()
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| names.contains(&definition.name))
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect()
    }
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        assert!(request.authorization.is_some());
        self.calls.lock().unwrap().push(request.clone());
        if harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
            .contains(&request.tool_name.as_str())
        {
            let services = self.services.get().unwrap().upgrade().unwrap();
            let parent = request
                .parent_execution
                .as_ref()
                .expect("action has original parent");
            let actor = services.resolve_agent_action_actor(parent, None).await;
            let result =
                match actor {
                    Ok(actor) => {
                        let binding = request
                            .step_binding
                            .as_ref()
                            .expect("Process action has original Step");
                        let call_id = &binding.origin.call_id;
                        let graph = services
                            .graph_state_store()
                            .load(&parent.execution_id)
                            .unwrap();
                        let packet = graph
                            .nodes
                            .iter()
                            .find(|node| {
                                node.id == parent.node_id
                                    && node.kind == ExecutionNodeKind::AgentTask
                            })
                            .and_then(|node| {
                                serde_json::from_str::<AgentTaskPacket>(&node.payload_ref).ok()
                            })
                            .expect("action retains its original AgentTask packet");
                        let path = Some(&packet)
                            .filter(|packet| {
                                matches!(packet.binding.as_ref().map(|binding| &binding.executor),
                            Some(harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }))
                            })
                            .map(|packet| {
                                process_control_directory(&services, packet)
                                    .join("actionmeta")
                                    .join(format!(
                                        "{}.json",
                                        crate::context_evidence::read_sha256(
                                            format!("{}:{call_id}", parent.execution_id).as_bytes(),
                                        )
                                        .trim_start_matches("sha256:")
                                    ))
                            });
                        let metadata: serde_json::Value = path
                            .and_then(|path| std::fs::read(path).ok())
                            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
                            .unwrap_or_default();
                        let action = serde_json::from_value(serde_json::json!({
                        "kind":request.tool_name,
                        "input":serde_json::from_str::<serde_json::Value>(&request.input).unwrap(),
                    })).unwrap();
                        let envelope = AgentActionEnvelope {
                            action_id: metadata["action_id"]
                                .as_str()
                                .unwrap_or(call_id)
                                .to_string(),
                            actor,
                            expected_revision: metadata["expected_revision"].as_u64(),
                            action,
                        };
                        let result = services.submit_agent_action(&envelope).await;
                        if let Ok(observation) = &result {
                            if observation.status
                                == harness_contract::agent_action::AgentActionStatus::Applied
                                && !matches!(&envelope.action, AgentAction::StateInspect(_))
                            {
                                let context = original_packet_followup_context(&packet, parent, &envelope.actor)
                                    .expect("followup requires the same original packet/parent/actor identity");
                                let barrier = services
                                    .resolve_agentic_program_barrier(&envelope.actor.program_id)
                                    .await;
                                let dispatch = services
                                    .dispatch_agentic_followups(&envelope, context.clone())
                                    .await;
                                eprintln!("R1_752_POSTCOMMIT {}", serde_json::json!({
                                    "envelope": &envelope,
                                    "observation": observation,
                                    "parent_execution": parent,
                                    "attempt": packet.attempt,
                                    "barrier": format!("{barrier:?}"),
                                    "dispatch": format!("{dispatch:?}"),
                                }));
                                let retry_services = services.clone();
                                let retry_owner = format!("fixture-postcommit:{}", envelope.action_id);
                                let retry_admitted = services.maintenance_supervisor().submit(retry_owner.clone(), async move {
                                    for delay_ms in [5_000u64, 15_000, 45_000] {
                                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms))
                                            .await;
                                        let dispatch = retry_services
                                            .dispatch_agentic_followups(&envelope, context.clone())
                                            .await;
                                        eprintln!("R1_752_POSTCOMMIT_RETRY {}", serde_json::json!({
                                            "action_id": &envelope.action_id,
                                            "dispatch": format!("{dispatch:?}"),
                                        }));
                                        if matches!(dispatch, Ok(ref admitted) if !admitted.is_empty()) {
                                            break;
                                        }
                                    }
                                }).await;
                                if !retry_admitted {
                                    eprintln!("R1_752_POSTCOMMIT_NOT_ADMITTED owner={retry_owner}");
                                }
                            }
                        }
                        result
                    }
                    Err(error) => Err(error),
                };
            let (status, output, error) = match result {
                Ok(observation) => (
                    crate::RuntimeToolExecutionStatus::Executed,
                    Some(serde_json::to_string(&observation).unwrap()),
                    None,
                ),
                Err(error) => (crate::RuntimeToolExecutionStatus::Failed, None, Some(error)),
            };
            return crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status,
                category: request.category,
                output,
                error,
                evidence_ref: format!("action:{}", request.idempotency_key),
                observed_evidence: vec![],
            };
        }
        if request.tool_name == "artifact_publish" {
            let output = self.publish_content(request).await;
            return crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: if output.is_ok() {
                    crate::RuntimeToolExecutionStatus::Executed
                } else {
                    crate::RuntimeToolExecutionStatus::Failed
                },
                category: request.category,
                output: output.as_ref().ok().cloned(),
                error: output.err(),
                evidence_ref: format!("publication:{}", request.idempotency_key),
                observed_evidence: vec![],
            };
        }
        if !matches!(
            request.tool_name.as_str(),
            "edit" | "read_file" | "read_ref" | "evidence_retrieve" | "tool_orchestrate"
        ) {
            let binding = request
                .step_binding
                .as_ref()
                .expect("catalog tool has original Step");
            let step = binding
                .resolve(self.catalog.get().map(tools::ToolHost::pin_snapshot))
                .unwrap();
            let input = serde_json::from_str(&request.input).unwrap();
            let mut observed_evidence = Vec::new();
            let result = if request.compiled_invocation.as_ref().is_some_and(|invocation| {
                invocation.effect.uses_network && !invocation.effect.spawns_process
            }) {
                match (
                    crate::ExecutionCommitService::local_network_binding(request),
                    request.session_id.as_deref(),
                ) {
                    (Ok(network_binding), Some(session)) => step
                        .lease()
                        .execute_joined_local_network(
                            request.authorization.as_ref().unwrap(),
                            &request.tool_name,
                            &request.input,
                            network_binding,
                            session,
                            &binding.origin,
                        )
                        .await
                        .map(|(output, fact)| {
                            observed_evidence.push(fact.into_observation(
                                request.tool_name.clone(),
                                request.observation_wave_sequence,
                            ));
                            output
                        })
                        .map_err(|error| error.to_string()),
                    (Err(error), _) => Err(error.to_string()),
                    (_, None) => Err("local network original Session missing".into()),
                }
            } else {
                step.lease()
                    .execute_async(
                        request.authorization.as_ref().unwrap(),
                        &request.tool_name,
                        &input,
                    )
                    .await
                    .map_err(|error| error.to_string())
            };
            let (status, output, error) = match result {
                Ok(output) => (
                    crate::RuntimeToolExecutionStatus::Executed,
                    Some(output),
                    None,
                ),
                Err(error) => (
                    crate::RuntimeToolExecutionStatus::Failed,
                    None,
                    Some(error.to_string()),
                ),
            };
            return crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status,
                category: request.category,
                output,
                error,
                evidence_ref: format!("catalog:{}", request.idempotency_key),
                observed_evidence,
            };
        }
        if matches!(request.tool_name.as_str(), "edit" | "tool_orchestrate")
            && self
                .held_write
                .get()
                .is_some_and(|hold| !hold.released.load(std::sync::atomic::Ordering::Acquire))
        {
            let services = self.services.get().unwrap().upgrade().unwrap();
            let graph = &request.parent_execution.as_ref().unwrap().execution_id;
            let marker = services.workspace_root().join(".cowd/fixtures/busy").join(
                crate::context_evidence::read_sha256(graph.as_bytes())
                    .trim_start_matches("sha256:"),
            );
            publish_fixture_file(
                &marker,
                &serde_json::json!({"effect":request.idempotency_key}),
            )
            .unwrap();
        }
        if request.tool_name == "read_file" {
            let hold = self.read_hold.lock().unwrap().clone();
            if let Some(hold) =
                hold.filter(|hold| !hold.released.load(std::sync::atomic::Ordering::Acquire))
            {
                hold.entered.notify_one();
                hold.release.notified().await;
                hold.released
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        let result = if matches!(request.tool_name.as_str(), "edit" | "tool_orchestrate") {
            let binding = request
                .step_binding
                .as_ref()
                .expect("write retains original Step");
            match binding.resolve(self.catalog.get().map(tools::ToolHost::pin_snapshot)) {
                Ok(step) => {
                    let input = serde_json::from_str(&request.input).unwrap();
                    step.lease()
                        .execute_async(
                            request.authorization.as_ref().unwrap(),
                            &request.tool_name,
                            &input,
                        )
                        .await
                        .map_err(|error| error.to_string())
                }
                Err(error) => Err(error.to_string()),
            }
        } else {
            Ok(self
                .tool_output(
                    &request.tool_name,
                    &request.input,
                    &request.authorized_scopes,
                )
                .await)
        };
        if matches!(request.tool_name.as_str(), "edit" | "tool_orchestrate") {
            if let Some(gate) = self
                .held_write
                .get()
                .filter(|gate| !gate.released.load(std::sync::atomic::Ordering::Acquire))
            {
                gate.entered.notify_one();
                gate.release.notified().await;
                gate.released
                    .store(true, std::sync::atomic::Ordering::Release);
            }
        }
        if matches!(request.tool_name.as_str(), "edit" | "tool_orchestrate") {
            let services = self.services.get().unwrap().upgrade().unwrap();
            if let Some(parent) = &request.parent_execution {
                let marker = services.workspace_root().join(".cowd/fixtures/busy").join(
                    crate::context_evidence::read_sha256(parent.execution_id.as_bytes())
                        .trim_start_matches("sha256:"),
                );
                if marker.exists() {
                    std::fs::remove_file(marker).unwrap();
                }
            }
        }
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if result.is_ok() {
                crate::RuntimeToolExecutionStatus::Executed
            } else {
                crate::RuntimeToolExecutionStatus::Failed
            },
            category: request.category,
            output: result.as_ref().ok().cloned(),
            error: result.err(),
            evidence_ref: format!("read:{}", request.tool_use_id),
            observed_evidence: vec![],
        }
    }
    fn delegated_tool_effect_descriptor(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.catalog
            .get()
            .map(|catalog| catalog.pin_snapshot().describe_effect(name, input))
    }
}

pub(super) struct RootReviewReader(pub(super) Arc<ReviewReadHost>);
#[async_trait::async_trait]
impl crate::ToolExecutor for RootReviewReader {
    fn bounded_process_authority(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<crate::tool_host::BoundedProcessAuthority, crate::ToolError> {
        crate::RuntimeExecutionHost::bounded_process_authority(self.0.as_ref(), request)
    }

    fn bounded_process_lease(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, crate::ToolError> {
        crate::RuntimeExecutionHost::bounded_process_lease(self.0.as_ref(), request)
    }

    fn bounded_process_plane(&self) -> Option<Arc<crate::ToolExecutionPlane>> {
        crate::RuntimeExecutionHost::bounded_process_plane(self.0.as_ref())
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        self.0.catalog.get().map(tools::ToolHost::pin_snapshot)
    }
    fn available_tool_names(&self) -> Vec<String> {
        vec![
            "read_file".into(),
            "edit".into(),
            "evidence_retrieve".into(),
            "artifact_publish".into(),
            "execute_code".into(),
        ]
    }
    async fn execute_step_authorized_invocation_output(
        &self,
        binding: Option<&crate::step_snapshot::StepCallBinding>,
        invocation_id: &str,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        let services = self.0.services.get().unwrap().upgrade().unwrap();
        let binding = binding.ok_or_else(|| {
            crate::ToolError::new("Root evidence requires the original provider Step")
        })?;
        let identity = &binding.snapshot.identity;
        let root = identity
            .execution_id
            .as_deref()
            .filter(|root| *root == "root-agentic-execution")
            .ok_or_else(|| crate::ToolError::new("Root evidence Step has a foreign execution"))?;
        let plans = services
            .event_store()
            .list_stream(&format!("session:{}", identity.session_id))
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        let mut matching = plans
            .iter()
            .filter(|event| {
                event.kind == "tool.execution_plan.created"
                    && event.scope == crate::RuntimeEventScope::Tool
                    && event.actor.as_deref() == Some("conversation_runtime")
                    && event.status.as_deref() == Some("planned")
                    && [
                        ("execution", root),
                        ("session", identity.session_id.as_str()),
                        ("turn", identity.turn_id.as_str()),
                    ]
                    .iter()
                    .all(|(kind, reference)| {
                        event
                            .refs
                            .iter()
                            .any(|item| item.kind == *kind && item.id == *reference)
                    })
                    && event.refs.iter().any(|item| {
                        item.kind == "governed_tool_plan"
                            && event.payload["plan_id"].as_str() == Some(item.id.as_str())
                    })
            })
            .filter_map(|event| {
                serde_json::from_value::<crate::governed_tool_plan::GovernedToolPlan>(
                    event.payload.clone(),
                )
                .ok()
            })
            .filter_map(|plan| {
                plan.tasks
                    .iter()
                    .find(|task| {
                        task.step_binding
                            .as_ref()
                            .is_some_and(|step| step == binding)
                            && task.tool_call_id == binding.origin.call_id
                    })
                    .cloned()
                    .map(|task| (plan, task))
            });
        let (plan, task) = matching.next().ok_or_else(|| {
            crate::ToolError::new("Root bound call has no original persisted governed plan")
        })?;
        if matching.next().is_some() {
            return Err(crate::ToolError::new(
                "Root bound call has ambiguous persisted plan",
            ));
        }
        let mut request = crate::RuntimeToolExecutionRequest::from_tool_request(
            &crate::tool_dispatch::ToolRequest {
                tool_use_id: invocation_id.into(),
                tool_name: name.into(),
                input: input.into(),
                depends_on: vec![],
            },
        );
        request.compiled_invocation = Some(task.invocation);
        request.step_binding = Some(binding.clone());
        request.governed_plan_id = plan.plan_id;
        request.governed_plan_revision = plan.revision;
        request.idempotency_key = binding.effect_idempotency_key();
        request.authorization = Some(authorization.clone());
        request.category = crate::ToolSafetyCategory::from_tool_name(name);
        request.session_id = Some(binding.snapshot.identity.session_id.clone());
        request.policy_revision = authorization.policy_revision;
        request.authorized_scopes =
            vec![format!("session:{}", binding.snapshot.identity.session_id)];
        assert!(request.parent_execution.is_none());
        let intent = services
            .event_store()
            .event_by_idempotency_key(
                &format!("execution-effect:{}", request.idempotency_key),
                &format!(
                    "{}:{}",
                    request.idempotency_key,
                    if request.compiled_invocation.as_ref().unwrap().effect.effect_kind
                        == harness_contract::tool::ToolEffectKind::Read
                    {
                        "read-intent"
                    } else {
                        "intent"
                    }
                ),
            )
            .map_err(|error| crate::ToolError::new(error.to_string()))?
            .ok_or_else(|| {
                crate::ToolError::new("Root physical call requires original Runtime admission")
            })?;
        assert_eq!(
            intent.payload["replay"]["invocation"]["effect"],
            serde_json::to_value(&request.compiled_invocation.as_ref().unwrap().effect).unwrap()
        );
        assert_eq!(
            intent.payload["input_sha256"],
            crate::context_evidence::read_sha256(input.as_bytes())
        );
        assert!(intent.payload["replay"]["parent_execution"].is_null());
        let outcome =
            crate::RuntimeExecutionHost::execute_runtime_tool(self.0.as_ref(), &request).await;
        if outcome.status != crate::RuntimeToolExecutionStatus::Executed {
            return Err(crate::ToolError::new(outcome.error.unwrap_or_default()));
        }
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            outcome.output.unwrap(),
        ))
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        assert_eq!(authorization.tool_id, name);
        self.execute_output(name, input).await
    }
    async fn execute_output(
        &self,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        let _ = (name, input);
        Err(crate::ToolError::new(
            "Root evidence requires its original bound provider call",
        ))
    }
    fn registered_tool_effect(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        crate::RuntimeExecutionHost::delegated_tool_effect_descriptor(self.0.as_ref(), name, input)
    }
}
pub(super) struct NoReviewProvider;
impl crate::ApiClient for NoReviewProvider {
    fn stream(
        &mut self,
        _: crate::ApiRequest,
    ) -> std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<crate::AssistantEvent, crate::RuntimeError>>
                + Send
                + '_,
        >,
    > {
        panic!("physical review gate must not call a paid Provider")
    }
}
pub(super) async fn write_root_review_effect(runtime: &RootReviewRuntime, id: &str, content: &str) {
    root_review_target_tool(runtime, id, "edit", "review-effect.txt", content).await;
}

pub(super) async fn root_review_target_tool(
    runtime: &RootReviewRuntime,
    id: &str,
    tool_name: &str,
    target: &str,
    content: &str,
) {
    let input = r1_root_review_target_input(tool_name, target, content);
    root_review_input_tool(runtime, id, tool_name, &input.to_string()).await;
}

fn r1_root_review_target_input(tool_name: &str, target: &str, content: &str) -> serde_json::Value {
    match tool_name {
        "read_file" => serde_json::json!({"path":target,"complete":true}),
        "edit" => serde_json::json!({"action":"write","path":target,"content":content}),
        _ => panic!("target fixture requires an explicit registered tool shape: {tool_name}"),
    }
}

async fn root_review_input_tool(
    runtime: &RootReviewRuntime,
    id: &str,
    tool_name: &str,
    input: &str,
) {
    let mut input: serde_json::Value = serde_json::from_str(input).unwrap();
    if tool_name == "edit" && input.get("action").is_none() {
        input["action"] = "write".into();
    }
    let input = input.to_string();
    let written = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
                id: id.into(),
                name: tool_name.into(),
                input: input.to_owned(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            4,
        )
        .await
        .unwrap();
    assert_eq!(written.failed, 0, "{:?}", written.messages);
}

pub(super) async fn root_reads_review_result(
    services: &Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &str,
) {
    root_reads_generic_results(services, host, selector, false, false).await;
}

pub(super) fn root_reads_generic_results<'fixture>(
    services: &'fixture Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &'fixture str,
    read_tool_receipt: bool,
    write_effect: bool,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = (Option<String>, RootReviewRuntime)> + Send + 'fixture>,
> {
    Box::pin(execute_root_review_reads_with_approval(
        services,
        host,
        selector,
        read_tool_receipt,
        write_effect,
        if write_effect {
            PermissionMode::DangerFullAccess
        } else {
            PermissionMode::ReadOnly
        },
        None,
    ))
}

pub(in crate::agentic) fn bind_review_fixture_root(
    services: &RuntimeServices,
    commits: &crate::execution_core::graph::ExecutionCommitService,
    mut graph: ExecutionGraph,
    node_id: &str,
) -> ExecutionGraph {
    use crate::execution_core::graph::{
        ExecutionGraphEvent, ExecutionNodeBinding, NodeExecutionTicket,
    };
    assert!(graph.parent_execution.is_none());
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == node_id)
        .unwrap()
        .clone();
    assert_eq!(node.kind, ExecutionNodeKind::InlineModel);
    if graph.node_statuses[node_id] == ExecutionNodeStatus::Running {
        let running = services
            .event_store()
            .latest_for_stream(&format!("{}:node:{node_id}", graph.id))
            .unwrap()
            .unwrap();
        let owner = running.activity_binding().unwrap();
        let original = services
            .event_store()
            .list_stream_after(
                &graph.id,
                owner.revision - 1,
                owner.revision,
                1,
                4 * 1024 * 1024,
            )
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(original.transaction_id, running.transaction_id);
        assert_eq!(original.commit_cursor, running.commit_cursor);
        let event: ExecutionGraphEvent = serde_json::from_value(
            services
                .event_store()
                .resolve_graph_record(&original)
                .unwrap(),
        )
        .unwrap();
        let binding = match event {
            ExecutionGraphEvent::NodeTransitioned {
                node_id: started,
                to: ExecutionNodeStatus::Running,
                binding: Some(binding),
                ..
            } if started == node_id => binding,
            ExecutionGraphEvent::NodesStarted { mut bindings, .. }
            | ExecutionGraphEvent::Checkpoint { mut bindings, .. } => {
                bindings.remove(node_id).unwrap()
            }
            _ => panic!("fixture root must retain its original bound start"),
        };
        binding.validate_identity(&node).unwrap();
        assert_eq!(
            graph.recovery_cursor.node_attempts[node_id],
            binding.attempt
        );
        return graph;
    }
    if graph.node_statuses[node_id] == ExecutionNodeStatus::Planned {
        graph = commits
            .transition_node(&graph, node_id, ExecutionNodeStatus::Ready, None, vec![])
            .unwrap()
            .graph;
    }
    assert_eq!(graph.node_statuses[node_id], ExecutionNodeStatus::Ready);
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node.id.clone(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: ExecutionNodeBinding::next_attempt(&graph, node_id).unwrap(),
        idempotency_key: node.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    ExecutionNodeBinding::validate_ticket(&ticket, &graph, &node, ticket.attempt).unwrap();
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind,
        ticket_idempotency_key: ticket.idempotency_key,
        attempt: ticket.attempt,
        resource_lease_refs: vec![],
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    commits
        .bind_and_start_node(&graph, node_id, binding)
        .unwrap()
        .graph
}

async fn execute_root_review_reads_with_approval(
    services: &Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &str,
    read_tool_receipt: bool,
    write_effect: bool,
    permission_mode: PermissionMode,
    approval_coordinator: Option<Arc<crate::ApprovalCoordinator>>,
) -> (Option<String>, RootReviewRuntime) {
    {
        let commits = services.commit_service();
        let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
        let mut graph = if services
            .event_store()
            .stream_revision("root-agentic-execution")
            .unwrap()
            > 0
        {
            graphs.load("root-agentic-execution").unwrap()
        } else {
            let mut graph = ExecutionGraph::new("Root effect verification");
            graph.id = "root-agentic-execution".into();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                root_task_id: "task-root-dispatch".into(),
                task_id: "task-root-dispatch".into(),
                generation: 1,
            });
            let mut node = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "payload:root",
            );
            node.id = "root-agentic-execution:model".into();
            graph.nodes.push(node);
            commits.register_graph(graph).unwrap().graph
        };
        let node = graph
            .nodes
            .iter()
            .find(|node| node.id == "root-agentic-execution:model")
            .expect("original fixture root caller")
            .id
            .clone();
        let _ = bind_review_fixture_root(services, commits, graph, &node);
    }
    let ports = if let Some(port) = services.session_journal_port() {
        port
    } else {
        #[cfg(test)]
        {
            let store = Arc::new(crate::test_support::session_store());
            store
                .create_session(&session::SessionRecord {
                    session_id: "session-dispatch".into(),
                    platform: "test".into(),
                    chat_id: "review".into(),
                    user_id: None,
                    model: None,
                    created_at: "2026-09-09T00:00:00Z".into(),
                    last_activity: "2026-09-09T00:00:00Z".into(),
                    message_count: 0,
                    reset_policy: "manual".into(),
                    metadata_json: None,
                    input_tokens: 0,
                    output_tokens: 0,
                    status: "active".into(),
                })
                .await
                .unwrap();
            let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
            services
                .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports.clone())
                .unwrap();
            ports as Arc<dyn crate::SessionRuntimeJournalPort>
        }
        #[cfg(not(test))]
        {
            panic!("shared integration fixture requires installed Session ports")
        }
    };
    let mut session = crate::session::Session::new().with_workspace_root(services.workspace_root());
    session.session_id = "session-dispatch".into();
    let bus = crate::CowdEventBus::new();
    let binding = services
        .event_store()
        .latest_for_stream("root-agentic-execution:node:root-agentic-execution:model")
        .unwrap()
        .unwrap()
        .activity_binding()
        .unwrap();
    let _scope = bus.enter_execution_with_activity(
        crate::CowdExecutionContext {
            execution_id: "root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
        },
        Some(binding),
    );
    let server = RootWireServer::start().await;
    let registry = Arc::new(
        crate::ProviderRegistry::new(crate::config::ProvidersConfig {
            providers: std::collections::HashMap::from([(
                "r1-root-loopback".into(),
                crate::config::ProviderConfig {
                    name: "r1-root-loopback".into(),
                    base_url: server.endpoint.clone(),
                    api_key: "local-fixture".into(),
                    models: vec!["r1-root-wire".into()],
                    protocol: Some("completions".into()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        })
        .unwrap(),
    );
    let definitions = host
        .catalog
        .get()
        .unwrap()
        .pin_snapshot()
        .snapshot()
        .catalog
        .definitions(None)
        .into_iter()
        .filter(|definition| {
            matches!(
                definition.name.as_str(),
                "read_file" | "edit" | "evidence_retrieve" | "artifact_publish" | "execute_code"
            )
        })
        .map(|definition| crate::ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        })
        .collect();
    let client =
        crate::ProviderRuntimeClient::new(registry, "r1-root-wire".into(), definitions).unwrap();
    let mut conversation = crate::ConversationRuntime::new(
        session,
        client,
        RootReviewReader(host),
        crate::PermissionPolicy::new(permission_mode),
        vec!["Read current result and independently verify the requested bounded evidence.".into()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(services.event_store()))
    .with_session_journal_port(ports)
    .with_artifact_store(Arc::clone(services.artifact_store()))
    .with_tool_execution_plane(Arc::clone(services.tool_execution_plane()))
    .with_cowd_event_bus(bus.clone());
    if let Some(coordinator) = approval_coordinator {
        conversation = conversation
            .with_checkpoint_identity(services.workspace_key(), None)
            .with_approval_coordinator(coordinator);
    }
    conversation.set_active_model("r1-root-wire");
    conversation = conversation.with_model_context_window(262_144);
    conversation.session_input_stream().set_active_turn(Some(
        harness_contract::turn::TurnId::from_string("turn-dispatch"),
    ));
    conversation.begin_turn_runtime_epoch().await;
    conversation
        .begin_turn_strategy_with_resource_snapshot(
            "turn-dispatch",
            "retrieve and verify the requested current result evidence",
            None,
        )
        .unwrap();
    conversation
        .bind_turn_strategy_execution("turn-dispatch", "root-agentic-execution")
        .unwrap();
    let runtime = RootReviewRuntime {
        runtime: tokio::sync::Mutex::new(conversation),
        server,
        bus,
        services: services.clone(),
    };
    let result = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
                id: "root-read-result".into(),
                name: "evidence_retrieve".into(),
                input: serde_json::json!({"evidence_ref":selector}).to_string(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(result.failed, 0, "{:?}", result.messages);
    if !read_tool_receipt {
        return (None, runtime);
    }
    if write_effect {
        let written = runtime.execute_tool_batch_step(&[crate::conversation::ModelToolCall {
            binding: None,
            id: "root-write-effect".into(), name: "edit".into(),
            input: serde_json::json!({"path":"review-effect.txt","content":"actually persisted revised content\nstable middle line\ninitial final line\n"}).to_string(),
            depends_on: vec![],
        }], &crate::SharedPrompter::none(), 2).await.unwrap();
        assert_eq!(written.failed, 0, "{:?}", written.messages);
    }
    let events = services
        .event_store()
        .events_for_root_execution_kind(
            "root-agentic-execution",
            "tool.invocation.completed",
            None,
            64,
        )
        .unwrap();
    let reference = events
        .iter()
        .rev()
        .find(|event| {
            event.payload["tool_call_id"]
                == if write_effect {
                    "root-write-effect"
                } else {
                    "root-read-result"
                }
        })
        .unwrap()
        .payload["full_output_ref"]
        .as_str()
        .unwrap()
        .to_string();
    let second = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
                id: "root-read-tool-receipt".into(),
                name: "evidence_retrieve".into(),
                input: serde_json::json!({"evidence_ref":reference}).to_string(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(second.failed, 0, "{:?}", second.messages);
    (Some(reference), runtime)
}

#[cfg(feature = "test-fixtures")]
pub struct ReviewedGoalFixture {
    pub services: Arc<RuntimeServices>,
    pub goal: harness_contract::goal::GoalContract,
    pub program: crate::AgenticProgramProjection,
}

#[cfg(feature = "test-fixtures")]
pub async fn independently_reviewed_goal_fixture(
    compose: impl FnOnce(Arc<dyn crate::RuntimeExecutionHost>) -> Arc<RuntimeServices>,
    store: Arc<session::UnifiedSessionStore>,
) -> ReviewedGoalFixture {
    let read_host = Arc::new(ReviewReadHost::default());
    let services = compose(read_host.clone());
    store
        .create_session(&session::SessionRecord {
            session_id: "session-dispatch".into(),
            platform: "test".into(),
            chat_id: "review".into(),
            user_id: None,
            model: None,
            created_at: "2026-09-09T00:00:00Z".into(),
            last_activity: "2026-09-09T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .unwrap();
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
    services
        .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports)
        .unwrap();
    exercise_reviewed_goal_delivery_with_drain(
        services.clone(),
        read_host,
        false,
        false,
        DelegatedEffectCase::None,
        true,
        PhysicalDrainTiming::BeforeGoal,
    )
    .await;
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
    ReviewedGoalFixture {
        services,
        goal,
        program,
    }
}

const REAL_PROCESS_DEFINITION: &str = "workspace/r1/controlled-process";

#[cfg(test)]
pub(super) fn observe_process_cleanup_driver(
    packet: &AgentTaskPacket,
) -> std::sync::mpsc::Receiver<String> {
    crate::agent_process_jsonl_adapter::observe_cleanup_driver_for_test(packet)
}

#[cfg(test)]
pub(super) fn retry_process_owned_scratch(
    packet: &AgentTaskPacket,
    directory: &std::path::Path,
) -> Result<(), String> {
    crate::agent_process_jsonl_adapter::retry_owned_scratch_for_test(packet, directory)
}

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) struct PhysicalProcessFixture {
    pub services: Arc<RuntimeServices>,
    pub spec: crate::ProcessJsonlSpec,
    host: Arc<ReviewReadHost>,
    definition: String,
    _storage_root: Option<tempfile::TempDir>,
}

#[cfg(any(test, feature = "test-fixtures"))]
impl PhysicalProcessFixture {
    #[cfg(test)]
    pub(crate) async fn new_with_journal_763(
        profile: crate::config::AgentExecutorSandboxProfile,
        name: &str,
        journal: Arc<crate::RuntimeEventStore>,
    ) -> Self {
        native_retirement_479::scope_journal_763(journal, Self::new(profile, name)).await
    }

    #[cfg(test)]
    pub(crate) async fn new(
        profile: crate::config::AgentExecutorSandboxProfile,
        name: &str,
    ) -> Self {
        Box::pin(Self::with_session_owner(profile, name, None)).await
    }

    async fn with_session_owner(
        profile: crate::config::AgentExecutorSandboxProfile,
        name: &str,
        original: Option<&Arc<RuntimeServices>>,
    ) -> Self {
        let host = Arc::new(ReviewReadHost::default());
        let storage_root = tempfile::tempdir().unwrap();
        let workspace = storage_root.path().join("workspace");
        std::fs::create_dir_all(&workspace).unwrap();
        #[cfg(test)]
        let fixture_journal = native_retirement_479::selected_journal();
        #[cfg(not(test))]
        let fixture_journal = Arc::new(crate::RuntimeEventStore::for_test());
        let mut builder = RuntimeServices::builder(storage_root.path().join("home"), &workspace)
            .runtime_event_store(fixture_journal)
            .artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
                storage_root.path().join("artifacts"),
            )))
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
            .tool_execution_host(host.clone());
        if let Some(original) = original {
            builder = builder.session_ports(
                original
                    .session_query_port()
                    .expect("original real Session query owner"),
                original
                    .session_ingress_port()
                    .expect("original real Session ingress owner"),
                original
                    .session_journal_port()
                    .expect("original real Session journal owner"),
                original
                    .session_application_port()
                    .expect("original real Session application owner"),
            );
        }
        let services = builder.build().unwrap();
        host.services.set(Arc::downgrade(&services)).unwrap();
        let spec = install_real_process_fixture_case(&services, &host, profile, name).await;
        let fixture_objective_id = "objective-dispatch".to_string();
        let fixture_program_id = "program-dispatch".to_string();
        let root_mission_id = "mission:auto:dispatch-regression";
        services
            .mission_runtime()
            .create_mission(root_mission_id, "root Agent-first Program", Vec::new())
            .expect("create non-default root mission");
        std::fs::create_dir_all(services.workspace_root().join("crates/runtime"))
            .expect("create bounded test workspace");
        std::fs::write(
            services.workspace_root().join("crates/runtime/Cargo.toml"),
            "[package]\nname = \"runtime-test\"\n",
        )
        .expect("write bounded test manifest");
        let mut root_graph = ExecutionGraph::new("root Agent-first Program");
        root_graph.id = "root-agentic-execution".to_string();
        root_graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            root_task_id: "task-root-dispatch".to_string(),
            task_id: "task-root-dispatch".to_string(),
            generation: 1,
        });
        let mut root_node = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "payload:root",
        );
        root_node.id = "root-agentic-execution:model".to_string();
        root_node.idempotency_key = "root-agentic-model".to_string();
        root_graph.nodes.push(root_node);
        services
            .commit_service()
            .register_graph(root_graph)
            .expect("register root graph");
        let bind_root = |mut envelope: AgentActionEnvelope| {
            envelope.actor.objective_id = fixture_objective_id.clone();
            envelope.actor.program_id = fixture_program_id.clone();
            envelope.actor.root_execution_id = Some("root-agentic-execution".to_string());
            envelope
        };
        services.publish_session_execution_policy(
            "session-dispatch",
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Autonomous,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
                ),
            ),
        );
        let root_task_spec = services
            .task_runtime_port()
            .bind_task_spec(
                "session-dispatch",
                Some(PermissionMode::DangerFullAccess),
                harness_contract::task::TaskSpec::new("root Agent-first Program"),
            )
            .expect("bind root task policy");
        services
            .task_aggregate_service()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: "task-root-dispatch".to_string(),
                mission_id: root_mission_id.to_string(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: "session-dispatch".to_string(),
                origin_turn_id: "turn-dispatch".to_string(),
                root_task_id: "task-root-dispatch".to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: root_task_spec,
                evidence_refs: Vec::new(),
            })
            .expect("create root task");

        let _ = bind_root;
        std::fs::create_dir_all(services.workspace_root().join("out")).unwrap();
        std::fs::write(
            services.workspace_root().join("out/physical-sentinel.txt"),
            "initial sentinel",
        )
        .unwrap();
        std::fs::write(
            services.workspace_root().join("out/a"),
            "initial narrow target",
        )
        .unwrap();
        services.release_recovered_producers().await.unwrap();
        Self {
            services,
            spec,
            host,
            definition: format!("workspace/r1/{name}"),
            _storage_root: Some(storage_root),
        }
    }

    pub(crate) async fn dispatch(
        &self,
        name: &str,
        scopes: Vec<String>,
        write: bool,
        network: bool,
        started: bool,
    ) -> PhysicalWorker {
        self.try_dispatch(name, scopes, write, network, started)
            .await
            .unwrap()
    }

    pub(super) async fn try_dispatch(
        &self,
        name: &str,
        scopes: Vec<String>,
        write: bool,
        network: bool,
        started: bool,
    ) -> Result<PhysicalWorker, String> {
        let mut capabilities = vec!["read".to_string()];
        if write {
            capabilities.push("write".into());
        }
        if network {
            capabilities.push("network".into());
        }
        let action = |suffix: &str, action| {
            let mut envelope = root(&format!("{name}:{suffix}"), action);
            envelope.actor.root_execution_id = Some("root-agentic-execution".into());
            envelope.actor.permission_ceiling = Some(PermissionMode::DangerFullAccess);
            envelope
        };
        let team = self
            .services
            .submit_agent_action(&action(
                "team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: name.into(),
                    mission: "Physical envelope actual Task".into(),
                    objective: None,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            team.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{team:?}"
        );
        let team_ref = team.changed_refs[0].clone();
        let invited = self
            .services
            .submit_agent_action(&action(
                "invite",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team_ref.clone(),
                    role: "Controlled physical probe".into(),
                    mission: "Execute admitted physical probe".into(),
                    required_capabilities: capabilities.clone(),
                    existing_agent_ref: None,
                    definition_ref: Some(self.definition.clone()),
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            invited.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{invited:?}"
        );
        let publication = action(
            "publish",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref,
                title: "Physical probe".into(),
                objective: "Exercise original Process boundary".into(),
                acceptance: "Use actual child and original Host".into(),
                criterion_source: Default::default(),
                acceptance_checks: vec![],
                required_capabilities: capabilities,
                depends_on: vec![],
                obligation_refs: vec![],
                purpose: harness_contract::agent_action::TaskPurpose::Exploration,
                execution_requirements: vec![],
                expertise_hints: vec![],
            }),
        );
        let published = self
            .services
            .submit_agent_action(&publication)
            .await
            .unwrap();
        assert_eq!(
            published.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{published:?}"
        );
        let receipts = self
            .services
            .dispatch_agentic_followups(
                &publication,
                AgenticDispatchContext {
                    session_id: "session-dispatch".into(),
                    turn_id: "turn-dispatch".into(),
                    model_lease: "test".into(),
                    permission_ceiling: if write {
                        PermissionMode::WorkspaceWrite
                    } else {
                        PermissionMode::ReadOnly
                    },
                    resource_scopes: scopes,
                },
            )
            .await
            .map_err(|error| format!("real dispatch/compiler admission: {error}"))?;
        if receipts.len() != 1 {
            return Err(format!(
                "real dispatch did not admit one Task: {name}, count={}",
                receipts.len()
            ));
        }
        let graph = self
            .services
            .graph_state_store()
            .load(&receipts[0].graph_id)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(
            &graph
                .nodes
                .iter()
                .find(|node| node.kind == ExecutionNodeKind::AgentTask)
                .unwrap()
                .payload_ref,
        )
        .unwrap();
        let binding = packet.binding.as_ref().unwrap();
        assert_eq!(
            binding.definition_ref.definition_id.as_str(),
            self.definition
        );
        assert_eq!(
            binding.executor,
            harness_contract::agent::AgentExecutorPolicy::ProcessJsonl {
                command_ref: self.spec.command_ref.clone(),
                command_digest: self.spec.command_digest.clone(),
            }
        );
        let process = RealProcessSession {
            services: self.services.clone(),
            packet: packet.clone(),
        };
        if started {
            let actual = process.started().await.unwrap();
            assert_eq!(actual.binding.as_ref().unwrap().executor, binding.executor);
            let actor = self
                .services
                .resolve_agent_action_actor(
                    &ExecutionParentBinding {
                        execution_id: packet.graph_id().into(),
                        node_id: packet.node_id().into(),
                    },
                    None,
                )
                .await
                .unwrap();
            let claim = process
                .action(&AgentActionEnvelope {
                    action_id: format!("{name}:claim"),
                    actor,
                    expected_revision: None,
                    action: AgentAction::TaskClaim(TaskClaimInput {
                        task_ref: published.changed_refs[0].clone(),
                        reason: None,
                    }),
                })
                .await
                .unwrap();
            assert_eq!(
                claim.status,
                harness_contract::agent_action::AgentActionStatus::Applied,
                "{claim:?}"
            );
        }
        Ok(PhysicalWorker {
            process,
            packet,
            task_ref: published.changed_refs[0].clone(),
            root_actor: publication.actor,
        })
    }

    pub(super) fn edit_calls(&self) -> usize {
        self.host
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|call| call.tool_name == "edit")
            .count()
    }
}

#[cfg(any(test, feature = "test-fixtures"))]
pub(crate) struct PhysicalWorker {
    process: RealProcessSession,
    root_actor: AgentActorBinding,
    pub packet: AgentTaskPacket,
    pub task_ref: String,
}

#[cfg(any(test, feature = "test-fixtures"))]
impl PhysicalWorker {
    pub(crate) fn terminal_failure(&self) -> String {
        let graph = self
            .process
            .services
            .graph_state_store()
            .load(self.packet.graph_id())
            .unwrap();
        graph
            .node_results
            .get(self.packet.node_id())
            .and_then(|result| result.failure.as_ref())
            .map(|failure| failure.message.clone())
            .or_else(|| {
                self.process
                    .services
                    .agent_runtime()
                    .terminal_return(self.packet.agent_id())
                    .and_then(|returned| returned.failure)
            })
            .expect("actual terminal owner must retain the rejection reason")
    }
    pub(crate) async fn withdraw(&self) {
        let reason = self
            .process
            .services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:session-dispatch".into(),
                    expected_bytes: None,
                    original_name: Some("physical-fixture-withdrawal.txt".into()),
                },
                b"The bounded physical fixture has completed its requested probes.",
            )
            .await
            .unwrap();
        let mut envelope = root(
            &format!("physical-withdraw:{}", self.packet.run_id()),
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: self.task_ref.clone(),
                reason_ref: reason.selector.clone(),
                evidence_refs: vec![reason.selector],
            }),
        );
        envelope.actor = self.root_actor.clone();
        let revision = self
            .process
            .services
            .agent_action_service()
            .project(&self.root_actor.program_id)
            .unwrap()
            .revision;
        let mut foreign = envelope.clone();
        foreign.action_id.push_str(":foreign-ceiling");
        foreign.actor.permission_ceiling = Some(PermissionMode::ReadOnly);
        assert_ne!(
            foreign.actor.permission_ceiling,
            envelope.actor.permission_ceiling
        );
        let rejected = self
            .process
            .services
            .submit_agent_action(&foreign)
            .await
            .unwrap();
        assert_eq!(
            rejected.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(
            rejected.error.as_ref().unwrap().code,
            "program_binding_mismatch"
        );
        assert_eq!(
            self.process
                .services
                .agent_action_service()
                .project(&self.root_actor.program_id)
                .unwrap()
                .revision,
            revision
        );
        let result = self
            .process
            .services
            .submit_agent_action(&envelope)
            .await
            .unwrap();
        assert_eq!(
            result.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{result:?}"
        );
    }
    #[cfg(test)]
    async fn withdraw_and_reap_original_709(&self) {
        let services = &self.process.services;
        self.withdraw().await;
        let withdrawal = services.event_store().event_by_idempotency_key(
            &format!("agentic-program:{}", self.root_actor.program_id),
            &format!("agent-action:physical-withdraw:{}", self.packet.run_id()),
        ).unwrap().expect("original withdrawal must be durably applied");
        let envelope: AgentActionEnvelope =
            serde_json::from_value(withdrawal.payload["envelope"].clone()).unwrap();
        assert_eq!(envelope.actor, self.root_actor);
        assert!(matches!(&envelope.action, AgentAction::TaskWithdraw(input)
            if input.task_ref == self.task_ref));
        services.dispatch_agentic_followups(&envelope, AgenticDispatchContext {
            session_id: self.packet.session_id().into(),
            turn_id: self.root_actor.turn_id.clone(),
            model_lease: self.packet.model_lease.clone(),
            permission_ceiling: self.packet.permission_ceiling,
            resource_scopes: self.packet.resource_scopes.clone(),
        }).await.expect("original withdrawal follow-up must drive physical cancellation");
        wait_process_graph_terminal(services, &self.process).await;
        assert!(confirmed_run_exit_with_retry(services.event_store(), &self.packet).is_some());
        assert!(self.lifecycle().no_direct_effects(),
            "this host-only fixture must not conceal opaque Process effects");
        assert!(!process_control_directory(services, &self.packet).exists(),
            "original Process driver must join and remove its owned scratch");
        wait_r1_resource_drain(services).await;
    }

    #[cfg(test)]
    async fn drain_original_runtime_709(&self) {
        let report = self.process.services.shutdown_execution().await;
        assert!(report.errors.is_empty(), "{report:?}");
        assert_eq!(report.remaining_keys, 0, "{report:?}");
        assert_eq!(report.forced_aborts, 0, "{report:?}");
        wait_r1_resource_drain(&self.process.services).await;
    }

    #[cfg(test)]
    pub(crate) async fn withdraw_and_require_retirement_709(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let services = &self.process.services;
            let goal_id = format!("goal:{}", self.root_actor.root_execution_id.as_ref().unwrap());
            let goal_before = services.goal_store().get(&goal_id).unwrap();
            if let Some(goal) = &goal_before {
                assert_eq!(goal.completion, harness_contract::goal::GoalCompletion::Open);
                assert!(goal.terminal.is_none());
            }
            self.withdraw_and_reap_original_709().await;
            assert!(services.retry_agentic_run_exit(self.packet.graph_id()).await
                .expect("legal original attempt must settle after physical reap"));
            let program = services.agent_action_service()
                .project(&self.root_actor.program_id).unwrap();
            let task = &program.tasks[&self.task_ref];
            assert_eq!(task.status, crate::AgenticTaskStatus::Withdrawn);
            assert!(task.active_attempts.is_empty());
            assert!(task.pending_retirement.is_none());
            assert!(task.claimant.is_none() && task.claim_execution_id.is_none());
            assert!(program.objective_verdict.is_none());
            assert_eq!(services.goal_store().get(&goal_id).unwrap(), goal_before);
            self.drain_original_runtime_709().await;
            let drained = services.agent_action_service()
                .project(&self.root_actor.program_id).unwrap();
            assert_eq!(drained.tasks[&self.task_ref].status, crate::AgenticTaskStatus::Withdrawn);
            assert!(drained.tasks[&self.task_ref].active_attempts.is_empty());
            assert!(drained.objective_verdict.is_none());
            assert_eq!(services.goal_store().get(&goal_id).unwrap(), goal_before);
        }).await.expect("legal withdrawal must reap, retire and drain within 30 seconds");
    }

    #[cfg(test)]
    pub(crate) async fn withdraw_and_require_rejected_child_709(&self, child_id: &str) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let services = &self.process.services;
            let original_child = services.graph_state_store().load(child_id).unwrap();
            assert!(original_child.parent_execution.as_ref().is_some_and(|parent|
                parent.execution_id == self.packet.graph_id()
                    && parent.node_id == self.packet.node_id()));
            assert!(!crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(
                &original_child));
            assert!(matches!(self.packet.binding.as_ref().map(|binding| &binding.executor),
                Some(harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. })));
            let goal_id = format!("goal:{}", self.root_actor.root_execution_id.as_ref().unwrap());
            let goal_before = services.goal_store().get(&goal_id).unwrap();
            if let Some(goal) = &goal_before {
                assert_eq!(goal.completion, harness_contract::goal::GoalCompletion::Open);
                assert!(goal.terminal.is_none());
            }
            self.withdraw_and_reap_original_709().await;
            let child = services.graph_state_store().load(child_id).unwrap();
            assert_eq!(child.nodes, original_child.nodes);
            assert_eq!(child.parent_execution, original_child.parent_execution);
            assert!(original_child.node_statuses.values().all(|status|
                *status == ExecutionNodeStatus::Planned));
            assert!(original_child.node_results.is_empty());
            assert_eq!(child.node_statuses.len(), original_child.node_statuses.len());
            assert!(child.node_statuses.values().all(|status|
                *status == ExecutionNodeStatus::Cancelled));
            assert_eq!(child.revision, original_child.revision + 1);
            let cancellation_records = services.event_store().list_stream(child_id).unwrap()
                .into_iter().filter(|record| record.sequence > original_child.revision)
                .collect::<Vec<_>>();
            assert_eq!(cancellation_records.len(), 1,
                "raw child must have exactly its original withdrawal cancellation, not execution");
            let cancellation = &cancellation_records[0];
            assert_eq!(cancellation.stream_id, child_id);
            assert_eq!(cancellation.sequence, child.revision);
            assert_eq!(cancellation.kind, "execution_graph.command_applied");
            assert_eq!(cancellation.idempotency_key.as_deref(),
                Some(format!("{child_id}:revision:{}", child.revision).as_str()));
            let withdrawal = services.event_store().event_by_idempotency_key(
                &format!("agentic-program:{}", self.root_actor.program_id),
                &format!("agent-action:physical-withdraw:{}", self.packet.run_id()),
            ).unwrap().expect("the original withdrawal must precede child cancellation");
            assert!(cancellation.commit_cursor > withdrawal.commit_cursor);
            let cancellation_event: crate::execution_core::graph::ExecutionGraphEvent =
                serde_json::from_value(services.event_store()
                    .resolve_graph_record(cancellation).unwrap()).unwrap();
            eprintln!("Own790_Core618 {}", serde_json::json!({
                "child_id":child_id, "parent_graph_id":self.packet.graph_id(),
                "parent_node_id":self.packet.node_id(), "run_id":self.packet.run_id(),
                "attempt":self.packet.attempt,
                "cancellation_sequence":cancellation.sequence,
                "cancellation_commit_cursor":cancellation.commit_cursor,
                "withdrawal_commit_cursor":withdrawal.commit_cursor,
                "cancellation_kind":cancellation.kind,
                "cancellation_idempotency_key":cancellation.idempotency_key,
                "resolved_cancellation_event":&cancellation_event
            }));
            let crate::execution_core::graph::ExecutionGraphEvent::CommandApplied {
                command, reason, ..
            } = &cancellation_event else {
                panic!("expected original child cancellation, got {cancellation_event:?}");
            };
            assert_eq!(command, "cancel", "{cancellation_event:?}");
            let program_stream = format!("agentic-program:{}", self.root_actor.program_id);
            let program_cut = services.event_store().list_stream(&program_stream).unwrap()
                .into_iter().filter(|record| record.commit_cursor <= cancellation.commit_cursor)
                .collect::<Vec<_>>();
            let opened = program_cut.first().expect("original Program opening before cancellation");
            let cut_revision = program_cut.last().unwrap().sequence;
            let retiring = services.agent_action_service().project_snapshot_bounded(
                &self.root_actor.program_id, cut_revision, opened, None,
                |after, through| -> Result<Vec<crate::DurableRuntimeEvent>, String> {
                    Ok(program_cut.iter().filter(|record|
                        record.sequence > after && record.sequence <= through).cloned().collect())
                }, |error| error.to_string(),
            ).expect("authenticate original Task projection at cancellation cursor");
            let task_at_cancel = &retiring.tasks[&self.task_ref];
            assert_eq!(task_at_cancel.status, crate::AgenticTaskStatus::CancelRequested,
                "Task was not pending retirement at original cancellation: {cancellation_event:?}");
            assert_eq!(task_at_cancel.pending_retirement,
                Some(crate::agentic::program::AgenticTaskRetirement::Withdrawn));
            assert!(task_at_cancel.active_attempts.contains_key(self.packet.graph_id()));
            let withdrawal_envelope: AgentActionEnvelope =
                serde_json::from_value(withdrawal.payload["envelope"].clone()).unwrap();
            assert_eq!(withdrawal_envelope.actor, self.root_actor);
            assert!(matches!(&withdrawal_envelope.action, AgentAction::TaskWithdraw(input)
                if input.task_ref == self.task_ref));
            match reason.as_deref() {
                Some("Agentic Task was withdrawn or superseded") => {}
                Some("Original Agent exited during Task retirement") => {
                    let (_, exit_sources) = confirmed_run_exit_with_retry(services.event_store(), &self.packet,).expect("same packet must have an authenticated positive local exit");
                    let lifecycle = crate::agent_runtime::authenticated_run_lifecycle(
                        services.event_store(), &self.packet,
                    ).unwrap();
                    assert!(lifecycle.no_direct_effects(), "exit producer requires original NoDirectEffects");
                    assert_eq!(exit_sources, lifecycle.sources, "exit proof changed during authentication");
                    assert_eq!(exit_sources.len(), 1);
                    let source = &exit_sources[0];
                    for kind in ["agent.run_admitted", "agent.backend_admitted", "agent.local_exit"] {
                        let record = services.event_store().event_by_idempotency_key(
                            &source.stream_id, &format!("{kind}:{}", self.packet.run_id()),
                        ).unwrap().expect("original Process lifecycle proof record must exist");
                        assert_eq!(record.kind, kind);
                        assert_eq!(record.stream_id, source.stream_id);
                        assert!(record.sequence <= source.expected_revision);
                        assert!(record.commit_cursor <= cancellation.commit_cursor,
                            "late exit/backend proof cannot justify earlier cancellation: {record:?}; {cancellation_event:?}");
                        eprintln!("Own790_Core618_exit_source {}", serde_json::json!({
                            "stream_id":source.stream_id, "expected_revision":source.expected_revision,
                            "record":record, "cancel_cursor":cancellation.commit_cursor
                        }));
                    }
                }
                _ => panic!("unrecognized original cancellation producer: {cancellation_event:?}"),
            }
            assert_eq!(cancellation_event.project(Some(original_child.clone())).unwrap(), child,
                "the original durable cancellation delta must explain the entire child state");
            assert_eq!(child.node_results, original_child.node_results,
                "physical reaping must not fabricate raw child results");
            let before = services.agent_action_service()
                .project(&self.root_actor.program_id).unwrap();
            let error = services.retry_agentic_run_exit(self.packet.graph_id()).await
                .expect_err("raw conversation child under Process must not retire as Native");
            assert!(error.contains("runtime_retirement_not_settled"), "{error}");
            assert!(error.contains("runtime retirement non-Native conversation child"), "{error}");
            let after = services.agent_action_service()
                .project(&self.root_actor.program_id).unwrap();
            assert_eq!(after.revision, before.revision);
            let task = &after.tasks[&self.task_ref];
            assert_eq!(task.status, crate::AgenticTaskStatus::CancelRequested);
            assert_eq!(task.pending_retirement, Some(crate::agentic::program::AgenticTaskRetirement::Withdrawn));
            assert!(task.active_attempts.contains_key(self.packet.graph_id()));
            assert!(after.objective_verdict.is_none());
            assert_eq!(services.goal_store().get(&goal_id).unwrap(), goal_before);
            self.drain_original_runtime_709().await;
            let drained = services.agent_action_service()
                .project(&self.root_actor.program_id).unwrap();
            assert_eq!(drained.tasks[&self.task_ref].status, crate::AgenticTaskStatus::CancelRequested);
            assert!(!drained.tasks[&self.task_ref].active_attempts.is_empty());
            assert!(drained.objective_verdict.is_none());
            assert_eq!(services.goal_store().get(&goal_id).unwrap(), goal_before);
            assert_eq!(services.graph_state_store().load(child_id).unwrap(), child,
                "shutdown must not execute, delete or rewrite the cancelled raw child");
        }).await.expect("rejected retirement must still physically reap and drain within 30 seconds");
    }

    pub(crate) fn directory(&self) -> std::path::PathBuf {
        process_control_directory(&self.process.services, &self.packet)
    }
    pub(super) fn lifecycle(&self) -> crate::agent_runtime::AuthenticatedRunLifecycle {
        // The agent run stream can advance while the lifecycle is read. That is
        // a stale view of a live stream, not an untrusted one, and the product
        // reports it as retryable for exactly that reason. Unwrapping it made
        // the whole scenario fail on a concurrent append that has nothing to
        // do with what the scenario asserts, so read it again on the fresh
        // view and only give up when the stream really stops moving.
        let mut attempt = 0;
        loop {
            match crate::agent_runtime::authenticated_run_lifecycle(
                self.process.services.event_store(),
                &self.packet,
            ) {
                Ok(lifecycle) => return lifecycle,
                Err(error)
                    if error == "Agent exit source changed during authentication"
                        && attempt < 400 =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(5));
                    attempt += 1;
                }
                Err(error) => panic!("{error}"),
            }
        }
    }
    pub(super) async fn probe(&self, probe: serde_json::Value) -> serde_json::Value {
        self.process
            .command(serde_json::json!({"direct_probe":probe}))
            .await
            .unwrap()
    }
    pub(super) async fn tool(
        &self,
        name: &str,
        input: serde_json::Value,
    ) -> Result<String, String> {
        self.process
            .execute_tool(&uuid::Uuid::new_v4().to_string(), name, &input.to_string())
            .await
    }
    pub(super) async fn finish(&self) {
        self.withdraw().await;
        self.process.finish().await;
    }
    fn assert_original_control_action(&self, expected: &AgentActionEnvelope) -> u64 {
        let services = &self.process.services;
        let packet = &self.packet;
        let graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .unwrap();
        let canonical: AgentTaskPacket = serde_json::from_str(
            &graph
                .nodes
                .iter()
                .find(|node| node.id == packet.node_id())
                .unwrap()
                .payload_ref,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&canonical).unwrap(),
            serde_json::to_value(packet).unwrap()
        );
        let binding = packet.binding.as_ref().unwrap();
        binding.validate().unwrap();
        assert_eq!(
            crate::agent::binding::recompute_binding_digest(binding).unwrap(),
            binding.binding_digest
        );
        assert!(packet
            .allowed_tools
            .contains(&expected.action.kind().to_string()));
        assert!(binding
            .tool_contract_refs
            .contains(&expected.action.kind().to_string()));
        let agentic = packet
            .agentic_binding
            .as_ref()
            .expect("original logical Program binding");
        agentic.validate().unwrap();
        assert_eq!(binding.instance.instance_id, packet.agent_id());
        assert_eq!(binding.display.as_ref().unwrap().agent_id, agentic.agent_id);
        assert_ne!(agentic.agent_id, packet.agent_id());
        assert_eq!(expected.actor.kind, AgentActorKind::Agent);
        assert_eq!(expected.actor.program_id, agentic.program_id);
        assert_eq!(
            expected.actor.team_id.as_deref(),
            Some(agentic.team_id.as_str())
        );
        assert_eq!(
            expected.actor.agent_id.as_deref(),
            Some(agentic.agent_id.as_str())
        );
        assert_eq!(expected.actor.actor_id, agentic.agent_id);
        assert_eq!(
            expected.actor.execution_id.as_deref(),
            Some(packet.graph_id())
        );
        assert_eq!(expected.actor.session_id, packet.session_id());
        assert_eq!(
            Some(expected.actor.turn_id.as_str()),
            packet.assignment.execution_identity.turn_id()
        );
        let program_stream = format!("agentic-program:{}", expected.actor.program_id);
        let applied = services
            .event_store()
            .event_by_idempotency_key(
                &program_stream,
                &format!("agent-action:{}", expected.action_id),
            )
            .unwrap()
            .unwrap();
        assert_eq!(applied.kind, "agentic.action_applied");
        assert_eq!(applied.scope, crate::RuntimeEventScope::Program);
        assert_eq!(applied.schema_version, 1);
        assert_eq!(applied.status.as_deref(), Some("applied"));
        assert_eq!(
            applied.actor.as_deref(),
            Some(expected.actor.actor_id.as_str())
        );
        assert_eq!(
            applied.payload["envelope"],
            serde_json::to_value(expected).unwrap()
        );
        assert_eq!(
            applied.payload["entity_ref"].as_str(),
            Some(self.task_ref.as_str())
        );
        applied.sequence
    }

    fn assert_original_control_transport(&self, expected: &AgentActionEnvelope) -> u64 {
        use sha2::{Digest, Sha256};
        let services = &self.process.services;
        let packet = &self.packet;
        let binding = packet.binding.as_ref().unwrap();
        self.assert_original_control_action(expected);
        let applied = services.event_store().event_by_idempotency_key(
            &format!("agentic-program:{}", expected.actor.program_id),
            &format!("agent-action:{}", expected.action_id),
        ).unwrap().unwrap();
        let identity = serde_json::json!([
            packet.session_id(),
            packet.graph_id(),
            packet.node_id(),
            packet.run_id(),
            packet.agent_id(),
            packet.attempt,
            Some(binding.binding_digest.as_str()),
        ]);
        let transport_stream = format!(
            "agent-process-transport:{:x}",
            Sha256::digest(identity.to_string().as_bytes())
        );
        let transports = services
            .event_store()
            .list_stream(&transport_stream)
            .unwrap();
        let completions = transports
            .iter()
            .filter(|event| event.kind == "agent.process_request_completed")
            .filter_map(|event| {
                let observation: harness_contract::agent_action::AgentActionObservation =
                    serde_json::from_str(event.payload["response"]["output"].as_str()?).ok()?;
                (observation.action_id == expected.action_id && !observation.duplicate)
                    .then_some((event, observation))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            completions.len(),
            1,
            "require one original completed transport, not a fabricated file-effect receipt"
        );
        let (completed, observation) = &completions[0];
        assert_eq!(
            observation.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        assert_eq!(observation.program_id, expected.actor.program_id);
        assert_eq!(observation.action, expected.action.kind());
        assert_eq!(observation.revision, applied.sequence);
        assert_eq!(observation.changed_refs, vec![self.task_ref.clone()]);
        let request_id = completed.payload["request_id"].as_str().unwrap();
        let key = format!("{:x}", Sha256::digest(request_id.as_bytes()));
        let started = services
            .event_store()
            .event_by_idempotency_key(&transport_stream, &format!("started:{key}"))
            .unwrap()
            .unwrap();
        let encoded = serde_json::to_value(&expected.action).unwrap();
        let fingerprint = harness_contract::step::canonical_digest(
            "process-tool-call",
            &serde_json::json!(["tool_result", expected.action.kind(), encoded["input"]]),
        )
        .unwrap();
        for (event, stage) in [(&started, "started"), (*completed, "completed")] {
            assert_eq!(event.kind, format!("agent.process_request_{stage}"));
            assert_eq!(event.scope, crate::RuntimeEventScope::Agent);
            assert_eq!(event.schema_version, 1);
            assert_eq!(event.status.as_deref(), Some(stage));
            assert_eq!(event.actor.as_deref(), Some(packet.agent_id()));
            assert_eq!(
                event.idempotency_key.as_deref(),
                Some(format!("{stage}:{key}").as_str())
            );
            assert_eq!(event.payload["request_id"], request_id);
            assert_eq!(event.payload["fingerprint"], fingerprint);
            assert_eq!(event.payload["authority"], "transport_replay_only");
            for (kind, reference) in [
                ("agent_run", packet.run_id()),
                ("session", packet.session_id()),
                ("execution_graph", packet.graph_id()),
            ] {
                assert!(event
                    .refs
                    .iter()
                    .any(|item| item.kind == kind && item.id == reference));
            }
        }
        let response = &completed.payload["response"];
        assert_eq!(response["kind"], "tool_result");
        assert_eq!(response["run_id"], packet.run_id());
        assert_eq!(response["agent_id"], packet.agent_id());
        assert_eq!(response["request_id"], request_id);
        assert!(response.get("error").is_none());
        let step_id = response["origin_step_id"].as_str().unwrap();
        let session_events = services
            .event_store()
            .list_stream(&format!("session:{}", packet.session_id()))
            .unwrap();
        let steps = session_events
            .iter()
            .filter(|event| {
                event.kind == "process.step_bound"
                    && event.payload["identity"]["step_id"].as_str() == Some(step_id)
            })
            .collect::<Vec<_>>();
        assert_eq!(steps.len(), 1);
        let step = steps[0];
        assert_eq!(step.scope, crate::RuntimeEventScope::Session);
        assert_eq!(step.schema_version, 1);
        assert_eq!(step.actor.as_deref(), Some(packet.agent_id()));
        assert_eq!(step.status.as_deref(), Some("prepared"));
        let snapshot: harness_contract::step::StepSnapshot =
            serde_json::from_value(step.payload.clone()).unwrap();
        assert_eq!(snapshot.identity.session_id, packet.session_id());
        assert_eq!(
            Some(snapshot.identity.turn_id.as_str()),
            packet.assignment.execution_identity.turn_id()
        );
        assert_eq!(
            snapshot.identity.execution_id.as_deref(),
            Some(packet.graph_id())
        );
        assert_eq!(snapshot.identity.run_ref.as_deref(), Some(packet.run_id()));
        assert_eq!(
            snapshot.identity.actor_ref.as_deref(),
            Some(packet.agent_id())
        );
        assert_eq!(snapshot.identity.attempt, Some(packet.attempt));
        assert_eq!(snapshot.related_revisions["worker_run"], packet.run_id());
        assert_eq!(
            snapshot.related_revisions["attempt"],
            packet.attempt.to_string()
        );
        assert!(step.commit_cursor < started.commit_cursor);
        assert!(started.commit_cursor < applied.commit_cursor);
        assert!(applied.commit_cursor < completed.commit_cursor);
        applied.sequence
    }

    pub(super) async fn finish_readonly_control(&self) {
        assert_eq!(self.packet.permission_ceiling, PermissionMode::ReadOnly);
        assert!(!self
            .packet
            .binding
            .as_ref()
            .unwrap()
            .effective_capabilities
            .contains(&harness_contract::agent::AgentCapability::Write));
        let services = &self.process.services;
        let reason = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:session-dispatch".into(),
                    expected_bytes: None,
                    original_name: Some("readonly-control-withdraw.txt".into()),
                },
                b"The original readonly Process withdraws its own admitted Task.",
            )
            .await
            .unwrap();
        let action = AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
            task_ref: self.task_ref.clone(),
            reason_ref: reason.selector.clone(),
            evidence_refs: vec![reason.selector],
        });
        let revision = services
            .agent_action_service()
            .project(&self.root_actor.program_id)
            .unwrap()
            .revision;
        for mutation in 0..4 {
            let mut foreign = self.root_actor.clone();
            match mutation {
                0 => foreign.root_execution_id = Some("foreign-root".into()),
                1 => foreign.permission_ceiling = Some(PermissionMode::ReadOnly),
                2 => foreign.resource_scopes.push("write:foreign".into()),
                _ => foreign.turn_id = "foreign-turn".into(),
            }
            let rejected = services
                .submit_agent_action(&AgentActionEnvelope {
                    action_id: format!(
                        "readonly-control-foreign:{mutation}:{}",
                        self.packet.run_id()
                    ),
                    actor: foreign,
                    expected_revision: None,
                    action: action.clone(),
                })
                .await
                .unwrap();
            assert_eq!(
                rejected.status,
                harness_contract::agent_action::AgentActionStatus::Rejected
            );
            assert_eq!(
                services
                    .agent_action_service()
                    .project(&self.root_actor.program_id)
                    .unwrap()
                    .revision,
                revision
            );
        }
        let actor = services
            .resolve_agent_action_actor(
                &ExecutionParentBinding {
                    execution_id: self.packet.graph_id().into(),
                    node_id: self.packet.node_id().into(),
                },
                None,
            )
            .await
            .unwrap();
        let actual = self.process.started().await.unwrap();
        assert_eq!(
            serde_json::to_value(&actual).unwrap(),
            serde_json::to_value(&self.packet).unwrap()
        );
        let claimed = services
            .agent_action_service()
            .project(&self.root_actor.program_id)
            .unwrap();
        let task = &claimed.tasks[&self.task_ref];
        let agentic = self
            .packet
            .agentic_binding
            .as_ref()
            .expect("original logical Program binding");
        assert_eq!(actor.actor_id, agentic.agent_id);
        assert_eq!(actor.agent_id.as_deref(), Some(agentic.agent_id.as_str()));
        assert_eq!(actor.program_id, agentic.program_id);
        assert_eq!(task.status, crate::AgenticTaskStatus::Claimed);
        assert_eq!(task.claimant.as_deref(), Some(agentic.agent_id.as_str()));
        assert_eq!(
            task.claim_execution_id.as_deref(),
            Some(self.packet.graph_id())
        );
        assert_eq!(task.claim_generation, 1);
        let generation = task.claim_generation;
        let claim_action = AgentAction::TaskClaim(TaskClaimInput {
            task_ref: self.task_ref.clone(),
            reason: None,
        });
        let program_events = services
            .event_store()
            .list_stream(&format!("agentic-program:{}", self.root_actor.program_id))
            .unwrap();
        let claims = program_events
            .iter()
            .filter(|event| event.kind == "agentic.action_applied")
            .filter_map(|event| {
                serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone())
                    .ok()
            })
            .filter(|envelope| envelope.actor == actor && envelope.action == claim_action)
            .collect::<Vec<_>>();
        assert_eq!(claims.len(), 1);
        let claim_sequence = self.assert_original_control_transport(&claims[0]);
        let goal_id = format!("goal:{}", self.root_actor.root_execution_id.as_ref().unwrap());
        let goal_before = services.goal_store().get(&goal_id).unwrap();
        let stale = self
            .process
            .action(&AgentActionEnvelope {
                action_id: format!("readonly-control-stale:{}", self.packet.run_id()),
                actor: actor.clone(),
                expected_revision: Some(u64::MAX),
                action: action.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            stale.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(
            services
                .agent_action_service()
                .project(&self.root_actor.program_id)
                .unwrap()
                .revision,
            revision
        );
        assert!(services
            .event_store()
            .event_by_idempotency_key(
                &format!("agentic-program:{}", self.root_actor.program_id),
                &format!(
                    "agent-action:readonly-control-stale:{}",
                    self.packet.run_id()
                )
            )
            .unwrap()
            .is_none());
        let withdrawal = AgentActionEnvelope {
            action_id: format!("readonly-control-withdraw:{}", self.packet.run_id()),
            actor,
            expected_revision: None,
            action,
        };
        self.process.retire_control(&withdrawal, &self.task_ref).await;
        assert!(claim_sequence < self.assert_original_control_action(&withdrawal));
        let retiring = services
            .agent_action_service()
            .project(&self.root_actor.program_id)
            .unwrap();
        let task = &retiring.tasks[&self.task_ref];
        assert_eq!(task.claim_generation, generation + 1);
        assert_eq!(
            task.cancel_requested_by.as_deref(),
            Some(agentic.agent_id.as_str())
        );
        assert_eq!(
            task.cancel_reason_ref.as_deref(),
            match &withdrawal.action {
                AgentAction::TaskWithdraw(input) => Some(input.reason_ref.as_str()),
                _ => unreachable!(),
            }
        );
        assert!(matches!(
            task.status,
            crate::AgenticTaskStatus::CancelRequested | crate::AgenticTaskStatus::Withdrawn
        ));
        self.process.wait_exit().await;
        self.wait_terminal().await;
        assert!(self.lifecycle().no_direct_effects());
        assert!(!self.directory().exists());
        assert!(
            confirmed_run_exit_with_retry(services.event_store(), &self.packet)
                .is_some()
        );
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let current = services.agent_action_service().project(&self.root_actor.program_id).unwrap();
                let task = current.tasks.get(&self.task_ref).expect("withdrawal must retain the original Task");
                if task.status == crate::AgenticTaskStatus::Withdrawn {
                    assert_eq!(task.claim_generation, generation + 1);
                    assert!(task.claimant.is_none() && task.claim_execution_id.is_none());
                    assert!(task.active_attempts.is_empty());
                    break;
                }
                assert_eq!(task.status, crate::AgenticTaskStatus::CancelRequested);
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }).await.unwrap_or_else(|error| panic!(
            "original withdrawal owner must settle after authenticated physical exit: {error:?}; reactor_health={:?}",
            services.event_reactor_health(),
        ));
        assert_eq!(services.goal_store().get(&goal_id).unwrap(), goal_before);
    }
    pub(super) async fn finish_only(&self) {
        self.withdraw().await;
        self.process
            .command(serde_json::json!({"finish":true}))
            .await
            .unwrap();
    }
    pub(super) async fn read(&self, request: &str, path: &str) -> String {
        self.process
            .execute_tool(
                request,
                "read_file",
                &serde_json::json!({"path":path,"complete":true}).to_string(),
            )
            .await
            .unwrap()
    }
    pub(super) async fn wait_exit(&self) {
        self.process.wait_exit().await;
    }
    pub(crate) async fn wait_terminal(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let graph = self
                    .process
                    .services
                    .graph_state_store()
                    .load(self.packet.graph_id())
                    .unwrap();
                if graph
                    .node_statuses
                    .values()
                    .all(|status| status.is_terminal())
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    pub(super) async fn cancel(&self) {
        let runtime = self.process.services.agent_runtime();
        let snapshot = runtime.get(self.packet.agent_id()).unwrap();
        let receipt = runtime
            .command(harness_contract::agent::AgentCommandRequest {
                command_id: format!("physical-cancel:{}", self.packet.run_id()),
                agent_id: self.packet.agent_id().into(),
                expected_revision: snapshot.revision,
                command: harness_contract::agent::AgentCommand::Cancel,
                input: None,
            })
            .await;
        assert!(receipt.accepted, "{receipt:?}");
    }
    pub(super) fn assert_host_read(&self, request: &str) {
        assert_process_delivery(&self.process.services, &self.packet, request);
    }
}

#[cfg(test)]
#[tokio::test]
async fn r1_709_legal_process_withdrawal_reaps_and_retires_without_worker_deadline() {
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::WorkspaceReadOnly,
        "709-legal-withdrawal",
    ).await;
    let worker = fixture.dispatch("709-legal-worker", vec!["read:.".into()], false, false, true).await;
    worker.withdraw_and_require_retirement_709().await;
}

const REAL_NATIVE_DEFINITION: &str = "workspace/r1/native-sources";

#[cfg(test)]
pub(super) async fn exercise_physical_cold_optional_goal(scenario: R1ProofScenario) {
    assert!(matches!(
        scenario,
        R1ProofScenario::OptionalSharedCheckpoint | R1ProofScenario::PhysicalAfterA
    ));
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("business");
    std::fs::create_dir(&workspace).unwrap();
    let blobs = root.path().join("physical-cold-artifacts");
    let repository = Arc::new(crate::EphemeralArtifactRepository::default());
    let artifacts = Arc::new(
        crate::ArtifactStore::new(
            &blobs,
            repository.clone(),
            crate::ArtifactStoreConfig::default(),
        )
        .unwrap(),
    );
    let host = Arc::new(ReviewReadHost::default());
    host.cold_artifacts
        .set((blobs, repository))
        .unwrap_or_else(|_| panic!("cold artifact recipe already installed"));
    let services = RuntimeServices::test_builder(root.path().join("home"), &workspace)
        .artifact_store(artifacts)
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services.clone(),
        host,
        false,
        false,
        DelegatedEffectCase::Review,
        false,
        PhysicalDrainTiming::AfterGoal,
        scenario,
    ))
    .await;
    services.shutdown_maintenance().await;
}

fn assert_other_processes_host_only(services: &RuntimeServices, opaque: &AgentTaskPacket) {
    let mut pending = vec!["root-agentic-execution".to_string()];
    let mut visited = std::collections::BTreeSet::new();
    let mut opaque_runs = Vec::new();
    while let Some(graph_id) = pending.pop() {
        if !visited.insert(graph_id.clone()) {
            continue;
        }
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        pending.extend(
            services
                .graph_state_store()
                .child_links(&graph_id)
                .unwrap()
                .into_iter()
                .map(|link| link.child_execution_id),
        );
        for node in &graph.nodes {
            if node.kind != ExecutionNodeKind::AgentTask {
                continue;
            }
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
            if matches!(
                packet.binding.as_ref().map(|binding| &binding.executor),
                Some(harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. })
            ) && !crate::agent_runtime::authenticated_run_lifecycle(
                services.event_store(),
                &packet,
            )
            .unwrap()
            .no_direct_effects()
            {
                opaque_runs.push(packet.run_id().to_string());
            }
        }
    }
    assert_eq!(
        opaque_runs,
        vec![opaque.run_id().to_string()],
        "opaque optional must be the target, not an old RW producer/reviewer"
    );
}

#[cfg(test)]
#[inline(never)]
fn prepare_physical_lineage_history<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    host: &'fixture Arc<ReviewReadHost>,
    root_actor: &'fixture AgentActorBinding,
    content_ref: &'fixture str,
    review: &'fixture AgentActionEnvelope,
) -> std::pin::Pin<Box<impl std::future::Future<Output = Vec<AgentTaskPacket>> + 'fixture>> {
    Box::pin(async move {
        let historical = start_r1_scoped_goal_worker(
            services,
            root_actor,
            content_ref,
            "physical-historical-attempt",
            false,
            false,
        )
        .await;
        let old = historical.packet.clone();
        historical.reader.finish().await;
        let current_graph = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let program = services
                    .agent_action_service()
                    .project(&root_actor.program_id)
                    .unwrap();
                let task = &program.tasks[&historical.task_ref];
                if let Some(attempt) = task
                    .active_attempts
                    .values()
                    .find(|attempt| attempt.execution_id != old.graph_id())
                {
                    break attempt.execution_id.clone();
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect(
            "the original failed-unsubmitted attempt must be retried by its original Task owner",
        );
        let process = wait_real_process(services, &current_graph).await;
        let current = process.packet.clone();
        assert_ne!(old.run_id(), current.run_id());
        assert!(current.attempt > old.attempt);
        let actor = services
            .resolve_agent_action_actor(
                &ExecutionParentBinding {
                    execution_id: current.graph_id().into(),
                    node_id: current.node_id().into(),
                },
                None,
            )
            .await
            .unwrap();
        let claimed = process
            .action(&AgentActionEnvelope {
                action_id: "physical-history-new-claim".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: historical.task_ref.clone(),
                    reason: None,
                }),
            })
            .await
            .unwrap();
        assert_eq!(
            claimed.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{claimed:?}"
        );
        process
            .execute_tool(
                "physical-historical-new-read",
                "evidence_retrieve",
                &serde_json::json!({"evidence_ref":content_ref}).to_string(),
            )
            .await
            .unwrap();
        let retired = R1ScopedReviewer {
            actor,
            packet: current.clone(),
            reader: process,
            task_ref: historical.task_ref.clone(),
            reason_ref: content_ref.into(),
        };
        retired.finish(services).await;
        let program = services
            .agent_action_service()
            .project(&root_actor.program_id)
            .unwrap();
        assert!(program.tasks[&historical.task_ref]
            .active_attempts
            .is_empty());
        let scratch = process_control_directory(services, &current);
        let mut spec = crate::ProcessJsonlSpec::new(
            "command:r1-controlled-process",
            ".cowd/fixtures/r1-worker.py",
            vec![scratch.parent().unwrap().to_string_lossy().into_owned()],
        );
        spec.sandbox_profile = crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly;
        spec.command_digest = crate::ProcessJsonlSpec::digest_for(
            &spec.command_ref,
            &spec.executable,
            &spec.args,
            None,
            &spec.environment_refs,
            spec.sandbox_profile,
            &spec.transport_limits,
        );
        let fixture = PhysicalProcessFixture {
            services: services.0.clone(),
            host: host.clone(),
            spec,
            definition: REAL_PROCESS_DEFINITION.into(),
            _storage_root: None,
        };
        let gate = services.agent_runtime().hold_next_process_run_admission();
        let zero = fixture
            .dispatch(
                "physical-zero-head",
                vec!["read:.".into()],
                false,
                false,
                false,
            )
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(30), gate.entered.notified())
            .await
            .unwrap();
        let lifecycle = zero.lifecycle();
        assert!(!lifecycle.no_direct_effects());
        assert_eq!(lifecycle.sources.len(), 1);
        assert_eq!(
            lifecycle.sources[0].expected_revision, 0,
            "real new Agent stream must be pinned even without lifecycle events"
        );
        let mut negative = review.clone();
        negative.action_id.push_str(":real-zero-head");
        let refused = services.submit_agent_action(&negative).await;
        let reason = match refused {
            Err(error) => error,
            Ok(result) => {
                assert_eq!(
                    result.status,
                    harness_contract::agent_action::AgentActionStatus::Rejected
                );
                result.error.unwrap().message
            }
        };
        assert!(
            reason.contains("opaque Process history"),
            "a missing admission must reach the physical predicate: {reason}"
        );
        gate.release.notify_one();
        zero.process.started().await.unwrap();
        let live = zero.lifecycle();
        assert!(live.no_direct_effects());
        assert!(live.sources[0].expected_revision > 0);
        let rejected_stream = "agent:physical-zero-cut-rejected";
        let mut expected_streams = lifecycle.sources;
        expected_streams.push(crate::ExpectedStreamRevision {
            stream_id: rejected_stream.into(),
            expected_revision: 0,
        });
        let result = services
            .event_store()
            .append_transaction(crate::AppendTransactionRequest {
                transaction_id: "physical-zero-cut-must-fail".into(),
                expected_streams,
                events: vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: rejected_stream.into(),
                        scope: crate::RuntimeEventScope::Agent,
                        kind: "fixture.zero_cut_never_committed".into(),
                        status: None,
                        actor: None,
                        refs: vec![],
                        payload: serde_json::json!({}),
                    },
                    idempotency_key: None,
                    schema_version: 1,
                }],
            });
        assert!(
            matches!(result, Err(crate::runtime_event_store::RuntimeEventStoreError::StaleRevision {
        stream_id, expected:0, actual,
    }) if stream_id == live.sources[0].stream_id && actual > 0),
            "the actual admission must invalidate precisely the original zero-head CAS"
        );
        assert!(services
            .event_store()
            .list_stream(rejected_stream)
            .unwrap()
            .is_empty());
        zero.finish().await;
        vec![old, current, zero.packet]
    })
}

#[cfg(test)]
fn assert_physical_lineage_history_cut(
    services: &ReviewedFixtureServices,
    history: &[AgentTaskPacket],
) {
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let binding = goal.execution_binding.as_ref().unwrap();
    let program = services
        .agent_action_service()
        .project(&binding.agentic_program_id)
        .unwrap();
    let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
    let snapshot = crate::agentic::review_evidence::proof_input_snapshot(
        services.event_store(),
        &program,
        Some(&goal),
        proof,
        crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(services.artifact_store().as_ref()),
            history: services.session_history_reader().as_deref(),
        },
    )
    .unwrap();
    let mut pending = vec![binding.root_execution_id.clone()];
    let mut visited = std::collections::BTreeSet::new();
    let mut packets = Vec::new();
    while let Some(graph_id) = pending.pop() {
        if !visited.insert(graph_id.clone()) {
            continue;
        }
        let graph = services.graph_state_store().load(&graph_id).unwrap();
        pending.extend(
            services
                .graph_state_store()
                .child_links(&graph_id)
                .unwrap()
                .into_iter()
                .map(|link| link.child_execution_id),
        );
        for node in &graph.nodes {
            if node.kind != ExecutionNodeKind::AgentTask {
                continue;
            }
            let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
            let lifecycle =
                crate::agent_runtime::authenticated_run_lifecycle(services.event_store(), &packet)
                    .unwrap();
            assert!(
                lifecycle.no_direct_effects(),
                "all real related participants must be Host-only before healthy proof"
            );
            for source in lifecycle.sources {
                assert!(snapshot
                    .sources
                    .iter()
                    .any(|cut| cut.stream_id == source.stream_id
                        && cut.expected_revision == source.expected_revision));
            }
            packets.push(packet);
        }
    }
    assert!(
        packets.len() >= history.len() + 2,
        "history plus original producer and independent reviewer must all be covered"
    );
    for packet in history {
        assert!(packets.iter().any(|candidate| candidate == packet));
        assert!(!proof.input_set.as_ref().unwrap().packet_digests.contains_key(packet.graph_id()),
            "history must be covered before the selected-material continue, not by adding it to proof inputs");
    }
    println!(
        "PHYSICAL_LINEAGE_CUT real_attempts={} original_history={} all_agent_heads_pinned=true",
        packets.len(),
        history.len()
    );
}

#[cfg(test)]
pub(super) async fn exercise_physical_native_narrow_read() {
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "physical-native-migration",
    )
    .await;
    let services = &fixture.services;
    let wire = install_physical_native_wire(services).await;

    let envelope = |id: &str, action| {
        let mut envelope = root(id, action);
        envelope.actor.root_execution_id = Some("root-agentic-execution".into());
        envelope.actor.permission_ceiling = Some(PermissionMode::ReadOnly);
        envelope
    };
    exercise_physical_native_narrow_read_on(&fixture, wire, envelope).await;
}

#[cfg(test)]
async fn install_physical_native_wire(services: &Arc<RuntimeServices>) -> RootWireServer {
    use harness_contract::agent::*;
    let wire = RootWireServer::start().await;
    services
        .provider_registry()
        .replace_with_context_windows(
            crate::config::ProvidersConfig {
                providers: std::collections::HashMap::from([(
                    "r1-native-loopback".into(),
                    crate::config::ProviderConfig {
                        name: "r1-native-loopback".into(),
                        base_url: wire.endpoint.clone(),
                        api_key: "local-fixture".into(),
                        models: vec!["r1-native-wire".into()],
                        protocol: Some("completions".into()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            },
            std::collections::BTreeMap::from([("r1-native-wire".into(), 262_144)]),
        )
        .unwrap();
    assert_eq!(
        services
            .provider_registry()
            .pin()
            .configured_context_window("r1-native-wire"),
        Some(262_144)
    );
    let resources = services.provider_resource_config().read().unwrap().clone();
    services
        .replace_provider_resource_config(resources)
        .unwrap();
    let registry = services.definition_registry();
    let base = registry
        .resolve_agent(
            &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
            RevisionSelector::LatestApprovedStable,
        )
        .unwrap();
    let mut manifest = base.revision.manifest.clone();
    manifest.definition_id =
        AgentDefinitionId::new(DefinitionScope::Workspace, "r1/native-narrow-physical").unwrap();
    manifest.executor = AgentExecutorPolicy::CowdNative;
    manifest.model_policy.allowed_models = vec!["r1-native-wire".into()];
    manifest.model_policy.fallback_allowed = false;
    let revision = registry
        .agents()
        .store_revision(manifest, &base.agent_markdown)
        .unwrap();
    registry
        .agents()
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: revision.revision.revision_ref.clone(),
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "fixture:approved-native-sources".into(),
            },
            content_digest: revision.revision.content_digest.clone(),
        })
        .unwrap();
    services.refresh_definition_catalog().unwrap();
    wire
}

#[cfg(test)]
async fn exercise_physical_native_narrow_read_on(
    fixture: &PhysicalProcessFixture,
    wire: RootWireServer,
    envelope: impl Fn(&str, AgentAction) -> AgentActionEnvelope,
) {
    use harness_contract::agent::*;
    let services = &fixture.services;
    let team = services
        .submit_agent_action(&envelope(
            "native-narrow-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Fresh approved Native narrow read".into(),
                mission: "Read only one authorized file".into(),
                objective: None,
            }),
        ))
        .await
        .unwrap();
    let member = services
        .submit_agent_action(&envelope(
            "native-narrow-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.changed_refs[0].clone(),
                role: "Native narrow reader".into(),
                mission: "Use actual bounded Native tool reads".into(),
                required_capabilities: vec!["read".into()],
                existing_agent_ref: None,
                definition_ref: Some("workspace/r1/native-narrow-physical".into()),
                model_profile_ref: None,
                expertise_hints: vec![],
                execution_requirements: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        member.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{member:?}"
    );
    let publication = envelope(
        "native-narrow-task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team.changed_refs[0].clone(),
            title: "Native explicit migration".into(),
            objective: "Read out/a, refuse sibling".into(),
            acceptance: "Respect the original lexical lease".into(),
            criterion_source: Default::default(),
            acceptance_checks: vec![],
            required_capabilities: vec!["read".into()],
            depends_on: vec![],
            obligation_refs: vec![],
            purpose: harness_contract::agent_action::TaskPurpose::Exploration,
            execution_requirements: vec![],
            expertise_hints: vec![],
        }),
    );
    let published = services.submit_agent_action(&publication).await.unwrap();
    let task_ref = published.changed_refs[0].clone();
    let reply = |id: &str, name: &str, input: serde_json::Value, content: Option<&str>| {
        serde_json::json!({
            "role":"assistant", "content":content,
            "tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}}],
        })
    };
    {
        let mut replies = wire.replies.lock().unwrap();
        replies.push_back(reply(
            "native-narrow-claim",
            "task_claim",
            serde_json::json!({"task_ref":task_ref}),
            None,
        ));
        replies.push_back(reply(
            "native-narrow-allowed",
            "read_file",
            serde_json::json!({"path":"out/a","complete":true}),
            None,
        ));
        replies.push_back(reply(
            "native-narrow-forbidden",
            "read_file",
            serde_json::json!({"path":"out/physical-sentinel.txt","complete":true}),
            None,
        ));
        replies.push_back(reply(
            "native-narrow-submit",
            "task_submit",
            serde_json::json!({
                "task_ref":task_ref,"artifact_refs":[],"evidence_refs":[],"unresolved":[],
                "deliverable":{"block_index":0,"kind":"report","title":"Native bounded read"},
            }),
            Some(
                "Read the granted file. The sibling was refused by the original resource ceiling.",
            ),
        ));
        replies.push_back(
            serde_json::json!({"role":"assistant","content":"Native bounded read complete."}),
        );
    }
    let receipts = services
        .dispatch_agentic_followups(
            &publication,
            AgenticDispatchContext {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                model_lease: "r1-native-wire".into(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: vec!["read:out/a".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(receipts.len(), 1);
    let graph = services
        .graph_state_store()
        .load(&receipts[0].graph_id)
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
    assert_eq!(
        packet.binding.as_ref().unwrap().executor,
        AgentExecutorPolicy::CowdNative
    );
    wait_admitted_graph_terminal(services, &packet).await;
    let events = services
        .event_store()
        .list_stream("session:session-dispatch")
        .unwrap();
    let material = events
        .iter()
        .filter(|event| event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
        .filter_map(|event| crate::context_evidence::decode_read_delivery(&event.payload).ok())
        .find(|material| material.tool_invocation_id == "native-narrow-allowed")
        .expect("actual Native narrow read delivery");
    assert_native_task_material_wire(services, &packet, &material, &wire.requests).await;
    assert!(!events
        .iter()
        .filter(|event| event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
        .filter_map(|event| crate::context_evidence::decode_read_delivery(&event.payload).ok())
        .any(|material| material.tool_invocation_id == "native-narrow-forbidden"));
    assert!(!fixture
        .host
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|call| call.input.contains("out/physical-sentinel.txt")));
    eprintln!("Own809_NATIVE_NARROW {}", serde_json::json!({
        "graph":format!("{:?}", services.graph_state_store().load(packet.graph_id())),
        "wire":wire.actual_request_diagnostic(),
        "queued_reply_count":wire.replies.lock().unwrap().len(),
    }));
    let requests = wire.requests.lock().unwrap();
    assert!(
        requests.iter().any(|body| {
            let value: serde_json::Value = serde_json::from_slice(body).unwrap();
            value["messages"].as_array().is_some_and(|messages| {
                messages.iter().any(|message| {
                    message["tool_call_id"] == "native-narrow-forbidden"
                        && message["role"] == "tool"
                })
            })
        }),
        "the actual Native model continuation must observe the refused sibling read"
    );
    println!("PHYSICAL_NARROW_READ fresh approved Native actual allowed read and sibling denial");
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum R1ResourceBackend {
    Native,
    Process,
}

#[cfg(test)]
async fn dispatch_native_resource_worker(
    fixture: &PhysicalProcessFixture,
    wire: &RootWireServer,
    name: &str,
    content: &str,
    hold_read: bool,
) -> PhysicalWorker {
    use harness_contract::agent::AgentExecutorPolicy;
    let services = &fixture.services;
    let envelope = |suffix: &str, action| {
        let mut envelope = root(&format!("{name}:{suffix}"), action);
        envelope.actor.root_execution_id = Some("root-agentic-execution".into());
        envelope.actor.permission_ceiling = Some(PermissionMode::DangerFullAccess);
        envelope
    };
    let team = services
        .submit_agent_action(&envelope(
            "team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: name.into(),
                mission: "Observe actual Native resource ownership".into(),
                objective: None,
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        team.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{team:?}"
    );
    let member = services
        .submit_agent_action(&envelope(
            "agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.changed_refs[0].clone(),
                role: "Approved Native resource worker".into(),
                mission: "Claim, write the granted target and submit".into(),
                required_capabilities: vec!["read".into(), "write".into()],
                existing_agent_ref: None,
                definition_ref: Some("workspace/r1/native-narrow-physical".into()),
                model_profile_ref: None,
                expertise_hints: vec![],
                execution_requirements: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        member.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{member:?}"
    );
    let publication = envelope(
        "publish",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team.changed_refs[0].clone(),
            title: "Native physical resource lifetime".into(),
            objective: "Write out/a under the admitted original resource ceiling".into(),
            acceptance: "The original Tools write and receipt must finish truthfully".into(),
            criterion_source: Default::default(),
            acceptance_checks: vec![],
            required_capabilities: vec!["read".into(), "write".into()],
            depends_on: vec![],
            obligation_refs: vec![],
            purpose: harness_contract::agent_action::TaskPurpose::Exploration,
            execution_requirements: vec![],
            expertise_hints: vec![],
        }),
    );
    let published = services.submit_agent_action(&publication).await.unwrap();
    assert_eq!(
        published.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{published:?}"
    );
    let task_ref = published.changed_refs[0].clone();
    let reply = |suffix: &str, tool: &str, input: serde_json::Value, body: Option<&str>| {
        serde_json::json!({
            "role": "assistant", "content": body,
            "tool_calls": [{"index": 0, "id": format!("{name}:{suffix}"), "type": "function",
                "function": {"name": tool, "arguments": input.to_string()}}],
        })
    };
    {
        let mut replies = wire.replies.lock().unwrap();
        assert!(replies.is_empty());
        replies.push_back(reply(
            "claim",
            "task_claim",
            serde_json::json!({"task_ref": task_ref}),
            None,
        ));
        if hold_read {
            replies.push_back(reply(
                "held-read",
                "read_file",
                serde_json::json!({"path": "out/a", "complete": true}),
                None,
            ));
        }
        replies.push_back(reply(
            "write",
            "edit",
            serde_json::json!({"action": "write", "path": "out/a", "content": content}),
            None,
        ));
        replies.push_back(reply("submit", "task_submit", serde_json::json!({
            "task_ref": task_ref, "artifact_refs": [], "evidence_refs": [], "unresolved": [],
            "deliverable": {"block_index": 0, "kind": "report", "title": "Actual resource write"},
        }), Some("The granted out/a was written through the original Tools executor. This report records the completed physical operation.")));
        replies.push_back(serde_json::json!({"role": "assistant", "content": "The actual bounded write and Task submission are complete."}));
    }
    let dispatched = services
        .dispatch_agentic_followups(
            &publication,
            AgenticDispatchContext {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                model_lease: "r1-native-wire".into(),
                permission_ceiling: PermissionMode::WorkspaceWrite,
                resource_scopes: r1_resource_write_scopes(),
            },
        )
        .await
        .unwrap();
    assert_eq!(dispatched.len(), 1);
    assert_eq!(dispatched[0].agent_ref, member.changed_refs[0]);
    let graph = services
        .graph_state_store()
        .load(&dispatched[0].graph_id)
        .unwrap();
    let node = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::AgentTask)
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
    assert_eq!(
        packet.binding.as_ref().unwrap().executor,
        AgentExecutorPolicy::CowdNative
    );
    assert_eq!(packet.permission_ceiling, PermissionMode::WorkspaceWrite);
    assert!(packet.allowed_tools.iter().any(|tool| tool == "edit"));
    PhysicalWorker {
        process: RealProcessSession {
            services: services.clone(),
            packet: packet.clone(),
        },
        packet,
        task_ref,
        root_actor: publication.actor,
    }
}

#[cfg(test)]
fn r1_resource_write_scopes() -> Vec<String> {
    vec![
        "read:.".into(),
        "write:.cowd/checkpoints".into(),
        "write:out/a".into(),
    ]
}

#[cfg(test)]
async fn r1_resource_ticket_and_binding(
    services: &RuntimeServices,
    packet: &AgentTaskPacket,
) -> (
    crate::execution_core::graph::NodeExecutionTicket,
    crate::execution_core::graph::ExecutionNodeBinding,
) {
    use crate::execution_core::graph::NodeExecutionTicket;
    let graph = services
        .graph_state_store()
        .load(packet.graph_id())
        .unwrap();
    let node = graph
        .nodes
        .iter()
        .find(|node| node.id == packet.node_id())
        .unwrap();
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node.id.clone(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: graph.recovery_cursor.node_attempts[&node.id],
        idempotency_key: packet.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    let binding = services
        .execution_supervisor()
        .active_binding_for_ticket_for_test(&ticket)
        .await
        .expect("the actual admitted AgentTask ticket must still own its lease");
    assert_eq!(binding.attempt, ticket.attempt);
    assert!(!binding.resource_lease_refs.is_empty());
    (ticket, binding)
}

#[cfg(test)]
async fn assert_r1_foreign_binding_retains_owner(
    services: &RuntimeServices,
    ticket: &crate::execution_core::graph::NodeExecutionTicket,
    current: &crate::execution_core::graph::ExecutionNodeBinding,
    foreign: &crate::execution_core::graph::ExecutionNodeBinding,
) {
    use crate::execution_core::graph::{ExecutionResourceKind, ExecutionRunnerError};
    assert_ne!(foreign, current);
    let before = services.graph_state_store().load(&ticket.graph_id).unwrap();
    let graph_events = services
        .event_store()
        .list_stream(&ticket.graph_id)
        .unwrap();
    assert!(!graph_events.is_empty());
    let graph_events = serde_json::to_value(graph_events).unwrap();
    let node_stream = format!("{}:node:{}", ticket.graph_id, ticket.node_id);
    let events =
        serde_json::to_value(services.event_store().list_stream(&node_stream).unwrap()).unwrap();
    let held_agent = services
        .resource_manager()
        .snapshot(&ExecutionResourceKind::Agent)
        .unwrap();
    let held_tool = services
        .resource_manager()
        .snapshot(&ExecutionResourceKind::Tool)
        .unwrap();
    let rejected = services
        .execution_supervisor()
        .reject_stale_binding_for_test(&ticket.graph_id, &ticket.node_id, foreign)
        .await;
    assert!(
        matches!(rejected, Err(ExecutionRunnerError::CommandSuperseded { ref node_id }) if node_id == &ticket.node_id)
    );
    assert_eq!(
        services.graph_state_store().load(&ticket.graph_id).unwrap(),
        before
    );
    assert_eq!(
        serde_json::to_value(
            services
                .event_store()
                .list_stream(&ticket.graph_id)
                .unwrap()
        )
        .unwrap(),
        graph_events
    );
    assert_eq!(
        serde_json::to_value(services.event_store().list_stream(&node_stream).unwrap()).unwrap(),
        events
    );
    assert_eq!(
        services
            .execution_supervisor()
            .active_binding_for_ticket_for_test(ticket)
            .await
            .as_ref(),
        Some(current)
    );
    for (kind, held) in [
        (ExecutionResourceKind::Agent, held_agent),
        (ExecutionResourceKind::Tool, held_tool),
    ] {
        let retained = services.resource_manager().snapshot(&kind).unwrap();
        assert_eq!(retained.active_leases, held.active_leases);
        assert_eq!(retained.queued_waiters, held.queued_waiters);
    }
}

#[cfg(test)]
fn assert_r1_resource_effect_pending(
    services: &RuntimeServices,
    request: &crate::RuntimeToolExecutionRequest,
) {
    let events = services
        .event_store()
        .list_stream(&format!("execution-effect:{}", request.idempotency_key))
        .unwrap();
    assert!(events
        .iter()
        .any(|event| event.kind == "execution.effect.intent"));
    assert!(!events
        .iter()
        .any(|event| event.kind == "execution.effect.receipt"));
    let parent = request.parent_execution.as_ref().unwrap();
    let receipts = services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            &parent.execution_id,
            &parent.node_id,
            request.parent_execution_attempt.unwrap(),
        )
        .unwrap();
    assert!(!receipts
        .iter()
        .any(|receipt| receipt.outcome.tool_use_id == request.tool_use_id));
}

#[cfg(test)]
async fn wait_r1_resource_drain(services: &RuntimeServices) {
    use crate::execution_core::graph::ExecutionResourceKind;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let snapshots = [ExecutionResourceKind::Agent, ExecutionResourceKind::Tool].map(|kind|
                services.resource_manager().snapshot(&kind).unwrap());
            if snapshots.iter().all(|snapshot| snapshot.active_leases == 0 && snapshot.queued_waiters == 0) {
                assert_eq!(services.tool_execution_plane().stats().active, 0);
                assert_eq!(services.scope_locks().active_lease_count().unwrap(), 0);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("live original owners must return exact Agent/Tool capacity and waiters without being dropped");
}

#[cfg(test)]
pub(crate) async fn exercise_r1_backend_resource_lifetime_for_test(backend: R1ResourceBackend) {
    use crate::execution_core::graph::{
        ExecutionResourceKind, ResourceAcquireError, ResourceQuota, ScopeLockError, ScopeLockMode,
        ScopeLockRequest, ScopedResource,
    };
    use std::time::Duration;
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "backend-resource-lifetime",
    )
    .await;
    let services = &fixture.services;
    let _cleanup = RealFixtureCleanup {
        services: services.clone(),
        host: fixture.host.clone(),
    };
    let wire = if backend == R1ResourceBackend::Native {
        Some(install_physical_native_wire(services).await)
    } else {
        None
    };
    services
        .resource_manager()
        .update_quota(
            &ExecutionResourceKind::Tool,
            ResourceQuota::new(1, 1, 1).unwrap(),
        )
        .unwrap();
    wait_r1_resource_drain(services).await;
    let hold = Arc::new(ReviewWriteHold::default());
    fixture.host.held_write.set(hold.clone()).unwrap();
    for path in ["out/disjoint", "independent-probe/control.txt"] {
        let target = services.workspace_root().join(path);
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, "unchanged scope control").unwrap();
    }
    let first = if let Some(wire) = &wire {
        dispatch_native_resource_worker(
            &fixture,
            wire,
            "backend-resource-first",
            "first actual backend write",
            false,
        )
        .await
    } else {
        fixture
            .dispatch(
                "backend-resource-first",
                r1_resource_write_scopes(),
                true,
                false,
                true,
            )
            .await
    };
    let mut process_write =
        if backend == R1ResourceBackend::Process {
            let process = first.process.clone();
            Some(tokio::spawn(async move {
                process.execute_tool("backend-resource-first:write", "edit", &serde_json::json!({
                "action": "write", "path": "out/a", "content": "first actual backend write",
            }).to_string()).await
            }))
        } else {
            None
        };
    tokio::time::timeout(Duration::from_secs(30), hold.entered.notified())
        .await
        .expect("the original Native/Process Host must enter the physical post-write barrier");
    let expected_backend = match backend {
        R1ResourceBackend::Native => crate::agent_run_handle::AgentBackendKind::InProcess,
        R1ResourceBackend::Process => crate::agent_run_handle::AgentBackendKind::ProcessJsonl,
    };
    assert_eq!(
        services
            .agent_runtime()
            .get(first.packet.agent_id())
            .unwrap()
            .backend,
        expected_backend
    );
    if let Some(wire) = &wire {
        assert!(wire.requests.lock().unwrap().len() >= 2);
    } else {
        assert!(first.lifecycle().no_direct_effects());
    }
    assert_eq!(
        std::fs::read_to_string(services.workspace_root().join("out/a")).unwrap(),
        "first actual backend write"
    );
    let request = fixture
        .host
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|request| {
            request.tool_name == "edit"
                && request.parent_execution.as_ref().is_some_and(|parent| {
                    parent.execution_id == first.packet.graph_id()
                        && parent.node_id == first.packet.node_id()
                })
        })
        .cloned()
        .expect("actual backend-bound write request");
    assert_eq!(request.parent_execution_attempt, Some(first.packet.attempt));
    assert_eq!(
        request
            .step_binding
            .as_ref()
            .unwrap()
            .snapshot
            .identity
            .run_ref
            .as_deref(),
        Some(first.packet.run_id())
    );
    assert_r1_resource_effect_pending(services, &request);
    let (ticket, binding) = r1_resource_ticket_and_binding(services, &first.packet).await;
    let mut wrong_attempt = binding.clone();
    wrong_attempt.attempt = binding.attempt.checked_add(1).unwrap();
    assert_r1_foreign_binding_retains_owner(services, &ticket, &binding, &wrong_attempt).await;
    let mut wrong_lease = binding.clone();
    wrong_lease
        .resource_lease_refs
        .push("foreign-resource-lease".into());
    assert_r1_foreign_binding_retains_owner(services, &ticket, &binding, &wrong_lease).await;
    let held_tool = services
        .resource_manager()
        .snapshot(&ExecutionResourceKind::Tool)
        .unwrap();
    assert_eq!(held_tool.active_leases, 1);
    assert_eq!(held_tool.queued_waiters, 0);
    let held_agent = services
        .resource_manager()
        .snapshot(&ExecutionResourceKind::Agent)
        .unwrap();
    assert_eq!(held_agent.active_leases, 1);
    assert_eq!(held_agent.queued_waiters, 0);
    let claimed = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_eq!(
        claimed.tasks[&first.task_ref].claim_execution_id.as_deref(),
        Some(first.packet.graph_id())
    );
    assert_eq!(
        claimed.tasks[&first.task_ref].claim_generation,
        u64::from(first.packet.attempt)
    );
    assert_eq!(
        claimed.tasks[&first.task_ref].claimant.as_deref(),
        Some(
            first
                .packet
                .agentic_binding
                .as_ref()
                .unwrap()
                .agent_id
                .as_str()
        )
    );
    let conflict = ScopeLockRequest {
        scope: ScopedResource::workspace_object(
            services
                .path_identity_resolver()
                .resolve_planned_file("out/a")
                .unwrap(),
        ),
        mode: ScopeLockMode::Write,
    };
    let same_zone = ScopeLockRequest {
        scope: ScopedResource::workspace_object(
            services
                .path_identity_resolver()
                .resolve_existing("out/disjoint")
                .unwrap(),
        ),
        mode: ScopeLockMode::Write,
    };
    let disjoint_identity = services
        .path_identity_resolver()
        .resolve_existing("independent-probe/control.txt")
        .unwrap();
    assert_eq!(
        disjoint_identity.object_kind,
        harness_contract::context::WorkspaceObjectKind::File
    );
    assert!(services
        .path_identity_resolver()
        .physical_file_identity_at_admission(&disjoint_identity)
        .unwrap()
        .is_some());
    let disjoint = ScopeLockRequest {
        scope: ScopedResource::workspace_object(disjoint_identity),
        mode: ScopeLockMode::Write,
    };
    assert!(matches!(
        services
            .scope_locks()
            .acquire(vec![same_zone], Some(Duration::from_millis(30)))
            .await,
        Err(ScopeLockError::TimedOut { .. })
    ));
    assert!(matches!(
        services
            .scope_locks()
            .acquire(vec![conflict.clone()], Some(Duration::from_millis(30)))
            .await,
        Err(ScopeLockError::TimedOut { .. })
    ));
    let disjoint_lease = services
        .scope_locks()
        .acquire(vec![disjoint], Some(Duration::from_millis(100)))
        .await
        .unwrap();
    let queued_quota = {
        let resources = services.resource_manager().clone();
        tokio::spawn(async move {
            resources
                .acquire(ExecutionResourceKind::Tool, Some(Duration::from_secs(2)))
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshot = services
                .resource_manager()
                .snapshot(&ExecutionResourceKind::Tool)
                .unwrap();
            assert_eq!(snapshot.active_leases, 1);
            if snapshot.queued_waiters == 1 {
                break;
            }
            assert_eq!(snapshot.queued_waiters, 0);
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("a disjoint scope must not bypass the held instance Tool quota");
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(3), queued_quota)
            .await
            .unwrap()
            .unwrap(),
        Err(ResourceAcquireError::TimedOut {
            kind: ExecutionResourceKind::Tool,
            ..
        })
    ));
    assert_eq!(
        services
            .resource_manager()
            .snapshot(&ExecutionResourceKind::Tool)
            .unwrap()
            .queued_waiters,
        0
    );
    drop(disjoint_lease);
    assert_eq!(
        std::fs::read_to_string(services.workspace_root().join("out/disjoint")).unwrap(),
        "unchanged scope control"
    );
    assert_eq!(
        std::fs::read_to_string(
            services
                .workspace_root()
                .join("independent-probe/control.txt")
        )
        .unwrap(),
        "unchanged scope control"
    );
    first.withdraw().await;
    tokio::time::timeout(Duration::from_secs(10), first.cancel())
        .await
        .expect("actual backend cancellation command must be bounded");
    assert!(!hold.released.load(std::sync::atomic::Ordering::Acquire));
    assert!(
        confirmed_run_exit_with_retry(services.event_store(), &first.packet)
            .is_none()
    );
    assert_r1_resource_effect_pending(services, &request);
    assert_eq!(
        services
            .resource_manager()
            .snapshot(&ExecutionResourceKind::Tool)
            .unwrap()
            .active_leases,
        1
    );
    assert_eq!(
        services
            .resource_manager()
            .snapshot(&ExecutionResourceKind::Agent)
            .unwrap()
            .active_leases,
        1
    );
    assert_eq!(
        services
            .resource_manager()
            .snapshot(&ExecutionResourceKind::Tool)
            .unwrap()
            .queued_waiters,
        0
    );
    assert_eq!(
        services
            .resource_manager()
            .snapshot(&ExecutionResourceKind::Agent)
            .unwrap()
            .queued_waiters,
        0
    );
    assert_eq!(
        services
            .execution_supervisor()
            .active_binding_for_ticket_for_test(&ticket)
            .await
            .as_ref(),
        Some(&binding)
    );
    assert!(matches!(
        services
            .scope_locks()
            .acquire(vec![conflict.clone()], Some(Duration::from_millis(30)))
            .await,
        Err(ScopeLockError::TimedOut { .. })
    ));
    let program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert!(program.tasks[&first.task_ref]
        .active_attempts
        .values()
        .any(|attempt| attempt.execution_id == first.packet.graph_id()));
    if backend == R1ResourceBackend::Process {
        assert!(first.directory().exists());
    }
    hold.release.notify_one();
    first.wait_exit().await;
    if let Some(writer) = process_write.take() {
        let response = tokio::time::timeout(Duration::from_secs(30), writer)
            .await
            .unwrap()
            .unwrap();
        if let Err(error) = response {
            assert!(
                error.contains("exited before request completed")
                    || error.contains("physically exited"),
                "{error}"
            );
        }
    }
    first.wait_terminal().await;
    wait_retired_process(services, &first.packet, &first.task_ref).await;
    let receipts = services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            first.packet.graph_id(),
            first.packet.node_id(),
            first.packet.attempt,
        )
        .unwrap();
    assert!(receipts
        .iter()
        .any(|receipt| receipt.outcome.tool_use_id == request.tool_use_id
            && receipt.outcome.tool_name == "edit"
            && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed));
    if backend == R1ResourceBackend::Process {
        assert!(!first.directory().exists());
    }
    wait_r1_resource_drain(services).await;
    let available = services
        .scope_locks()
        .acquire(vec![conflict], Some(Duration::from_millis(100)))
        .await
        .unwrap();
    drop(available);
    if let Some(wire) = &wire {
        wire.replies.lock().unwrap().clear();
    }
    let second_hold = Arc::new(ReviewWriteHold::default());
    *fixture.host.read_hold.lock().unwrap() = Some(second_hold.clone());
    let second = if let Some(wire) = &wire {
        dispatch_native_resource_worker(
            &fixture,
            wire,
            "backend-resource-successor",
            "successor actual backend write",
            true,
        )
        .await
    } else {
        fixture
            .dispatch(
                "backend-resource-successor",
                r1_resource_write_scopes(),
                true,
                false,
                true,
            )
            .await
    };
    assert_ne!(first.task_ref, second.task_ref);
    assert_ne!(first.packet.run_id(), second.packet.run_id());
    assert_ne!(first.packet.graph_id(), second.packet.graph_id());
    let successor_read = if backend == R1ResourceBackend::Process {
        let process = second.process.clone();
        Some(tokio::spawn(async move {
            process
                .execute_tool(
                    "backend-successor-held-read",
                    "read_file",
                    r#"{"path":"out/a","complete":true}"#,
                )
                .await
        }))
    } else {
        None
    };
    tokio::time::timeout(Duration::from_secs(30), second_hold.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        services
            .agent_runtime()
            .get(second.packet.agent_id())
            .unwrap()
            .backend,
        expected_backend
    );
    let (successor_ticket, successor_binding) =
        r1_resource_ticket_and_binding(services, &second.packet).await;
    let claimed = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_eq!(
        claimed.tasks[&second.task_ref]
            .claim_execution_id
            .as_deref(),
        Some(second.packet.graph_id())
    );
    assert_eq!(
        claimed.tasks[&second.task_ref].claim_generation,
        u64::from(second.packet.attempt)
    );
    assert_ne!(
        binding.resource_lease_refs,
        successor_binding.resource_lease_refs
    );
    assert_ne!(ticket.idempotency_key, successor_ticket.idempotency_key);
    assert_r1_foreign_binding_retains_owner(
        services,
        &successor_ticket,
        &successor_binding,
        &binding,
    )
    .await;
    assert!(
        confirmed_run_exit_with_retry(services.event_store(), &second.packet)
            .is_none()
    );
    second_hold.release.notify_one();
    if let Some(read) = successor_read {
        assert!(tokio::time::timeout(Duration::from_secs(30), read)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .contains("first actual backend write"));
        second.tool("edit", serde_json::json!({"action":"write","path":"out/a","content":"successor actual backend write"})).await.unwrap();
        second.finish().await;
    } else {
        wait_admitted_graph_terminal(services, &second.packet).await;
        second.wait_exit().await;
        assert_eq!(
            services
                .graph_state_store()
                .load(second.packet.graph_id())
                .unwrap()
                .node_statuses[second.packet.node_id()],
            ExecutionNodeStatus::Completed
        );
        assert_eq!(
            services
                .agent_runtime()
                .get(second.packet.agent_id())
                .unwrap()
                .status,
            harness_contract::agent::AgentStatus::Completed
        );
        let program = services
            .agent_action_service()
            .project("program-dispatch")
            .unwrap();
        assert_eq!(
            program.tasks[&second.task_ref].status,
            crate::AgenticTaskStatus::Submitted
        );
        second.withdraw().await;
        wait_retired_process(services, &second.packet, &second.task_ref).await;
    }
    assert_eq!(
        std::fs::read_to_string(services.workspace_root().join("out/a")).unwrap(),
        "successor actual backend write"
    );
    let successor_receipts = services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            second.packet.graph_id(),
            second.packet.node_id(),
            second.packet.attempt,
        )
        .unwrap();
    assert!(successor_receipts
        .iter()
        .any(|receipt| receipt.outcome.tool_name == "edit"
            && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed));
    if backend == R1ResourceBackend::Process {
        assert!(!second.directory().exists());
    }
    wait_r1_resource_drain(services).await;
    services.shutdown_maintenance().await;
}

#[cfg(test)]
pub(super) async fn exercise_r1_backend_unknown_exit_for_test() {
    use std::sync::atomic::Ordering;
    use std::time::Duration;
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "backend-resource-unknown-exit",
    )
    .await;
    let services = &fixture.services;
    let _cleanup = RealFixtureCleanup {
        services: services.clone(),
        host: fixture.host.clone(),
    };
    let worker = fixture
        .dispatch(
            "backend-resource-unknown",
            r1_resource_write_scopes(),
            true,
            false,
            true,
        )
        .await;
    worker.tool("edit", serde_json::json!({"action":"write","path":"out/a","content":"actual effect before unknown exit"})).await.unwrap();
    assert_eq!(
        std::fs::read_to_string(services.workspace_root().join("out/a")).unwrap(),
        "actual effect before unknown exit"
    );
    let receipts = services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            worker.packet.graph_id(),
            worker.packet.node_id(),
            worker.packet.attempt,
        )
        .unwrap();
    assert!(receipts
        .iter()
        .any(|receipt| receipt.outcome.tool_name == "edit"
            && receipt.outcome.status == crate::RuntimeToolExecutionStatus::Executed));
    let gate = services.agent_runtime().process_fixture_exit_gate(true);
    worker.finish_only().await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !gate.entered.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the real Process driver must reach its original exit witness gate");
    assert!(worker.directory().exists());
    assert!(
        confirmed_run_exit_with_retry(services.event_store(), &worker.packet)
            .is_none()
    );
    gate.release();
    worker.wait_terminal().await;
    assert!(
        worker.directory().exists(),
        "uncertain driver join cannot authorize private scratch deletion"
    );
    assert!(
        confirmed_run_exit_with_retry(services.event_store(), &worker.packet)
            .is_none(),
        "an actual effect receipt cannot manufacture confirmed Process exit"
    );
    let program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_ne!(program.status, crate::AgenticProgramStatus::Verified);
    assert_eq!(
        program.tasks[&worker.task_ref].status,
        crate::AgenticTaskStatus::CancelRequested
    );
    assert!(program.tasks[&worker.task_ref]
        .active_attempts
        .values()
        .any(|attempt| attempt.execution_id == worker.packet.graph_id()));
    services.shutdown_maintenance().await;
}

fn register_opaque_process_variant(services: &Arc<RuntimeServices>) -> String {
    use harness_contract::agent::*;
    let run = services
        .agent_runtime()
        .list()
        .into_iter()
        .find(|run| run.backend == crate::agent_run_handle::AgentBackendKind::ProcessJsonl)
        .unwrap();
    let graph = services.graph_state_store().load(&run.graph_id).unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(
        &graph
            .nodes
            .iter()
            .find(|node| node.id == run.node_id)
            .unwrap()
            .payload_ref,
    )
    .unwrap();
    let directory = process_control_directory(services, &packet);
    let mut spec = crate::ProcessJsonlSpec::new(
        "command:r1-opaque-process",
        ".cowd/fixtures/r1-worker.py",
        vec![directory.parent().unwrap().to_string_lossy().into_owned()],
    );
    spec.sandbox_profile = crate::config::AgentExecutorSandboxProfile::IsolatedReadWrite;
    spec.command_digest = crate::ProcessJsonlSpec::digest_for(
        &spec.command_ref,
        &spec.executable,
        &spec.args,
        None,
        &spec.environment_refs,
        spec.sandbox_profile,
        &spec.transport_limits,
    );
    services
        .register_process_jsonl_command(spec.clone())
        .unwrap();
    let registry = services.definition_registry();
    let base = registry
        .resolve_agent(
            &AgentDefinitionId::new(DefinitionScope::Workspace, "r1/controlled-process").unwrap(),
            RevisionSelector::LatestApprovedStable,
        )
        .unwrap();
    let mut manifest = base.revision.manifest.clone();
    manifest.definition_id =
        AgentDefinitionId::new(DefinitionScope::Workspace, "r1/opaque-process").unwrap();
    manifest.executor = AgentExecutorPolicy::ProcessJsonl {
        command_ref: spec.command_ref,
        command_digest: spec.command_digest,
    };
    let stored = registry
        .agents()
        .store_revision(manifest, &base.agent_markdown)
        .unwrap();
    registry
        .agents()
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: stored.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "fixture:physical-opaque".into(),
            },
            content_digest: stored.revision.content_digest,
        })
        .unwrap();
    services.refresh_definition_catalog().unwrap();
    std::fs::create_dir_all(services.workspace_root().join("out")).unwrap();
    std::fs::write(
        services.workspace_root().join("out/physical-sentinel.txt"),
        "initial sentinel",
    )
    .unwrap();
    "workspace/r1/opaque-process".into()
}

fn process_control_directory(
    services: &RuntimeServices,
    packet: &AgentTaskPacket,
) -> std::path::PathBuf {
    services
        .agent_runtime()
        .process_fixture_scratch(packet)
        .expect("exact original Process scratch")
}

async fn wait_fixture_file(path: &std::path::Path) -> Result<Vec<u8>, String> {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            match std::fs::read(path) {
                Ok(bytes) => return Ok(bytes),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    })
    .await
    .map_err(|_| format!("real Process checkpoint timed out: {}", path.display()))?
}

fn publish_fixture_file(path: &std::path::Path, value: &serde_json::Value) -> Result<(), String> {
    std::fs::create_dir_all(path.parent().ok_or("fixture path has no parent")?)
        .map_err(|error| error.to_string())?;
    let temporary = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    std::fs::write(
        &temporary,
        serde_json::to_vec(value).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    std::fs::rename(temporary, path).map_err(|error| error.to_string())
}

fn publish_immutable_action_metadata_239(
    path: &std::path::Path,
    value: &serde_json::Value,
) -> Result<(), String> {
    let pending = path.with_extension(format!("{}.pending", uuid::Uuid::new_v4()));
    publish_fixture_file(&pending, value)?;
    let published = std::fs::hard_link(&pending, path);
    let cleanup = std::fs::remove_file(&pending).map_err(|error| error.to_string());
    match published {
        Ok(()) => cleanup,
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            cleanup?;
            let original: serde_json::Value =
                serde_json::from_slice(&std::fs::read(path).map_err(|error| error.to_string())?)
                    .map_err(|error| error.to_string())?;
            if original == *value {
                Ok(())
            } else {
                Err("Process action invocation metadata conflict".into())
            }
        }
        Err(error) => {
            cleanup?;
            Err(error.to_string())
        }
    }
}

pub(super) async fn install_real_process_fixture(
    services: &Arc<RuntimeServices>,
    host: &ReviewReadHost,
) {
    install_real_process_fixture_case(
        services,
        host,
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "controlled-process",
    )
    .await;
}

async fn install_real_process_fixture_case(
    services: &Arc<RuntimeServices>,
    host: &ReviewReadHost,
    profile: crate::config::AgentExecutorSandboxProfile,
    name: &str,
) -> crate::ProcessJsonlSpec {
    use harness_contract::agent::*;
    #[cfg(test)]
    if services.session_journal_port().is_none() {
        let store = Arc::new(crate::test_support::session_store());
        store
            .create_session(&session::SessionRecord {
                session_id: "session-dispatch".into(),
                platform: "test".into(),
                chat_id: "real-process-review".into(),
                user_id: None,
                model: None,
                created_at: "2026-10-17T00:00:00Z".into(),
                last_activity: "2026-10-17T00:00:00Z".into(),
                message_count: 0,
                reset_policy: "manual".into(),
                metadata_json: None,
                input_tokens: 0,
                output_tokens: 0,
                status: "active".into(),
            })
            .await
            .unwrap();
        let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
        services
            .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports)
            .unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        sandbox_launcher::probe()
            .expect("R1 real Process fixture requires the installed sandbox launcher");
        let worker = services
            .workspace_root()
            .join(".cowd/fixtures/r1-worker.py");
        std::fs::create_dir_all(worker.parent().unwrap()).unwrap();
        std::fs::write(&worker, REAL_PROCESS_WORKER).unwrap();
        std::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    #[cfg(not(unix))]
    panic!("R1 real Process fixture requires Unix process and sandbox support");
    let mut definitions = harness_contract::agent_action::AGENT_ACTION_TOOL_IDS
        .iter()
        .map(|name| tools::RuntimeToolDefinition {
            name: (*name).into(),
            description: Some("Original Runtime Agent action".into()),
            input_schema: serde_json::json!({"type":"object"}),
            required_permission: PermissionMode::ReadOnly,
            effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                resolver_id: "runtime.agent_action".into(),
                resolver_version: 2,
            },
        })
        .collect::<Vec<_>>();
    for name in [
        "context_retrieve",
        "evidence_retrieve",
        "read_ref",
        "working_context",
        "private_note",
        "artifact_publish",
    ] {
        definitions.push(tools::RuntimeToolDefinition {
            name: name.into(), description: Some("Original Runtime evidence access".into()),
            input_schema: match name {
                "read_ref" => serde_json::json!({"type":"object", "properties":{
                    "reference":{"type":"string"}, "cursor":{"type":"string"},
                    "limit":{"type":"integer","minimum":1,"maximum":64}},
                    "required":["reference"],"additionalProperties":false}),
                "evidence_retrieve" => serde_json::json!({"type":"object", "properties":{
                    "evidence_ref":{"type":"string"}, "cursor":{"type":"string"}, "query":{"type":"string"},
                    "limit":{"type":"integer","minimum":1,"maximum":16}},
                    "required":["evidence_ref"],"additionalProperties":false}),
                _ => serde_json::json!({"type":"object"}),
            },
            required_permission: PermissionMode::ReadOnly,
            effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
                resolver_id: if name == "private_note" { "runtime.private_note" } else { "runtime.readonly" }.into(),
                resolver_version: if name == "private_note" { 2 } else { 1 },
            },
        });
    }
    definitions.push(tools::RuntimeToolDefinition {
        name: "artifact_materialize".into(),
        description: Some("Original Runtime artifact export".into()),
        input_schema: serde_json::to_value(schemars::schema_for!(
            harness_contract::content_publication::ArtifactMaterializeInput
        ))
        .unwrap(),
        required_permission: PermissionMode::WorkspaceWrite,
        effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
            resolver_id: "runtime.state_write".into(),
            resolver_version: 1,
        },
    });
    let catalog = tools::ToolCatalog::builtin()
        .with_runtime_tools(definitions)
        .unwrap();
    host.catalog
        .set(
            tools::ToolHost::new(
                "r1-real-process",
                services.workspace_root(),
                tools::ToolHostSnapshot::new(
                    Arc::new(catalog),
                    Arc::new(tools::lsp_client::LspRegistry::default()),
                    None,
                ),
            )
            .with_authorization_lease_verifier(Arc::new(
                crate::authorization_negotiator::AuthorizationNegotiator::verify_lease_signature,
            )),
        )
        .unwrap_or_else(|_| panic!("fixture catalog installed twice"));
    let scratch =
        std::env::temp_dir().join(format!("cowd-physical-control-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir(&scratch).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&scratch, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let mut spec = crate::ProcessJsonlSpec::new(
        format!("command:r1-{name}"),
        ".cowd/fixtures/r1-worker.py",
        vec![scratch.to_string_lossy().into_owned()],
    );
    spec.sandbox_profile = profile;
    spec.command_digest = crate::ProcessJsonlSpec::digest_for(
        &spec.command_ref,
        &spec.executable,
        &spec.args,
        spec.working_directory.as_deref(),
        &spec.environment_refs,
        spec.sandbox_profile,
        &spec.transport_limits,
    );
    services
        .register_process_jsonl_command(spec.clone())
        .unwrap();
    services
        .agent_runtime()
        .install_process_fixture_scratch(scratch)
        .unwrap();
    let registry = services.definition_registry();
    let base = registry
        .resolve_agent(
            &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/autonomous").unwrap(),
            RevisionSelector::LatestApprovedStable,
        )
        .unwrap();
    let mut manifest = base.revision.manifest.clone();
    manifest.definition_id =
        AgentDefinitionId::new(DefinitionScope::Workspace, format!("r1/{name}")).unwrap();
    manifest.executor = AgentExecutorPolicy::ProcessJsonl {
        command_ref: spec.command_ref.clone(),
        command_digest: spec.command_digest.clone(),
    };
    manifest.model_policy.fallback_allowed = false;
    let stored = registry
        .agents()
        .store_revision(manifest, &base.agent_markdown)
        .unwrap();
    registry
        .agents()
        .record_release_assignment(&ReleaseAssignment {
            scope: DefinitionScope::Workspace,
            revision_ref: stored.revision.revision_ref,
            channel: ReleaseChannel::Stable,
            status: ReleaseAssignmentStatus::Active,
            authorization: ReleaseAuthorization::HumanApproval {
                approval_ref: "test-fixture:r1-operator-approved-process".into(),
            },
            content_digest: stored.revision.content_digest,
        })
        .unwrap();
    services.refresh_definition_catalog().unwrap();
    spec
}

#[derive(Clone)]
struct RealProcessSession {
    services: Arc<RuntimeServices>,
    packet: AgentTaskPacket,
}

impl RealProcessSession {
    fn approve_bounded_execute_code_848(&self, request_id: &str, input: &str) {
        use harness_contract::policy::{
            ApprovalDecisionActor, ApprovalDecisionActorKind,
            ApprovalDecisionCommand, ApprovalDomain, ApprovalGrantScope, ApprovalSource,
            ApprovalSourceKind, ApprovalTimeoutPolicy, AuthorizationPath, RiskLevel,
            SubmitApprovalRequest,
        };
        use sha2::Digest;
        let snapshot = self.services.session_execution_policy(self.packet.session_id()).unwrap();
        assert_eq!(snapshot.revision, self.packet.policy_revision);
        let pin = self.services.tool_execution_host().unwrap().pin_step_catalog().unwrap();
        let value: serde_json::Value = serde_json::from_str(input).unwrap();
        let descriptor = pin.describe_effect("execute_code", &value);
        let required = crate::AuthorizationNegotiator::compile_effective_descriptor(&descriptor, input)
            .descriptor.required_permission;
        assert!(snapshot.permission_mode.permits(required));
        assert!(self.packet.permission_ceiling.permits(required));
        let invocation_id = format!("process-tool:{:x}", sha2::Sha256::digest(
            serde_json::to_vec(&(self.packet.graph_id(), self.packet.attempt, request_id)).unwrap()
        ));
        let request = crate::AuthorizationRequest {
            principal_id: format!("agent:{}", self.packet.agent_id()),
            capability: "execute_code".into(), input: input.into(),
            idempotency_key: invocation_id.clone(), effect: descriptor,
            parent_ceiling: self.packet.permission_ceiling, parent_lease_id: None,
            policy_revision: snapshot.revision,
            recovery_scope: format!("execution:{}", self.packet.graph_id()),
            context: crate::PermissionContext::default(), safe_alternatives: vec![],
        };
        let policy = crate::PermissionPolicy::new(snapshot.permission_mode)
            .with_immutable_ceiling(self.packet.permission_ceiling);
        let evaluated = crate::AuthorizationNegotiator::new().assess_effective(&policy, &request);
        assert_eq!(evaluated.assessment.path, AuthorizationPath::HumanApproval);
        assert!(evaluated.assessment.lease.is_none());
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Agent,
            session_id: Some(self.packet.session_id().into()),
            agent_id: Some(self.packet.agent_id().into()),
            team_id: None, mission_id: None, resource_ref: None,
            review_ref: None, application: None,
        };
        let context = crate::agent_in_process_worker::process_tool_approval_context(
            &self.packet, &snapshot, &evaluated.effective,
            self.services.path_identity_resolver().workspace_id(), &invocation_id);
        let risk = match evaluated.assessment.risk {
            RiskLevel::Low => harness_contract::core::TaskRisk::Low,
            RiskLevel::Medium => harness_contract::core::TaskRisk::Medium,
            RiskLevel::High => harness_contract::core::TaskRisk::High,
            RiskLevel::Critical => harness_contract::core::TaskRisk::Critical,
        };
        let queue = self.services.approval_queue();
        let pending = queue.submit_scoped_with_policy(format!("Own848:{invocation_id}"),
            SubmitApprovalRequest {
                source, context, action: "execute:execute_code".into(),
                summary: "Approve the exact bounded Process fixture call".into(),
                risk, domain: ApprovalDomain::Execution, blocks_execution: true,
                evidence_refs: vec![evaluated.effective.fingerprint.clone()],
                timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
            }, None, false, vec![ApprovalGrantScope::Once]).unwrap();
        let human = crate::security::test_human_interactive_principal();
        queue.decide(&human, ApprovalDecisionCommand {
            approval_id: pending.approval_id.clone(), approved: true, skip: false,
            reason: "Original fixture human authorizes only this pinned invocation".into(),
            scope: ApprovalGrantScope::Once,
            actor: ApprovalDecisionActor { kind: ApprovalDecisionActorKind::Human,
                actor_id: human.claims().principal_id.clone() },
            evidence_refs: vec![evaluated.effective.fingerprint],
        }).unwrap();
        let grant = queue.grant_for_approval(&pending.approval_id).unwrap();
        assert_eq!(grant.invocation_id.as_deref(), Some(invocation_id.as_str()));
        assert!(self.services.event_store().stream_revision(
            &format!("approval:{}", pending.approval_id)).unwrap() > 0);
    }

    fn failure_diagnostic(&self) -> String {
        let agent_stream = format!("agent:{}", self.packet.agent_id());
        crate::failure_evidence::collect_failure_evidence(
            self.services.event_store(),
            &[
                crate::failure_evidence::FailureEvidenceSource {
                    stream_id: &agent_stream,
                    after_sequence: 0,
                    through_sequence: u64::MAX,
                },
                crate::failure_evidence::FailureEvidenceSource {
                    stream_id: self.packet.graph_id(),
                    after_sequence: 0,
                    through_sequence: u64::MAX,
                },
            ],
            "Original Process failure is reported by caller; exact admitted agent and graph",
        ).to_string()
    }

    fn prepare(
        services: &Arc<RuntimeServices>,
        packet: &AgentTaskPacket,
        _: &crate::agent_model_selector::AgentModelSelection,
    ) -> Result<Self, String> {
        Ok(Self {
            services: services.clone(),
            packet: packet.clone(),
        })
    }

    fn ensure_not_exited(&self) -> Result<(), String> {
        if crate::agent_runtime::confirmed_run_exit(self.services.event_store(), &self.packet)?
            .is_some()
        {
            return Err("tool_effect_authority: original Process has physically exited".into());
        }
        Ok(())
    }

    async fn started(&self) -> Result<AgentTaskPacket, String> {
        let path = process_control_directory(&self.services, &self.packet).join("started.json");
        let bytes = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                self.wait_not_exited().await?;
                match std::fs::read(&path) {
                    Ok(bytes) => return Ok(bytes),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
        })
        .await
        .map_err(|_| {
            format!(
                "real Process checkpoint timed out: {}; {}",
                path.display(),
                self.failure_diagnostic()
            )
        })??;
        let packet: AgentTaskPacket =
            serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
        if packet.run_id() != self.packet.run_id()
            || packet.graph_id() != self.packet.graph_id()
            || packet.node_id() != self.packet.node_id()
            || packet.attempt != self.packet.attempt
        {
            return Err("tool_effect_authority: immutable Process packet identity differs".into());
        }
        Ok(packet)
    }

    async fn command(&self, command: serde_json::Value) -> Result<serde_json::Value, String> {
        self.wait_not_exited().await?;
        self.started().await?;
        self.wait_not_exited().await?;
        let directory = process_control_directory(&self.services, &self.packet);
        let token = uuid::Uuid::new_v4().to_string();
        publish_fixture_file(&directory.join(format!("{token}.request")), &command)?;
        let path = directory.join(format!("{token}.response"));
        // The Runtime may still have to retire the Process that owns this
        // request, and that retirement is bounded by the runtime's own
        // physical-closure budget. The fixture must not preempt that bound:
        // an in-flight control request that the runtime closes with an
        // authenticated cancellation is a legitimate terminal, and the
        // fixture can only prove it when its wait outlasts the product's
        // bound. Two closure budgets plus one budget of slack cover the
        // pre-closure phase, the bounded closure and the exit witness.
        let request_budget = crate::execution_core::graph::PHYSICAL_CLOSURE_BUDGET * 2
            + std::time::Duration::from_secs(60);
        tokio::time::timeout(request_budget, async {
            loop {
                if let Ok(bytes) = std::fs::read(&path) {
                    return serde_json::from_slice(&bytes).map_err(|error| error.to_string());
                }
                let exit = match crate::agent_runtime::confirmed_run_exit(
                    self.services.event_store(), &self.packet,
                ) {
                    Ok(exit) => exit,
                    Err(error) if error == "Agent exit source changed during authentication" => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                if exit.is_some()
                {
                    return Err(format!(
                        "tool_effect_authority: Process exited before request completed; {}",
                        self.failure_diagnostic()
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| format!("Process request did not complete: {command}"))?
    }

    async fn execute_tool(
        &self,
        request_id: &str,
        tool_name: &str,
        input: &str,
    ) -> Result<String, String> {
        if self
            .services
            .session_execution_policy(self.packet.session_id())
            .is_some_and(|policy| {
                self.packet.policy_revision != 0 && policy.revision != self.packet.policy_revision
            })
        {
            return Err(
                "ProcessJsonl packet policy revision is stale; reauthorize before continuing"
                    .into(),
            );
        }
        let graph = self
            .services
            .graph_state_store()
            .load(self.packet.graph_id())
            .map_err(|error| error.to_string())?;
        let original: AgentTaskPacket = serde_json::from_str(
            &graph
                .nodes
                .iter()
                .find(|node| node.id == self.packet.node_id())
                .ok_or("original Process node missing")?
                .payload_ref,
        )
        .map_err(|error| error.to_string())?;
        if original.attempt != self.packet.attempt {
            return Err("tool_effect_authority: immutable Process attempt differs".into());
        }
        let mut value =
            serde_json::from_str::<serde_json::Value>(input).map_err(|error| error.to_string())?;
        if tool_name == "edit" && value.get("action").is_none() {
            value["action"] = "write".into();
        }
        let response = self
            .command(serde_json::json!({
                "request_id":request_id, "tool_name":tool_name, "input":value,
            }))
            .await?;
        if let Some(error) = response.get("error").and_then(serde_json::Value::as_str) {
            return Err(error.into());
        }
        response["output"]
            .as_str()
            .map(str::to_string)
            .ok_or_else(|| format!("Process response lacks output: {response}"))
    }

    async fn wait_not_exited(&self) -> Result<(), String> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                match self.ensure_not_exited() {
                    Err(error) if error == "Agent exit source changed during authentication" => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    result => return result,
                }
            }
        }).await.map_err(|_| "Process exit snapshot authentication did not stabilize".to_string())?
    }

    async fn retire_control(&self, envelope: &AgentActionEnvelope, task_ref: &str) {
        assert_eq!(envelope.actor.execution_id.as_deref(), Some(self.packet.graph_id()));
        assert!(match &envelope.action {
            AgentAction::TaskWithdraw(input) => input.task_ref == task_ref,
            AgentAction::TaskSupersede(input) => input.task_ref == task_ref,
            _ => false,
        });
        let worker = PhysicalWorker {
            process: RealProcessSession { services: self.services.clone(), packet: self.packet.clone() },
            root_actor: envelope.actor.clone(),
            packet: self.packet.clone(),
            task_ref: task_ref.into(),
        };
        let request_id = format!("{}:invocation:retirement", envelope.action_id);
        let response = self.action_with_request_id(envelope, &request_id).await;
        match &response {
            Ok(observation) => {
                assert_eq!(observation.status, harness_contract::agent_action::AgentActionStatus::Applied,
                    "{observation:?}");
                assert_eq!(observation.action_id, envelope.action_id);
                assert_eq!(observation.program_id, envelope.actor.program_id);
                assert_eq!(observation.action, envelope.action.kind());
                assert!(!observation.duplicate);
                assert_eq!(observation.revision, worker.assert_original_control_transport(envelope));
                assert_eq!(observation.changed_refs, vec![task_ref.to_string()]);
            }
            Err(error) => {
                worker.assert_original_control_action(envelope);
                assert!(confirmed_run_exit_with_retry(self.services.event_store(), &self.packet).is_some(), "retirement requires authenticated original physical exit");
                let run = self.services.agent_runtime().get(self.packet.agent_id()).unwrap();
                assert_eq!(run.status, harness_contract::agent::AgentStatus::Cancelled);
                assert_eq!(run.failure.as_deref(), Some("agent cancelled by command"));
                assert_eq!(self.services.graph_state_store().load(self.packet.graph_id()).unwrap()
                    .node_statuses[self.packet.node_id()], ExecutionNodeStatus::Cancelled);
                assert!(worker.lifecycle().no_direct_effects());
                eprintln!("R1_772_CONTROL_RESPONSE_UNCONFIRMED {}", serde_json::json!({
                    "action_id": envelope.action_id, "request_id": request_id, "error": error,
                    "response_delivered": false, "transport_completion_proven": false,
                    "semantic_receipt": "original_agentic.action_applied",
                }));
            }
        }
    }

    async fn action(
        &self,
        envelope: &AgentActionEnvelope,
    ) -> Result<harness_contract::agent_action::AgentActionObservation, String> {
        let request_id = format!("{}:invocation:{}", envelope.action_id, uuid::Uuid::new_v4());
        self.action_with_request_id(envelope, &request_id).await
    }

    fn action_metadata_path_239(&self, request_id: &str) -> std::path::PathBuf {
        let metadata = process_control_directory(&self.services, &self.packet)
            .join("actionmeta")
            .join(format!(
                "{}.json",
                crate::context_evidence::read_sha256(
                    format!("{}:{request_id}", self.packet.graph_id()).as_bytes(),
                )
                .trim_start_matches("sha256:")
            ));
        metadata
    }

    fn prepare_action_metadata_239(
        &self,
        envelope: &AgentActionEnvelope,
        request_id: &str,
    ) -> Result<std::path::PathBuf, String> {
        self.ensure_not_exited()?;
        let metadata = self.action_metadata_path_239(request_id);
        publish_immutable_action_metadata_239(
            &metadata,
            &serde_json::json!({
                "action_id":envelope.action_id, "expected_revision":envelope.expected_revision,
                "envelope":envelope,
            }),
        )?;
        Ok(metadata)
    }

    async fn action_with_request_id(
        &self,
        envelope: &AgentActionEnvelope,
        request_id: &str,
    ) -> Result<harness_contract::agent_action::AgentActionObservation, String> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                match self.prepare_action_metadata_239(envelope, request_id) {
                    Ok(_) => return Ok(()),
                    Err(error) if error == "Agent exit source changed during authentication" => {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                    Err(error) => return Err(error),
                }
            }
        }).await.map_err(|_| "Process metadata exit snapshot did not stabilize".to_string())??;
        let action = serde_json::to_value(&envelope.action).map_err(|error| error.to_string())?;
        let name = action["kind"].as_str().ok_or("action kind missing")?;
        let output = self
            .execute_tool(request_id, name, &action["input"].to_string())
            .await?;
        serde_json::from_str(&output).map_err(|error| error.to_string())
    }

    async fn finish(&self) {
        let result = self.command(serde_json::json!({"finish":true})).await;
        if let Err(error) = result {
            assert!(
                confirmed_run_exit_with_retry(self.services.event_store(), &self.packet)
                    .is_some(),
                "{error}"
            );
        }
        self.wait_exit().await;
    }

    async fn wait_exit(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some((_, sources)) = confirmed_run_exit_with_retry(self.services.event_store(), &self.packet,)
                {
                    assert!(sources
                        .iter()
                        .any(|source| source.stream_id
                            == format!("agent:{}", self.packet.agent_id())));
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|elapsed| {
            if let Some(binding) = self.packet.agentic_binding.as_ref() {
                let task_ref = match &binding.focus {
                    harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                    | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => Some(task_ref),
                    _ => None,
                };
                if let Some(task_ref) = task_ref {
                    own832_diagnostics::capture(&self.services, &binding.program_id, task_ref,
                        "original_process_exit_timeout", Some(&self.packet));
                }
            }
            let durable = crate::execution_core::graph::ExecutionGraphStateStore::new(
                self.services.event_store().clone(),
            );
            let summarize = |graph_id: &str| match durable.load(graph_id) {
                Ok(graph) => serde_json::json!({
                    "id":graph.id,"revision":graph.revision,"statuses":graph.node_statuses,
                    "failures":graph.node_results.iter().filter_map(|(id,result)|
                        result.failure.as_ref().map(|failure|
                            serde_json::json!({"node":id,"kind":failure.kind,
                                "message":failure.message.chars().take(600).collect::<String>()}))
                    ).collect::<Vec<_>>()
                }),
                Err(error) => serde_json::json!({"error":error.to_string()}),
            };
            let lifecycle = match crate::agent_runtime::authenticated_run_lifecycle(
                self.services.event_store(), &self.packet,
            ) {
                Ok(state) => serde_json::json!({
                    "fact":state.semantic_fact(),"sources":state.sources,
                }),
                Err(error) => serde_json::json!({"error":error}),
            };
            let children = match durable.child_links(self.packet.graph_id()) {
                Ok(links) => serde_json::json!(links.iter().take(16)
                    .map(|link| summarize(&link.child_execution_id)).collect::<Vec<_>>()),
                Err(error) => serde_json::json!({"error":error.to_string()}),
            };
            let index = format!("execution-agent-receipts:{}:{}:{}",
                self.packet.graph_id(),self.packet.node_id(),self.packet.attempt);
            let receipts = match self.services.event_store().list_stream(&index) {
                Ok(events) => serde_json::json!(events.iter().rev().take(8).map(|event|
                    serde_json::json!({"kind":event.kind,"cursor":event.commit_cursor,
                        "transaction":event.transaction_id,"position":event.transaction_index,
                        "key":event.payload["idempotency_key"],"status":event.payload["outcome"]["status"]})
                ).collect::<Vec<_>>()),
                Err(error) => serde_json::json!({"error":error.to_string()}),
            };
            let confirmed = crate::agent_runtime::confirmed_run_exit(
                self.services.event_store(), &self.packet,
            ).map(|exit|exit.is_some());
            let diagnostic = serde_json::json!({
                "packet":{"graph":self.packet.graph_id(),"node":self.packet.node_id(),
                    "attempt":self.packet.attempt,"run":self.packet.run_id(),"agent":self.packet.agent_id(),
                    "sha256":crate::context_evidence::read_sha256(&serde_json::to_vec(&self.packet).unwrap())},
                "runtime":self.failure_diagnostic().chars().take(1600).collect::<String>(),
                "durable_graph":summarize(self.packet.graph_id()),"children":children,
                "lifecycle":lifecycle,"confirmed_exit":confirmed,"receipt_index":receipts,
            });
            panic!("original retained Runtime must persist the actual local exit: {elapsed:?}; R1_PROCESS_629_EXIT={diagnostic}");
        });
    }
}

async fn wait_real_process(services: &Arc<RuntimeServices>, graph_id: &str) -> RealProcessSession {
    wait_real_process_definition(services, graph_id, REAL_PROCESS_DEFINITION).await
}

async fn wait_real_process_definition(
    services: &Arc<RuntimeServices>,
    graph_id: &str,
    definition: &str,
) -> RealProcessSession {
    let graph = services.graph_state_store().load(graph_id).unwrap();
    let mut nodes = graph
        .nodes
        .iter()
        .filter(|node| node.kind == ExecutionNodeKind::AgentTask);
    let node = nodes.next().expect("one actual AgentTask");
    assert!(nodes.next().is_none());
    let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
    assert_eq!(
        packet
            .binding
            .as_ref()
            .unwrap()
            .definition_ref
            .definition_id
            .as_str(),
        definition
    );
    let binding = packet.binding.as_ref().unwrap();
    let approved = services
        .definition_registry()
        .resolve_agent(
            &binding.definition_ref.definition_id,
            harness_contract::agent::RevisionSelector::LatestApprovedStable,
        )
        .unwrap();
    assert_eq!(approved.revision.revision_ref, binding.definition_ref);
    assert_eq!(approved.revision.manifest.executor, binding.executor);
    assert!(matches!(
        packet.binding.as_ref().unwrap().executor,
        harness_contract::agent::AgentExecutorPolicy::ProcessJsonl { .. }
    ));
    let session = RealProcessSession {
        services: services.clone(),
        packet,
    };
    let actual = session.started().await.unwrap_or_else(|error| {
        if let Some(binding) = session.packet.agentic_binding.as_ref() {
            let task_ref = match &binding.focus {
                harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
                | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } => Some(task_ref),
                _ => None,
            };
            if let Some(task_ref) = task_ref {
                own832_diagnostics::capture(services, &binding.program_id, task_ref,
                    "original_process_start_failed", Some(&session.packet));
            }
        }
        panic!("original Process must start: {error}; see OWN832 snapshot");
    });
    assert_eq!(actual.agentic_binding, session.packet.agentic_binding);
    assert!(
        confirmed_run_exit_with_retry(services.event_store(), &session.packet)
            .is_none()
    );
    session
}

// The same read-only traversal backs the retirement assertions and waiting
// fixture. Graph terminal truth alone never proves original lease publication.
fn original_binding_closures_ready(
    services: &RuntimeServices,
    execution: &str,
    packet: Option<&AgentTaskPacket>,
) -> Result<Option<usize>, String> {
    let store = services.graph_state_store();
    let mut pending = vec![execution.to_string()];
    let mut seen = std::collections::BTreeSet::new();
    let mut observed = 0;
    let mut original_seen = packet.is_none();
    while let Some(graph_id) = pending.pop() {
        if !seen.insert(graph_id.clone()) {
            return Err("retirement lineage cycle".into());
        }
        let graph = store.load(&graph_id).map_err(|error| error.to_string())?;
        if !graph
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
        {
            return Ok(None);
        }
        let mut after = 0;
        while after < graph.revision {
            let (next, starts) = store
                .binding_starts_page(&graph.id, after, graph.revision)
                .map_err(|error| error.to_string())?;
            if next <= after {
                return Err("retirement binding page made no progress".into());
            }
            for (node, binding, start) in starts {
                let (ticket, spec) = store
                    .binding_start_ticket(&graph, &node, &binding)
                    .map_err(|error| error.to_string())?;
                if ticket.attempt != binding.attempt || ticket.payload_ref != spec.payload_ref {
                    return Err("retirement original ticket/spec mismatch".into());
                }
                if let Some(packet) = packet
                    .filter(|packet| packet.graph_id() == graph_id && packet.node_id() == node)
                {
                    let original: AgentTaskPacket = serde_json::from_str(&ticket.payload_ref)
                        .map_err(|error| error.to_string())?;
                    if original != *packet || ticket.attempt != packet.attempt {
                        return Err("retirement original packet/attempt mismatch".into());
                    }
                    original_seen = true;
                }
                if !store
                    .binding_lease_closure(&graph, &node, &binding, start)
                    .map_err(|error| error.to_string())?
                {
                    return Ok(None);
                }
                observed += 1;
            }
            after = next;
        }
        pending.extend(
            store
                .child_links(&graph_id)
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|link| link.child_execution_id),
        );
    }
    if !original_seen || observed == 0 {
        return Err("retirement original binding was not observed".into());
    }
    Ok(Some(observed))
}

async fn wait_retired_process(
    services: &Arc<RuntimeServices>,
    packet: &AgentTaskPacket,
    task_ref: &str,
) {
    RealProcessSession {
        services: services.clone(),
        packet: packet.clone(),
    }
    .wait_exit()
    .await;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let projection = services.agent_action_service()
                .project(&packet.agentic_binding.as_ref().unwrap().program_id).unwrap();
            let task = &projection.tasks[task_ref];
            let graph = services.graph_state_store().load(packet.graph_id()).unwrap();
            if task.status == crate::AgenticTaskStatus::Withdrawn && task.active_attempts.is_empty()
                && graph.node_statuses.values().all(|status| status.is_terminal())
                && original_binding_closures_ready(services, packet.graph_id(), Some(packet))
                    .expect("original retirement closure must authenticate").is_some()
            {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.unwrap_or_else(|elapsed| {
        own832_diagnostics::capture(services,
            &packet.agentic_binding.as_ref().unwrap().program_id,
            task_ref, "original_task_retirement_timeout", Some(packet));
        panic!("original exit/graph/effect lane must retire the same Task attempt: {elapsed:?}; see retirement diagnostics");
    });
}

const REAL_PROCESS_WORKER: &str = r###"#!/usr/bin/env python3
import fcntl,hashlib,json,os,pathlib,socket,sys,time
hello=json.loads(sys.stdin.readline())
identity=dict(protocol_version=3,run_id=hello['run_id'],agent_id=hello['agent_id'])
sequence=0
def send(kind,**fields):
 global sequence
 sequence+=1
 print(json.dumps(dict(identity,sequence=sequence,kind=kind,**fields)),flush=True)
def receive():
 while True:
  line=sys.stdin.readline()
  if not line: raise RuntimeError('Runtime pipe closed')
  frame=json.loads(line)
  if frame['kind']=='agent_command':
   send('command_ack',command_id=frame['command_id'],expected_run_revision=frame['expected_run_revision'],accepted=True)
   continue
  return frame
def acknowledge(frame):
 delta=frame.get('context_delta')
 if delta:
  send('context_ack',delivery_id=delta['delivery_id'])
  assert receive()['kind']=='context_acknowledged'
def publish(path,value):
 temporary=path.with_suffix('.tmp')
 temporary.write_text(json.dumps(value),encoding='utf-8')
 temporary.replace(path)
send('ready',manifest_digest=hello['manifest_digest'],focus=hello['focus'],model_control='external_configured',capabilities=dict(features=['step_binding_v1'],commands=['step_begin','tool_request','action_request','context_ack','command_ack','result','error'],supports_recovery=False,evidence_mode='runtime_receipts',model_control='external_configured'))
start=receive()
assert start['kind']=='start'
packet=start['packet']
directory=pathlib.Path(sys.argv[1])/(hashlib.sha256(hello['run_id'].encode()).hexdigest()+'-'+str(packet['attempt']))
assert directory.is_dir()
acknowledge(start)
publish(directory/'started.json',packet)
deadline=time.monotonic()+180
while time.monotonic()<deadline:
 requests=sorted(directory.glob('*.request'))
 if not requests:
  time.sleep(.005)
  continue
 for path in requests:
  command=json.loads(path.read_text(encoding='utf-8'))
  path.unlink()
  response_path=path.with_suffix('.response')
  if command.get('direct_probe'):
   probe=command['direct_probe']
   try:
    if probe['kind'] in ['read','write']:
     target=pathlib.Path(probe['path']).absolute()
     scratch_root=pathlib.Path(sys.argv[1])
     allowed=target in [pathlib.Path.cwd()/'out/a',pathlib.Path.cwd()/'out/physical-sentinel.txt',pathlib.Path.cwd()/'.cowd/fixtures/r1-worker.py',pathlib.Path.cwd()/'.cowd/checkpoints/direct-probe.txt']
     allowed=allowed or (scratch_root in target.parents and target.name in ['private-control.txt','started.json','cross-run.txt'])
     if not allowed: raise RuntimeError('probe outside disposable fixture sentinels/control scratch')
    if probe['kind']=='read':
     value=pathlib.Path(probe['path']).read_text(encoding='utf-8')
    elif probe['kind']=='write':
     pathlib.Path(probe['path']).write_text(probe['value'],encoding='utf-8')
     value='written'
    elif probe['kind']=='nonce':
     with socket.create_connection(('127.0.0.1',probe['port']),timeout=.5) as channel:
      channel.sendall(probe['nonce'].encode())
     value='sent'
    else: raise RuntimeError('unknown fixture direct probe')
    publish(response_path,{'value':value,'ok':True})
   except OSError as error:
    publish(response_path,{'ok':False,'errno':error.errno})
   continue
  if command.get('finish'):
   assignment=packet['assignment']
   result={key:assignment[key] for key in ['run_id','task_id','session_id','mission_id','graph_id','node_id']}
   result.update(agent_id=hello['agent_id'],team_id=assignment.get('team_run_id'),attempt=packet['attempt'],expected_graph_revision=packet['expected_graph_revision'],status='completed',outcome='Controlled local Process completed its admitted actions',observed_acceptance={'satisfied_criteria':[],'observed_evidence':[],'unresolved_obligation_ids':[]},acceptance=[],evidence_refs=[],changes=[],conflicts=[],unresolved=[],input_tokens=0,output_tokens=0,model='fixture',provider='external_process',tool_calls=0,failure=None)
   publish(response_path,{'finished':True})
   send('result',result=result)
   sys.exit(0)
  send('step_begin')
  step=receive()
  assert step['kind']=='step_bound',step
  fields=dict(request_id=command['request_id'],step_id=step['binding']['step']['identity']['step_id'],tool_name=command['tool_name'],input=command['input'])
  if command.get('defer_receive'): fcntl.fcntl(0,1031,4096)
  send('tool_request',**fields)
  if command.get('defer_receive'):
   publish(directory/'receive-held.json',{'request_id':command['request_id']})
   while not (directory/'receive-release.json').exists():
    if time.monotonic()>deadline: raise RuntimeError('receive gate timed out')
    time.sleep(.005)
  if command.get('close_stdin'):
   stdin_inode=os.fstat(0).st_ino
   os.close(0)
   publish(directory/'stdin-closed.json',{'request_id':command['request_id'],'packet':packet,'stdin_inode':stdin_inode})
   os._exit(0)
  reply=receive()
  acknowledge(reply)
  send('step_begin')
  barrier=receive()
  assert barrier['kind']=='step_bound',barrier
  publish(response_path,reply)
raise RuntimeError('Controlled Process exceeded its bounded lifetime')
"###;

struct RootWireServer {
    endpoint: String,
    replies: Arc<std::sync::Mutex<std::collections::VecDeque<serde_json::Value>>>,
    requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    task: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    pause:
        Arc<std::sync::Mutex<Option<(String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>>>,
}

impl Drop for RootWireServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RootWireServer {
    fn actual_request_diagnostic(&self) -> serde_json::Value {
        let requests = self.requests.lock().unwrap();
        let parsed = requests
            .last()
            .map(|body| serde_json::from_slice::<serde_json::Value>(body));
        let last = parsed.as_ref().and_then(|value| value.as_ref().ok());
        let messages = last.and_then(|value| value["messages"].as_array());
        let calls = messages
            .into_iter()
            .flatten()
            .rev()
            .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
            .take(8)
            .map(|call| {
                serde_json::json!({
                    "id":call["id"],"name":call["function"]["name"],
                })
            })
            .collect::<Vec<_>>();
        let results = messages
            .into_iter()
            .flatten()
            .rev()
            .filter(|message| message["role"] == "tool")
            .take(4)
            .map(|message| {
                serde_json::json!({
                    "tool_call_id":message["tool_call_id"],
                    "content":message["content"].as_str()
                        .map(|text| text.chars().take(512).collect::<String>()),
                })
            })
            .collect::<Vec<_>>();
        serde_json::json!({
            "request_count":requests.len(),
            "last_request_bytes":requests.last().map(Vec::len),
            "last_request_parse_error":parsed.as_ref()
                .and_then(|value| value.as_ref().err()).map(ToString::to_string),
            "last_request_tool_definitions":last.and_then(|value| value["tools"].as_array())
                .map(|tools| tools.iter().take(16)
                    .map(|tool| &tool["function"]["name"]).collect::<Vec<_>>()),
            "last_request_assistant_tool_calls":calls,
            "last_request_tool_results":results,
            "scope":"outbound request transcript, not queued fixture replies or proof of execution",
        })
    }

    async fn start() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/v1", listener.local_addr().unwrap());
        let replies = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::<
            serde_json::Value,
        >::new()));
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let queued = replies.clone();
        let captured = requests.clone();
        #[cfg(test)]
        let pause = Arc::new(std::sync::Mutex::new(
            None::<(String, Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        ));
        #[cfg(test)]
        let response_pause = pause.clone();
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let body = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                    let mut bytes = Vec::new();
                    loop {
                        let mut buffer = [0_u8; 8192];
                        let count = socket.read(&mut buffer).await.unwrap();
                        assert!(count > 0 && bytes.len() < 16 * 1024 * 1024);
                        bytes.extend_from_slice(&buffer[..count]);
                        if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n")
                        {
                            let headers = String::from_utf8_lossy(&bytes[..offset]);
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    let (name, value) = line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap();
                            if bytes.len() >= offset + 4 + length {
                                break bytes[offset + 4..offset + 4 + length].to_vec();
                            }
                        }
                    }
                })
                .await
                .unwrap();
                captured.lock().unwrap().push(body);
                #[cfg(test)]
                {
                    let next = queued.lock().unwrap().front().cloned();
                    let next = next.unwrap_or_else(|| {
                        let (request_count, last_body) = {
                            let requests = captured.lock().unwrap();
                            (requests.len(), requests.last().map(|body| {
                                String::from_utf8_lossy(body).chars().take(4096).collect::<String>()
                            }))
                        };
                        panic!("R1_ROOT_WIRE_REPLIES_EXHAUSTED request_count={request_count} last_request_body={last_body:?}");
                    });
                    let gate = response_pause
                        .lock()
                        .unwrap()
                        .as_ref()
                        .filter(|(call, _, _)| {
                            next["tool_calls"][0]["id"].as_str() == Some(call.as_str())
                        })
                        .cloned();
                    if let Some((_, arrived, resume)) = gate {
                        response_pause.lock().unwrap().take();
                        arrived.notify_one();
                        resume.notified().await;
                    }
                }
                let delta = { queued.lock().unwrap().pop_front() };
                let delta = delta.expect("loopback reply was explicitly queued");
                let finish = if delta.get("tool_calls").is_some() {
                    "tool_calls"
                } else {
                    "stop"
                };
                let chunk = |delta, finish_reason| {
                    serde_json::json!({
                        "id":"r1-root-loopback","object":"chat.completion.chunk","model":"r1-root-wire",
                        "choices":[{"index":0,"delta":delta,"finish_reason":finish_reason}],
                    })
                };
                let body = format!(
                    "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                    chunk(delta, serde_json::Value::Null),
                    chunk(serde_json::json!({}), serde_json::json!(finish))
                );
                socket.write_all(format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(), body,
                ).as_bytes()).await.unwrap();
            }
        });
        Self {
            endpoint,
            replies,
            requests,
            task,
            #[cfg(test)]
            pause,
        }
    }
}

#[cfg(test)]
mod root_wire_diagnostics_tests {
    use super::RootWireServer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn send_request(server: &RootWireServer, body: &[u8]) -> tokio::net::TcpStream {
        let address = server
            .endpoint
            .strip_prefix("http://")
            .unwrap()
            .strip_suffix("/v1")
            .unwrap();
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        let headers = format!("POST /v1/chat/completions HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        socket.write_all(headers.as_bytes()).await.unwrap();
        socket.write_all(body).await.unwrap();
        socket
    }

    #[tokio::test]
    async fn r1_620_wire_diagnostic_bounds_actual_transcript_without_counting_queued_replies() {
        let server = RootWireServer::start().await;
        server.requests.lock().unwrap().push(
            serde_json::to_vec(&serde_json::json!({
                "messages":[
                    {"role":"assistant","tool_calls":[
                        {"id":"actual-read","function":{"name":"read_file","arguments":"{}"}},
                    ]},
                    {"role":"tool","tool_call_id":"actual-read","content":"λ".repeat(4096)},
                ],
                "tools":[{"function":{"name":"task_submit"}}],
            }))
            .unwrap(),
        );
        server.replies.lock().unwrap().push_back(serde_json::json!({
            "tool_calls":[{"id":"queued-not-executed","function":{"name":"task_submit"}}],
        }));
        let diagnostic = server.actual_request_diagnostic();
        assert_eq!(diagnostic["request_count"], 1);
        assert_eq!(
            diagnostic["last_request_assistant_tool_calls"][0]["name"],
            "read_file"
        );
        assert_eq!(
            diagnostic["last_request_tool_results"][0]["tool_call_id"],
            "actual-read"
        );
        assert_eq!(
            diagnostic["last_request_tool_results"][0]["content"]
                .as_str()
                .unwrap()
                .chars()
                .count(),
            512
        );
        assert!(!diagnostic.to_string().contains("queued-not-executed"));
        assert!(diagnostic.to_string().len() < 4096);
        assert_eq!(server.replies.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn r1_root_wire_queued_reply_preserves_actual_request_and_response() {
        let server = RootWireServer::start().await;
        server.replies.lock().unwrap().push_back(serde_json::json!({
            "role": "assistant", "content": "only the explicitly queued response",
        }));
        let body = br#"{"messages":[{"role":"user","content":"original bounded request"}]}"#;
        let mut socket = send_request(&server, body).await;
        let mut response = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            socket.read_to_end(&mut response),
        )
        .await
        .unwrap()
        .unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(response.contains("only the explicitly queued response"));
        assert!(response.ends_with("data: [DONE]\n\n"));
        assert_eq!(*server.requests.lock().unwrap(), vec![body.to_vec()]);
        assert!(server.replies.lock().unwrap().is_empty());
        assert!(!server.task.is_finished());
    }

    #[tokio::test]
    async fn r1_root_wire_exhaustion_retains_protocol_denial_and_never_supplies_success() {
        let mut server = RootWireServer::start().await;
        let body = br#"{"messages":[{"role":"tool","tool_call_id":"native-submit","content":"task_submission_has_no_artifact"}]}"#;
        let _socket = send_request(&server, body).await;
        let failure = tokio::time::timeout(std::time::Duration::from_secs(2), &mut server.task)
            .await
            .unwrap()
            .unwrap_err();
        assert!(failure.is_panic());
        let payload = failure.into_panic();
        let message = payload.downcast_ref::<String>().unwrap();
        assert!(message.contains("R1_ROOT_WIRE_REPLIES_EXHAUSTED request_count=1"));
        assert!(message.contains("native-submit"));
        assert!(message.contains("task_submission_has_no_artifact"));
        assert_eq!(*server.requests.lock().unwrap(), vec![body.to_vec()]);
        assert!(server.replies.lock().unwrap().is_empty());
    }
}

pub(super) struct RootReviewRuntime {
    runtime: tokio::sync::Mutex<
        crate::ConversationRuntime<crate::ProviderRuntimeClient, RootReviewReader>,
    >,
    server: RootWireServer,
    bus: crate::CowdEventBus,
    services: Arc<RuntimeServices>,
}

impl RootReviewRuntime {
    pub(super) fn cowd_bus(&self) -> Option<&crate::CowdEventBus> {
        Some(&self.bus)
    }

    async fn execute_approved_bounded_code(&self, input: &str) -> Result<(), String> {
        use harness_contract::policy::{
            ApprovalDecisionActorKind, ApprovalGrantScope, ApprovalGrantStatus, ApprovalStatus,
        };

        let call_id = "bounded-root-nonowns-283";
        let expected_input: serde_json::Value =
            serde_json::from_str(input).map_err(|error| error.to_string())?;
        let input = expected_input.to_string();
        let store = self.services.event_store();
        let cursor = store.durable_commit_cursor().map_err(|error| error.to_string())?;
        let owner = store
            .latest_for_stream("root-agentic-execution:node:root-agentic-execution:model")
            .map_err(|error| error.to_string())?
            .and_then(|event| event.activity_binding())
            .ok_or("bounded root approval requires the original root activity")?;
        let (host, policy) = {
            let runtime = self.runtime.lock().await;
            (Arc::clone(&runtime.tool_executor().0),
                runtime.permission_policy().execution_policy_control().snapshot())
        };
        let queue = self.services.approval_coordinator().queue();
        let prior_approvals: std::collections::BTreeSet<_> = queue.list().into_iter()
            .map(|request| request.approval_id).collect();
        let calls = [crate::conversation::ModelToolCall {
            binding: None, id: call_id.into(), name: "execute_code".into(),
            input: input.clone(), depends_on: vec![],
        }];
        let prompter = crate::SharedPrompter::none();
        let executed = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.execute_tool_batch_step(&calls, &prompter, 4),
        ).await;
        let approvals = queue.list_for_execution_scope(
            Some(&owner.session_id), &Default::default(), &Default::default(),
        );
        let result = executed
            .map_err(|_| format!("bounded root execution exceeded its 30s deadline; owner={owner:?}; approvals={approvals:?}"))?
            .map_err(|error| format!("bounded root execution failed: {error}; owner={owner:?}; approvals={approvals:?}"))?;
        if result.failed != 0 || !result.refused_before_dispatch.is_empty() {
            return Err(format!("bounded root execution rejected: {result:?}; owner={owner:?}; approvals={approvals:?}"));
        }
        let requests: Vec<_> = host.calls.lock().unwrap().iter()
            .filter(|request| request.tool_use_id == call_id && request.tool_name == "execute_code")
            .cloned().collect();
        let [request] = requests.as_slice() else {
            return Err("bounded root must dispatch its exact original call once".into());
        };
        let authorization = request.authorization.as_ref()
            .ok_or("bounded root actual request has no authorization")?;
        let matching: Vec<_> = approvals.iter().filter(|approval| {
            !prior_approvals.contains(&approval.approval_id)
                && approval.context.invocation_id.as_deref() == Some(authorization.request_id.as_str())
        }).collect();
        let [approval] = matching.as_slice() else {
            return Err(format!("bounded root requires one new exact approval; approvals={approvals:?}"));
        };
        let context = &approval.context;
        if approval.status != ApprovalStatus::Approved
            || context.principal_id != format!("session:{}", owner.session_id)
            || context.session_id.as_deref() != Some(owner.session_id.as_str())
            || context.turn_id.as_deref() != Some(owner.turn_id.as_str())
            || context.execution_id.as_deref() != Some(owner.root_execution_id.as_str())
            || context.task_id.as_deref() != Some(owner.task_id.as_str())
            || context.workspace_key != self.services.workspace_key()
            || context.capability != "execute_code"
            || context.approval_profile != Some(policy.approval_profile)
            || context.profile_id != policy.approval_profile.as_str()
            || context.policy_revision != policy.revision
            || approval.decision.as_ref().is_none_or(|decision| {
                !decision.evidence_refs.iter()
                    .any(|evidence| evidence == "approval.router.auto")
                    || !decision.evidence_refs.iter()
                        .any(|evidence| evidence == "approval.router.profile:Autonomous")
            })
        {
            return Err(format!("bounded root approval owner or policy mismatch: {approval:?}"));
        }
        let mut matching_tasks = Vec::new();
        for event in store.list_stream(&format!("session:{}", owner.session_id))
            .map_err(|error| error.to_string())?
        {
            if event.commit_cursor <= cursor || event.kind != "tool.execution_plan.created"
                || event.actor.as_deref() != Some("conversation_runtime")
                || event.scope != crate::RuntimeEventScope::Tool
            {
                continue;
            }
            let plan: crate::governed_tool_plan::GovernedToolPlan =
                serde_json::from_value(event.payload).map_err(|error| error.to_string())?;
            matching_tasks.extend(plan.tasks.into_iter().filter(|task| task.tool_call_id == call_id));
        }
        let [task] = matching_tasks.as_slice() else {
            return Err("root approval must identify one actual persisted plan call".into());
        };
        let binding = task.step_binding.as_ref().ok_or("root approval call lacks its Step")?;
        let identity = &binding.snapshot.identity;
        if task.tool_name != "execute_code" || task.normalized_input != expected_input
            || task.invocation.intent.normalized_input != expected_input
            || binding.origin.call_id != call_id
            || identity.session_id != owner.session_id || identity.turn_id != owner.turn_id
            || identity.execution_id.as_deref() != Some(owner.root_execution_id.as_str())
            || request.compiled_invocation.as_ref() != Some(&task.invocation)
            || request.idempotency_key != binding.effect_idempotency_key()
        {
            return Err("root approval Step or exact input mismatch".into());
        }
        let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(&task.effect, &input);
        let targets: Vec<String> = effective.descriptor.scopes.iter()
            .filter_map(|scope| scope.target.clone()).collect();
        let grant = queue.grant_for_approval(&approval.approval_id)
            .ok_or("bounded root Policy grant is missing")?;
        if context.effect.as_ref() != Some(&effective.descriptor)
            || context.resource_targets != targets
            || authorization.descriptor_hash != effective.descriptor.descriptor_hash
            || authorization.policy_revision != policy.revision
            || grant.scope != ApprovalGrantScope::Once
            || grant.issued_by.kind != ApprovalDecisionActorKind::Policy
            || grant.issued_by.actor_id != "approval-router-auto"
            || grant.status != ApprovalGrantStatus::Expired
            || grant.invocation_id != context.invocation_id
            || grant.session_id != context.session_id || grant.turn_id != context.turn_id
            || grant.task_id != context.task_id || grant.principal_id != context.principal_id
            || grant.workspace_key != context.workspace_key || grant.profile_id != context.profile_id
            || grant.capability != context.capability || grant.policy_revision != context.policy_revision
            || grant.resource_targets != targets
            || grant.effect_descriptor_hash.as_deref() != Some(effective.descriptor.descriptor_hash.as_str())
        {
            return Err(format!("bounded root exact Policy Once grant mismatch: {approval:?}; {grant:?}"));
        }
        let events = store.list_stream(&format!("approval:{}", approval.approval_id))
            .map_err(|error| error.to_string())?;
        let issued: Vec<_> = events.iter().filter(|event| event.commit_cursor > cursor
            && event.kind == "approval.grant_issued"
            && event.payload["grant"]["grant_id"].as_str() == Some(grant.grant_id.as_str())).collect();
        let consumed: Vec<_> = events.iter().filter(|event| event.commit_cursor > cursor
            && event.kind == "approval.grant_consumed"
            && event.payload["grant_id"].as_str() == Some(grant.grant_id.as_str())).collect();
        if issued.len() != 1 || consumed.len() != 1
            || issued[0].commit_cursor >= consumed[0].commit_cursor
        {
            return Err(format!("bounded root requires its unique Policy Once issuance and consumption: {events:?}"));
        }
        Ok(())
    }

    pub(super) async fn execute_tool_batch_step(
        &self,
        calls: &[crate::conversation::ModelToolCall],
        prompter: &crate::SharedPrompter,
        iteration: usize,
    ) -> Result<crate::conversation::ToolBatchStepResult, crate::RuntimeError> {
        let owner = self
            .services
            .event_store()
            .latest_for_stream("root-agentic-execution:node:root-agentic-execution:model")
            .map_err(|error| crate::RuntimeError::new(error.to_string()))?
            .and_then(|event| event.activity_binding())
            .ok_or_else(|| crate::RuntimeError::new("fixture original root activity is missing"))?;
        let _scope = self.bus.enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: owner.root_execution_id.clone(),
                session_id: owner.session_id.clone(),
                turn_id: owner.turn_id.clone(),
            },
            Some(owner),
        );
        let mut runtime = self.runtime.lock().await;
        let tool_calls = calls
            .iter()
            .enumerate()
            .map(|(index, call)| {
                let mut input: serde_json::Value = serde_json::from_str(&call.input).unwrap();
                if call.name == "edit" && input.get("action").is_none() {
                    input["action"] = "write".into();
                }
                serde_json::json!({
                    "index":index, "id":call.id, "type":"function",
                    "function":{"name":call.name,"arguments":input.to_string()},
                })
            })
            .collect::<Vec<_>>();
        self.server
            .replies
            .lock()
            .unwrap()
            .push_back(serde_json::json!({
                "role":"assistant", "tool_calls":tool_calls,
            }));
        runtime.require_next_model_tool_action(calls.iter().map(|call| call.name.clone()));
        let model = runtime
            .execute_model_step_with_early_dispatch(
                "Execute the explicitly requested bounded evidence operation",
                false,
                None,
                false,
                false,
            )
            .await?;
        let crate::conversation::ModelStepIntent::ToolCalls { calls: bound_calls } = model.intent
        else {
            return Err(crate::RuntimeError::new(format!(
                "Root loopback did not emit the original bound tool call: intent={:?}; preflight={:?}; requested={calls:?}; text_only={}; wire_requests={}; queued_replies={}",
                model.intent,
                model.preflight_tool_results,
                model.text_only_response,
                self.server.requests.lock().unwrap().len(),
                self.server.replies.lock().unwrap().len(),
            )));
        };
        assert_eq!(bound_calls.len(), calls.len());
        assert!(bound_calls.iter().all(|call| call.binding.is_some()));
        runtime.remember_native_tool_calls(&bound_calls)?;
        let result = runtime
            .execute_tool_batch_step(&bound_calls, prompter, iteration)
            .await?;
        self.server.replies.lock().unwrap().push_back(serde_json::json!({
            "role":"assistant", "content":"The actual tool result was supplied in this local continuation.",
        }));
        runtime.require_next_model_final_response();
        runtime
            .execute_model_step_with_early_dispatch(
                "Report only the supplied current evidence",
                false,
                None,
                false,
                false,
            )
            .await?;
        let sent = self.server.requests.lock().unwrap().clone();
        assert!(sent.len() >= 2);
        assert!(sent.last().is_some_and(|body| {
            let value: serde_json::Value = serde_json::from_slice(body).unwrap();
            value["messages"]
                .as_array()
                .is_some_and(|messages| messages.iter().any(|message| message["role"] == "tool"))
        }));
        if result.failed == 0 {
            for call in bound_calls
                .iter()
                .filter(|call| matches!(call.name.as_str(), "read_file" | "edit" | "execute_code"))
            {
                let key = call.binding.as_ref().unwrap().effect_idempotency_key();
                if let Some((_, reason)) = result
                    .refused_before_dispatch
                    .iter()
                    .find(|(refused, _)| refused == &call.id)
                {
                    assert!(!reason.trim().is_empty());
                    for phase in ["receipt", "read-receipt"] {
                        assert!(self.services.event_store().event_by_idempotency_key(
                            &format!("execution-effect:{key}"),
                            &format!("{key}:{phase}"),
                        ).unwrap().is_none(), "pre-dispatch refusal must not publish an effect receipt for {}", call.id);
                    }
                    continue;
                }
                let primary = self
                    .services
                    .event_store()
                    .event_by_idempotency_key(
                        &format!("execution-effect:{key}"),
                        &format!(
                            "{key}:{}",
                            if call.name == "read_file" {
                                "read-receipt"
                            } else {
                                "receipt"
                            }
                        ),
                    )
                    .unwrap()
                    .unwrap_or_else(|| panic!(
                        "executed root call requires primary receipt: id={} name={} key={key} refused={:?} results={:?}",
                        call.id, call.name, result.refused_before_dispatch, result.messages,
                    ));
                assert!(primary.payload["replay"]["parent_execution"].is_null());
                let owner: harness_contract::projection::RuntimeActivityBinding =
                    serde_json::from_value(primary.payload["root_effect_owner"]["binding"].clone())
                        .unwrap();
                let attempt = primary.payload["root_effect_owner"]["attempt"]
                    .as_u64()
                    .unwrap();
                let node = owner.node_id.as_deref().unwrap();
                let original = self
                    .services
                    .event_store()
                    .event_by_idempotency_key(
                        &format!("{}:node:{node}", owner.root_execution_id),
                        &format!("{}:{node}:{}", owner.root_execution_id, owner.revision),
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(original.activity_binding().as_ref(), Some(&owner));
                let index = self
                    .services
                    .event_store()
                    .event_by_idempotency_key(
                        &format!(
                            "execution-agent-receipts:{}:{node}:{attempt}",
                            owner.root_execution_id
                        ),
                        &format!("agent-tool-receipt:{key}"),
                    )
                    .unwrap()
                    .unwrap();
                assert_eq!(primary.transaction_id, index.transaction_id);
                assert_eq!(
                    primary.payload["root_effect_owner"],
                    index.payload["root_effect_owner"]
                );
                assert_eq!(
                    primary.payload["resolved_effect_scope"],
                    index.payload["resolved_effect_scope"]
                );
                let scope: harness_contract::goal::ProofEffectScope =
                    serde_json::from_value(primary.payload["resolved_effect_scope"].clone())
                        .unwrap();
                let descriptor: harness_contract::tool::ToolEffectDescriptor =
                    serde_json::from_value(
                        primary.payload["replay"]["invocation"]["effect"].clone(),
                    )
                    .unwrap();
                scope.validate_purpose_mapping(&descriptor).unwrap();
                assert_eq!(scope.version, 2);
            }
            let events = self
                .services
                .event_store()
                .list_stream(&format!("session:{}", runtime.session_id()))
                .unwrap();
            for call in bound_calls
                .iter()
                .filter(|call| matches!(call.name.as_str(), "read_file" | "evidence_retrieve"))
            {
                let material = events.iter().filter(|event|event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
                    .filter_map(|event|crate::context_evidence::decode_read_delivery(&event.payload).ok())
                    .find(|material|material.acquisition.origin["call_id"] == call.id
                        && call.binding.as_ref().is_some_and(|binding|binding.snapshot.identity.step_id == material.acquisition.step.step_id))
                    .expect("Root read must have original acquisition, actual wire and successful continuation");
                assert_eq!(
                    material.grade,
                    crate::context_evidence::ReadDeliveryGrade::ProviderModelObserved
                );
                assert!(matches!(
                    material.recipient,
                    crate::context_evidence::ReadDeliveryRecipient::Root { identity: None, .. }
                ));
                let crate::context_evidence::ReadDeliveryTransport::NativeProviderContinuation {
                    wire_body_sha256,
                    wire_artifact,
                    ..
                } = &material.transport
                else {
                    panic!("Root must retain native provider transport")
                };
                let bytes = self
                    .services
                    .artifact_store()
                    .read(
                        wire_artifact,
                        &format!("session:{}", runtime.session_id()),
                        None,
                    )
                    .await
                    .unwrap();
                let artifact: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                let canonical =
                    serde_json::to_vec(&artifact["provider_request"]["wire_request"]["body"])
                        .unwrap();
                assert_eq!(
                    crate::context_evidence::read_sha256(&canonical).trim_start_matches("sha256:"),
                    wire_body_sha256.as_str()
                );
                assert!(sent.iter().any(|body| body == &canonical));
            }
        }
        Ok(result)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum RealProcessProofCase {
    Authorized,
    FailedSend,
    Preview,
    Mechanical,
    Pages,
    LateDelivery,
    Sources,
    NativeSources,
    Legacy,
    CommandAck,
    Restart,
    ReapAndEffect,
    Effects,
}

fn process_materials(
    services: &RuntimeServices,
    packet: &AgentTaskPacket,
) -> Vec<(
    crate::DurableRuntimeEvent,
    crate::context_evidence::ReadDeliveryMaterial,
)> {
    services
        .event_store()
        .list_stream(&format!("session:{}", packet.session_id()))
        .unwrap()
        .into_iter()
        .filter(|event| event.kind == crate::context_evidence::PROCESS_DELIVERY_KIND)
        .filter_map(|event| {
            let material = crate::context_evidence::decode_read_delivery(&event.payload).unwrap();
            match &material.recipient {
                crate::context_evidence::ReadDeliveryRecipient::AgentTask { identity }
                    if identity.agent_run_id() == Some(packet.run_id()) =>
                {
                    Some((event, material))
                }
                _ => None,
            }
        })
        .collect()
}

fn assert_process_delivery(
    services: &RuntimeServices,
    packet: &AgentTaskPacket,
    request_id: &str,
) -> crate::context_evidence::ReadDeliveryMaterial {
    use crate::context_evidence::{
        ReadDeliveryGrade, ReadDeliveryTransport, READ_ACQUISITION_FIELD,
    };
    let mut materials = process_materials(services, packet)
        .into_iter()
        .filter(|(_, material)| material.tool_invocation_id == request_id);
    let (event, material) = materials
        .next()
        .expect("actual successful JSONL write/flush produced delivery");
    assert!(materials.next().is_none());
    assert_ne!(
        packet.assignment.execution_identity.principal_id(),
        packet.agent_id(),
        "Process actor regression requires distinct principal and instance namespaces"
    );
    assert_eq!(
        material.acquisition.step.actor_ref.as_deref(),
        Some(packet.agent_id())
    );
    assert_eq!(material.grade, ReadDeliveryGrade::AgentChannelSend);
    assert_eq!(material.acquisition.attempt, packet.attempt);
    assert_eq!(material.acquisition.graph_id, packet.graph_id());
    let primary = services
        .event_store()
        .event_by_idempotency_key(
            &material.acquisition.primary_stream,
            &material.acquisition.primary_key,
        )
        .unwrap()
        .unwrap();
    let index = services
        .event_store()
        .event_by_idempotency_key(
            &material.acquisition.index_stream,
            &material.acquisition.index_key,
        )
        .unwrap()
        .unwrap();
    assert_eq!(primary.event_id, material.acquisition_event_id);
    assert_eq!(index.event_id, material.index_event_id);
    assert_eq!(primary.transaction_id, index.transaction_id);
    assert_eq!(
        primary.payload[READ_ACQUISITION_FIELD],
        index.payload[READ_ACQUISITION_FIELD]
    );
    if material.acquisition.tool_name == "read_file" {
        let scope: harness_contract::goal::ProofEffectScope =
            serde_json::from_value(primary.payload["resolved_effect_scope"].clone()).unwrap();
        let descriptor: harness_contract::tool::ToolEffectDescriptor =
            serde_json::from_value(primary.payload["replay"]["invocation"]["effect"].clone())
                .unwrap();
        assert_eq!(scope.version, 2);
        assert!(scope.complete);
        scope.validate_purpose_mapping(&descriptor).unwrap();
        assert!(scope
            .scope_purposes
            .iter()
            .any(|entry| entry.purpose == harness_contract::goal::ProofEffectPurpose::InputRead));
        assert_eq!(
            primary.payload["resolved_effect_scope"],
            index.payload["resolved_effect_scope"]
        );
        let mut omitted = scope.clone();
        omitted.scope_purposes.clear();
        assert!(omitted.validate_purpose_mapping(&descriptor).is_err());
    }
    assert!(event.commit_cursor > primary.commit_cursor);
    let ReadDeliveryTransport::ProcessChannelSend {
        worker_id,
        request_id: transported,
        sequence,
        step_id,
        response_sha256,
        response_bytes,
        ..
    } = &material.transport
    else {
        panic!("only the real Process channel may produce this grade")
    };
    assert_eq!(worker_id, packet.run_id());
    assert_eq!(transported, request_id);
    assert!(*sequence > 0 && !step_id.is_empty() && *response_bytes > 0);
    assert!(!response_sha256.is_empty());
    material
}

async fn assert_process_review_refused(
    services: &ReviewedFixtureServices,
    envelope: &AgentActionEnvelope,
    suffix: &str,
) -> String {
    let before = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let mut request = envelope.clone();
    request.action_id = format!("{}:{suffix}", envelope.action_id);
    let error = services
        .submit_agent_action(&request)
        .await
        .expect_err("incomplete or stale actual delivery must not certify the Goal");
    assert!(
        error.contains("review_requires") || error.contains("read") || error.contains("evidence"),
        "{error}"
    );
    assert_eq!(
        services.goal_store().get(&before.id).unwrap().unwrap(),
        before
    );
    assert!(before.terminal.is_none());
    error
}

fn process_send_observer_665(
    packet: &AgentTaskPacket,
) -> std::sync::mpsc::Receiver<String> {
    #[cfg(test)]
    {
        observe_process_cleanup_driver(packet)
    }
    #[cfg(not(test))]
    {
        let _ = packet;
        panic!("FailedSend requires the original cfg(test) Process driver observer")
    }
}

#[cfg(target_os = "linux")]
async fn wait_process_pipe_readers_closed_665(
    inode: u64,
) -> Result<std::os::fd::OwnedFd, String> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::fs::{FileTypeExt, MetadataExt};

    let pipe_name = std::path::PathBuf::from(format!("pipe:[{inode}]"));
    let mut matching = Vec::new();
    for entry in std::fs::read_dir("/proc/self/fd").map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        if std::fs::read_link(entry.path()).ok().as_ref() != Some(&pipe_name) {
            continue;
        }
        let metadata = std::fs::metadata(entry.path()).map_err(|error| error.to_string())?;
        if !metadata.file_type().is_fifo() || metadata.ino() != inode {
            return Err("original Process stdin pipe metadata changed".into());
        }
        let descriptor: libc::c_int = entry
            .file_name()
            .to_str()
            .ok_or("original pipe fd is not UTF-8")?
            .parse()
            .map_err(|_| "original pipe fd is not numeric")?;
        let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
        if flags < 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        if flags & libc::O_ACCMODE == libc::O_WRONLY {
            matching.push(descriptor);
        }
    }
    let [descriptor] = matching.as_slice() else {
        return Err(format!(
            "expected one original Process stdin write fd, found {}",
            matching.len()
        ));
    };
    let duplicate = unsafe { libc::fcntl(*descriptor, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    let owned = unsafe { OwnedFd::from_raw_fd(duplicate) };
    let file = std::fs::File::from(owned);
    let metadata = file.metadata().map_err(|error| error.to_string())?;
    let owned: OwnedFd = file.into();
    let flags = unsafe { libc::fcntl(owned.as_raw_fd(), libc::F_GETFL) };
    if flags < 0 {
        return Err(std::io::Error::last_os_error().to_string());
    }
    if !metadata.file_type().is_fifo()
        || metadata.ino() != inode
        || std::fs::read_link(format!("/proc/self/fd/{}", owned.as_raw_fd()))
            .map_err(|error| error.to_string())?
            != pipe_name
        || flags & libc::O_ACCMODE != libc::O_WRONLY
    {
        return Err("duplicated original Process pipe identity/access mode changed".into());
    }
    loop {
        let mut observation = libc::pollfd {
            fd: owned.as_raw_fd(),
            events: 0,
            revents: 0,
        };
        let result = unsafe { libc::poll(&mut observation, 1, 0) };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                continue;
            }
            return Err(error.to_string());
        }
        if observation.revents & libc::POLLNVAL != 0 {
            return Err("original Process pipe fd became invalid".into());
        }
        if observation.revents & libc::POLLERR != 0 {
            return Ok(owned);
        }
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

#[cfg(not(target_os = "linux"))]
async fn wait_process_pipe_readers_closed_665(
    _: u64,
) -> Result<std::os::fd::OwnedFd, String> {
    Err("FailedSend original pipe observation requires Linux /proc/self/fd and poll".into())
}

#[inline(never)]
fn exercise_process_proof_case<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    host: &'fixture Arc<ReviewReadHost>,
    reviewer: &'fixture R1ScopedReviewer,
    envelope: &'fixture AgentActionEnvelope,
    content_ref: &'fixture str,
    root_actor: &'fixture AgentActorBinding,
    case: RealProcessProofCase,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = bool> + 'fixture>> {
    match case {
        RealProcessProofCase::Sources | RealProcessProofCase::NativeSources => {
            exercise_source_process_proof_case(services, host, reviewer, envelope, case)
        }
        _ => exercise_non_source_process_proof_case(
            services,
            host,
            reviewer,
            envelope,
            content_ref,
            root_actor,
            case,
        ),
    }
}

#[inline(never)]
fn exercise_non_source_process_proof_case<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    host: &'fixture Arc<ReviewReadHost>,
    reviewer: &'fixture R1ScopedReviewer,
    envelope: &'fixture AgentActionEnvelope,
    content_ref: &'fixture str,
    root_actor: &'fixture AgentActorBinding,
    case: RealProcessProofCase,
) -> std::pin::Pin<Box<impl std::future::Future<Output = bool> + 'fixture>> {
    Box::pin(async move {
        let session = &reviewer.reader;
        let packet = &reviewer.packet;
        let before = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert!(before.terminal.is_none());
        assert!(confirmed_run_exit_with_retry(services.event_store(), packet).is_none());
        match case {
            RealProcessProofCase::FailedSend => {
                let driver_observation = process_send_observer_665(packet);
                let fault_deadline =
                    tokio::time::Instant::now() + std::time::Duration::from_secs(30);
                let hold = Arc::new(ReviewWriteHold::default());
                *host.read_hold.lock().unwrap() = Some(hold.clone());
                let driver = session.clone();
                let running = tokio::spawn(async move {
                    driver.command(serde_json::json!({
                "request_id":"failed-actual-send", "tool_name":"read_file",
                "input":{"path":"review-effect.txt","complete":true}, "close_stdin":true,
            })).await
                });
                tokio::time::timeout(std::time::Duration::from_secs(15), hold.entered.notified())
                    .await
                    .unwrap();
                let closed_pipe = tokio::time::timeout_at(fault_deadline, async {
                    let marker = wait_fixture_file(
                        &process_control_directory(services, packet).join("stdin-closed.json"),
                    )
                    .await?;
                    let marker: serde_json::Value =
                        serde_json::from_slice(&marker).map_err(|error| error.to_string())?;
                    if marker["request_id"] != "failed-actual-send"
                        || marker["packet"]
                            != serde_json::to_value(packet).map_err(|error| error.to_string())?
                    {
                        return Err(
                            "FailedSend marker does not match the original request/packet".into(),
                        );
                    }
                    let inode = marker["stdin_inode"]
                        .as_u64()
                        .filter(|inode| *inode > 0)
                        .ok_or("FailedSend marker omitted the original stdin pipe inode")?;
                    let retirement = AgentActionEnvelope {
                        action_id: "failed-send-original-retirement".into(),
                        actor: root_actor.clone(),
                        expected_revision: None,
                        action: AgentAction::TaskWithdraw(
                            harness_contract::agent_action::TaskWithdrawInput {
                                task_ref: reviewer.task_ref.clone(),
                                reason_ref: content_ref.into(),
                                evidence_refs: vec![content_ref.into()],
                            },
                        ),
                    };
                    let receipt = services.submit_agent_action(&retirement).await?;
                    if receipt.status != harness_contract::agent_action::AgentActionStatus::Applied
                    {
                        return Err("original FailedSend retirement was not Applied".into());
                    }
                    wait_process_pipe_readers_closed_665(inode).await
                })
                .await;
                hold.release.notify_one();
                let original_write_end = closed_pipe
                    .expect("original pipe readers must close within the original command deadline")
                    .expect("original pipe and retirement must be authenticated before release");
                let (command_result, driver_failure) = tokio::time::timeout_at(
                    fault_deadline,
                    async {
                        let command_result = running.await.expect("original Process driver task");
                        let driver_failure = loop {
                            match driver_observation.try_recv() {
                                Ok(failure) => break failure,
                                Err(std::sync::mpsc::TryRecvError::Empty) => {
                                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                                }
                                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                                    panic!("original Process driver observation disconnected");
                                }
                            }
                        };
                        (command_result, driver_failure)
                    },
                )
                .await
                .expect(
                    "actual write failure must be observed within the original command deadline",
                );
                assert!(command_result.is_err());
                assert!(
                driver_failure
                    .starts_with("ProcessJsonl protocol failed: write ProcessJsonl Runtime message: "),
                "FailedSend requires the original write_all/flush error, not command timeout or EOF: {}",
                driver_failure.chars().take(1200).collect::<String>()
            );
                drop(original_write_end);
                wait_retired_process(services, packet, &reviewer.task_ref).await;
                assert!(process_materials(services, packet)
                    .iter()
                    .all(|(_, material)| material.tool_invocation_id != "failed-actual-send"));
                let source = format!(
                    "execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                );
                assert!(services
                    .event_store()
                    .list_stream(&source)
                    .unwrap()
                    .iter()
                    .any(|event| event.kind == "execution.agent_tool.receipt"
                        && event
                            .payload
                            .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                            .is_some()));
                let acquisitions = services
                    .event_store()
                    .list_stream(&source)
                    .unwrap()
                    .into_iter()
                    .filter(|event| event.kind == "execution.agent_tool.receipt")
                    .filter_map(|event| {
                        event
                            .payload
                            .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                            .cloned()
                    })
                    .map(|value| {
                        serde_json::from_value::<crate::context_evidence::ReadAcquisitionSeal>(
                            value,
                        )
                        .expect("actual receipt acquisition seal")
                    })
                    .filter(|seal| seal.invocation_id == "failed-actual-send")
                    .collect::<Vec<_>>();
                assert_eq!(acquisitions.len(), 1);
                let acquisition = &acquisitions[0];
                assert_eq!(acquisition.graph_id, packet.graph_id());
                assert_eq!(acquisition.node_id, packet.node_id());
                assert_eq!(acquisition.attempt, packet.attempt);
                assert_eq!(acquisition.tool_name, "read_file");
                let primary = services
                    .event_store()
                    .event_by_idempotency_key(&acquisition.primary_stream, &acquisition.primary_key)
                    .unwrap()
                    .expect("failed send retains the original acquisition primary");
                let index = services
                    .event_store()
                    .event_by_idempotency_key(&acquisition.index_stream, &acquisition.index_key)
                    .unwrap()
                    .expect("failed send retains the original acquisition index");
                assert_eq!(primary.transaction_id, index.transaction_id);
                assert_eq!(
                    primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                    index.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
                );
                assert_eq!(
                    index.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                    serde_json::to_value(acquisition).unwrap()
                );
                eprintln!(
                    "R1_PROCESS_665_SEND {}",
                    serde_json::json!({
                        "run_id": packet.run_id(),
                        "graph_id": packet.graph_id(),
                        "node_id": packet.node_id(),
                        "attempt": packet.attempt,
                        "request_id": acquisition.invocation_id,
                        "driver_failure": driver_failure.chars().take(1200).collect::<String>(),
                        "acquisition_primary": primary.event_id,
                        "acquisition_index": index.event_id,
                        "confirmed_exit": confirmed_run_exit_with_retry(services.event_store(), packet).is_some(),
                    })
                );
                assert_eq!(
                    services.goal_store().get(&before.id).unwrap().unwrap(),
                    before
                );
                return true;
            }
            RealProcessProofCase::Preview => {
                let huge = "actual complete source body\n".repeat(16_384);
                std::fs::write(services.workspace_root().join("review-effect.txt"), &huge).unwrap();
                let output = session
                    .execute_tool(
                        "actual-frame-preview",
                        "read_file",
                        r#"{"path":"review-effect.txt","complete":true}"#,
                    )
                    .await
                    .unwrap();
                let preview: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(preview["kind"], "process_tool_preview");
                assert_eq!(preview["truncated"], true);
                let material = assert_process_delivery(services, packet, "actual-frame-preview");
                assert!(material.body_omitted);
                assert!(material.delivered_extent.is_none());
                assert_process_review_refused(
                    services,
                    envelope,
                    "preview-is-not-complete-delivery",
                )
                .await;
                let (_, runtime) =
                    root_reads_generic_results(services, host.clone(), content_ref, false, true)
                        .await;
                write_root_review_effect(
                    &runtime,
                    "restore-after-frame-preview",
                    "actual delegated write\nstable middle line\ninitial dependency last line\n",
                )
                .await;
            }
            RealProcessProofCase::Pages => {
                for (index, input) in [
                    serde_json::json!({"path":"review-effect.txt","offset":0,"limit":1}),
                    serde_json::json!({"path":"review-effect.txt","offset":1,"limit":1}),
                ]
                .into_iter()
                .enumerate()
                {
                    let id = format!("same-attempt-page-{index}");
                    session
                        .execute_tool(&id, "read_file", &input.to_string())
                        .await
                        .unwrap();
                    let material = assert_process_delivery(services, packet, &id);
                    assert_eq!(
                        material.grade,
                        crate::context_evidence::ReadDeliveryGrade::AgentChannelSend
                    );
                    assert_process_review_refused(services, envelope, &id).await;
                }
                let peer = start_r1_scoped_goal_reviewer(
                    services,
                    root_actor,
                    content_ref,
                    "page-peer",
                    false,
                )
                .await;
                peer.reader
                    .execute_tool(
                        "other-attempt-final-page",
                        "read_file",
                        r#"{"path":"review-effect.txt","offset":2,"limit":1}"#,
                    )
                    .await
                    .unwrap();
                assert_process_review_refused(
                    services,
                    envelope,
                    "cross-attempt-cannot-complete-pages",
                )
                .await;
                peer.finish(services).await;
                let (_, runtime) =
                    root_reads_generic_results(services, host.clone(), content_ref, false, true)
                        .await;
                write_root_review_effect(
                    &runtime,
                    "advance-page-content-version",
                    "new first page\nnew middle page\nnew final page\n",
                )
                .await;
                session
                    .execute_tool(
                        "different-version-final-page",
                        "read_file",
                        r#"{"path":"review-effect.txt","offset":2,"limit":1}"#,
                    )
                    .await
                    .unwrap();
                assert_process_review_refused(
                    services,
                    envelope,
                    "mixed-content-versions-cannot-union",
                )
                .await;
                write_root_review_effect(
                    &runtime,
                    "restore-after-version-pages",
                    "actual delegated write\nstable middle line\ninitial dependency last line\n",
                )
                .await;
            }
            RealProcessProofCase::LateDelivery => {
                let old = "old actual acquisition line\n".repeat(1024);
                std::fs::write(services.workspace_root().join("review-effect.txt"), &old).unwrap();
                let driver = session.clone();
                let reading = tokio::spawn(async move {
                    driver.command(serde_json::json!({
                "request_id":"late-old-acquisition","tool_name":"read_file",
                "input":{"path":"review-effect.txt","complete":true},"defer_receive":true,
            })).await
                });
                let directory = process_control_directory(services, packet);
                wait_fixture_file(&directory.join("receive-held.json"))
                    .await
                    .unwrap();
                let source = format!(
                    "execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                );
                tokio::time::timeout(std::time::Duration::from_secs(15), async {
                    loop {
                        if services
                            .event_store()
                            .list_stream(&source)
                            .unwrap()
                            .iter()
                            .any(|event| {
                                event.kind == "execution.agent_tool.receipt"
                                    && event
                                        .payload
                                        .get(crate::context_evidence::READ_ACQUISITION_FIELD)
                                        .is_some_and(|value| {
                                            value["origin"]["call_id"] == "late-old-acquisition"
                                        })
                            })
                        {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
                assert!(process_materials(services, packet)
                    .iter()
                    .all(|(_, material)| material.tool_invocation_id != "late-old-acquisition"));
                let (_, runtime) =
                    root_reads_generic_results(services, host.clone(), content_ref, false, true)
                        .await;
                write_root_review_effect(
                    &runtime,
                    "write-after-actual-acquisition",
                    "new version after acquisition\n",
                )
                .await;
                publish_fixture_file(
                    &directory.join("receive-release.json"),
                    &serde_json::json!({"release":true}),
                )
                .unwrap();
                let reply = reading.await.unwrap().unwrap();
                assert!(reply["output"]
                    .as_str()
                    .unwrap()
                    .contains("old actual acquisition"));
                let material = assert_process_delivery(services, packet, "late-old-acquisition");
                assert!(!material.body_omitted);
                assert_process_review_refused(
                    services,
                    envelope,
                    "late-send-keeps-old-acquisition-version",
                )
                .await;
                write_root_review_effect(
                    &runtime,
                    "restore-after-late-delivery",
                    "actual delegated write\nstable middle line\ninitial dependency last line\n",
                )
                .await;
            }
            RealProcessProofCase::Legacy => {
                let source = format!(
                    "execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                );
                let original = services
                    .event_store()
                    .list_stream(&source)
                    .unwrap()
                    .into_iter()
                    .find(|event| event.kind == "execution.agent_tool.receipt")
                    .unwrap();
                let mut legacy = original.payload.clone();
                legacy
                    .as_object_mut()
                    .unwrap()
                    .remove(crate::context_evidence::READ_ACQUISITION_FIELD);
                services
                    .event_store()
                    .append(crate::RuntimeEventInput {
                        stream_id: source,
                        scope: original.scope,
                        kind: original.kind,
                        status: original.status,
                        actor: Some("untrusted-legacy-replay".into()),
                        refs: original.refs,
                        payload: legacy,
                    })
                    .unwrap();
                let calls = host
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|request| request.tool_name == "read_file")
                    .count();
                assert_process_review_refused(services, envelope, "legacy-cannot-read-todays-file")
                    .await;
                assert_eq!(
                    host.calls
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|request| request.tool_name == "read_file")
                        .count(),
                    calls
                );
                return true;
            }
            RealProcessProofCase::CommandAck => {
                use harness_contract::agent::{AgentCommand, AgentCommandRequest};
                let caller = services.agent_runtime().clone();
                let original = packet.clone();
                let entered = Arc::new(tokio::sync::Notify::new());
                let started = entered.clone();
                let waiter = tokio::spawn(async move {
                    started.notify_one();
                    caller.execute_task(original).await
                });
                entered.notified().await;
                waiter.abort();
                let snapshot = services.agent_runtime().get(packet.agent_id()).unwrap();
                let request = AgentCommandRequest {
                    command_id: format!("real-ack:{}", packet.run_id()),
                    agent_id: packet.agent_id().into(),
                    expected_revision: snapshot.revision,
                    command: AgentCommand::Interrupt,
                    input: None,
                };
                let command = services.agent_runtime().command(request);
                let input = serde_json::json!({"evidence_ref":content_ref}).to_string();
                let read = session.execute_tool("drive-command-ack", "evidence_retrieve", &input);
                let (receipt, output) = tokio::join!(command, read);
                assert!(receipt.accepted, "{receipt:?}");
                output.unwrap();
                assert!(confirmed_run_exit_with_retry(services.event_store(), packet).is_none());
                let projection = services
                    .agent_action_service()
                    .project(&envelope.actor.program_id)
                    .unwrap();
                assert!(!projection.tasks[&reviewer.task_ref]
                    .active_attempts
                    .is_empty());
                assert!(services
                    .settle_abandoned_agentic_attempt(packet, "actual ACK is not exit")
                    .await
                    .is_err());
                assert_eq!(
                    services
                        .event_store()
                        .list_stream(&format!("agent:{}", packet.agent_id()))
                        .unwrap()
                        .iter()
                        .filter(|event| event.kind == "agent.run_admitted")
                        .count(),
                    1
                );
            }
            RealProcessProofCase::Restart => {
                let peer = start_r1_scoped_goal_reviewer(
                    services,
                    root_actor,
                    content_ref,
                    "restart-peer",
                    false,
                )
                .await;
                let original = peer.packet.clone();
                let withdrawn = AgentActionEnvelope {
                    action_id: "restart-peer-retire".into(),
                    actor: root_actor.clone(),
                    expected_revision: None,
                    action: AgentAction::TaskWithdraw(
                        harness_contract::agent_action::TaskWithdrawInput {
                            task_ref: peer.task_ref.clone(),
                            reason_ref: content_ref.into(),
                            evidence_refs: vec![content_ref.into()],
                        },
                    ),
                };
                services.submit_agent_action(&withdrawn).await.unwrap();
                peer.reader.finish().await;
                wait_retired_process(services, &original, &peer.task_ref).await;
                let stream = format!("agent:{}", original.agent_id());
                let head = services.event_store().stream_revision(&stream).unwrap();
                let cold = crate::AgentRuntime::new(
                    services.event_store().clone(),
                    Arc::new(crate::ProviderRegistry::empty()),
                );
                assert!(confirmed_run_exit_with_retry(services.event_store(), &original).is_some());
                cold.execute_task(original.clone()).await.unwrap();
                assert_eq!(
                    services.event_store().stream_revision(&stream).unwrap(),
                    head
                );
                let mut wrong = original.clone();
                wrong.attempt += 1;
                assert!(
                    crate::agent_runtime::confirmed_run_exit(services.event_store(), &wrong)
                        .ok()
                        .flatten()
                        .is_none()
                );
                wrong = original.clone();
                wrong.assignment.run_id = "legacy-without-original-exit".into();
                assert!(
                    crate::agent_runtime::confirmed_run_exit(services.event_store(), &wrong)
                        .ok()
                        .flatten()
                        .is_none()
                );
                assert!(confirmed_run_exit_with_retry(services.event_store(), packet).is_none());
            }
            RealProcessProofCase::ReapAndEffect => {
                use harness_contract::agent::{AgentCommand, AgentCommandRequest};
                let optional = start_r1_optional_effect(
                    services,
                    host,
                    root_actor,
                    content_ref,
                    R1ProofScenario::OptionalSharedCheckpoint,
                )
                .await;
                optional.assert_pending(services);
                let snapshot = services
                    .agent_runtime()
                    .get(optional.packet.agent_id())
                    .unwrap();
                let killed = services
                    .agent_runtime()
                    .command(AgentCommandRequest {
                        command_id: format!("real-kill:{}", optional.packet.run_id()),
                        agent_id: optional.packet.agent_id().into(),
                        expected_revision: snapshot.revision,
                        command: AgentCommand::Cancel,
                        input: None,
                    })
                    .await;
                assert!(killed.accepted, "{killed:?}");
                assert!(
                    confirmed_run_exit_with_retry(services.event_store(), &optional.packet)
                        .is_none()
                );
                assert!(services
                    .settle_abandoned_agentic_attempt(
                        &optional.packet,
                        "kill precedes wait and receipt"
                    )
                    .await
                    .is_err());
                optional.assert_pending(services);
                optional.hold.release.notify_one();
                assert!(optional.worker.await.unwrap().is_err());
                wait_retired_process(services, &optional.packet, &optional.task_ref).await;
                let events = services.event_store().list_stream(&optional.index).unwrap();
                assert!(events
                    .iter()
                    .any(|event| event.kind == "execution.agent_tool.receipt"
                        && event.payload["idempotency_key"] == optional.request.idempotency_key));
            }
            RealProcessProofCase::Effects => {
                exercise_effect_observation_negatives(
                    services,
                    reviewer,
                    envelope,
                    content_ref,
                    root_actor,
                )
                .await;
            }
            RealProcessProofCase::Authorized | RealProcessProofCase::Mechanical => {}
            RealProcessProofCase::Sources | RealProcessProofCase::NativeSources => {
                unreachable!("source cases are selected before future construction")
            }
        }
        let _material = process_proof_common_delivery(services, reviewer, case).await;
        assert!(!host.calls.lock().unwrap().is_empty());
        false
    })
}

#[inline(never)]
fn exercise_source_process_proof_case<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    host: &'fixture Arc<ReviewReadHost>,
    reviewer: &'fixture R1ScopedReviewer,
    envelope: &'fixture AgentActionEnvelope,
    case: RealProcessProofCase,
) -> std::pin::Pin<Box<impl std::future::Future<Output = bool> + 'fixture>> {
    Box::pin(async move {
        assert!(matches!(
            case,
            RealProcessProofCase::Sources | RealProcessProofCase::NativeSources
        ));
        let session = &reviewer.reader;
        let packet = &reviewer.packet;
        let before = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert!(before.terminal.is_none());
        assert!(confirmed_run_exit_with_retry(services.event_store(), packet).is_none());
        let _material = process_proof_common_delivery(services, reviewer, case).await;
        #[cfg(test)]
        {
            let program = services
                .agent_action_service()
                .project(&envelope.actor.program_id)
                .unwrap();
            services.assert_read_delivery_actor_matrix_for_test(
                &program,
                packet,
                "real-independent-final-file",
                case == RealProcessProofCase::NativeSources,
            );
        }
        let mut initial = envelope.clone();
        initial.action_id = "delivery-source-initial-review".into();
        let observed = services.submit_agent_action(&initial).await.unwrap();
        assert_eq!(
            observed.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
        let program = services
            .agent_action_service()
            .project(&envelope.actor.program_id)
            .unwrap();
        let snapshot = crate::agentic::review_evidence::validate_proof_inputs(
            services.event_store(),
            &program,
            Some(&goal),
            proof,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .unwrap();
        let serialized = serde_json::to_string(&snapshot.sources).unwrap();
        assert!(serialized.contains(&format!("session:{}", packet.session_id())));
        session
            .execute_tool(
                "advance-real-delivery-source",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        let refreshed = crate::agentic::review_evidence::validate_proof_inputs(
            services.event_store(),
            &program,
            Some(&goal),
            proof,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .unwrap();
        assert!(snapshot.sources.iter().any(|old| refreshed
            .sources
            .iter()
            .any(|current| current.stream_id == old.stream_id
                && current.expected_revision > old.expected_revision)));
        let rejected_stream = format!("proof-stale-cut:{}", packet.run_id());
        let mut expected_sources = snapshot.sources.clone();
        expected_sources.push(crate::ExpectedStreamRevision {
            stream_id: rejected_stream.clone(),
            expected_revision: 0,
        });
        let rejected =
            services
                .event_store()
                .append_transaction(crate::AppendTransactionRequest {
                    transaction_id: format!("proof-stale-cut:{}", packet.run_id()),
                    expected_streams: expected_sources,
                    events: vec![crate::RuntimeTransactionEventInput {
                        event: crate::RuntimeEventInput {
                            stream_id: rejected_stream.clone(),
                            scope: crate::RuntimeEventScope::Agent,
                            kind: "fixture.stale_cut_never_committed".into(),
                            status: None,
                            actor: None,
                            refs: vec![],
                            payload: serde_json::json!({}),
                        },
                        idempotency_key: None,
                        schema_version: 1,
                    }],
                });
        match rejected {
            Err(crate::runtime_event_store::RuntimeEventStoreError::StaleRevision {
                stream_id,
                expected,
                actual,
            }) => {
                assert_ne!(stream_id, rejected_stream);
                assert!(actual > expected);
                assert!(snapshot
                    .sources
                    .iter()
                    .any(|source| source.stream_id == stream_id
                        && source.expected_revision == expected));
            }
            other => panic!("original source cut must fail at stale CAS, got {other:?}"),
        }
        assert!(services
            .event_store()
            .list_stream(&rejected_stream)
            .unwrap()
            .is_empty());
        assert!(!host.calls.lock().unwrap().is_empty());
        false
    })
}

#[inline(never)]
fn process_proof_common_delivery<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    reviewer: &'fixture R1ScopedReviewer,
    case: RealProcessProofCase,
) -> std::pin::Pin<
    Box<
        impl std::future::Future<Output = crate::context_evidence::ReadDeliveryMaterial> + 'fixture,
    >,
> {
    Box::pin(async move {
        let session = &reviewer.reader;
        let packet = &reviewer.packet;
        session
            .execute_tool(
                "real-independent-final-file",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        let material = assert_process_delivery(services, packet, "real-independent-final-file");
        assert!(!material.body_omitted);
        assert_ne!(
            material.grade,
            crate::context_evidence::ReadDeliveryGrade::ProviderModelObserved
        );
        if matches!(
            case,
            RealProcessProofCase::Authorized | RealProcessProofCase::Mechanical
        ) {
            let actual = session.started().await.unwrap();
            assert!(actual
                .required_acceptance
                .evidence_obligations
                .iter()
                .all(|obligation| obligation.observation_requirement
                    != harness_contract::context::EvidenceObservationRequirement::ProviderModel));
            assert_eq!(
                material.grade,
                crate::context_evidence::ReadDeliveryGrade::AgentChannelSend
            );
        }
        material
    })
}

#[cfg(test)]
pub(super) async fn exercise_real_process_grade_contract() {
    use harness_contract::agent::{
        AgentDefinitionId, AgentTaskIntent, DefinitionScope, RevisionSelector,
    };
    use harness_contract::context::{
        ChildExecutionBudgetReservation, EvidenceObservationRequirement,
    };
    for provider_model in [false, true] {
        let host = Arc::new(ReviewReadHost::default());
        let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
        host.services.set(Arc::downgrade(&services)).unwrap();
        install_real_process_fixture(&services, &host).await;
        services.publish_session_execution_policy(
            "session-dispatch",
            crate::permissions::SessionExecutionPolicyControl::from_policy(
                harness_contract::policy::SessionExecutionPolicy::from_profile(
                    harness_contract::policy::AutonomyProfileId::Autonomous,
                    1,
                    harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
                ),
            ),
        );
        std::fs::write(
            services.workspace_root().join("review-effect.txt"),
            "Actual immutable requirement input, not a model observation",
        )
        .unwrap();
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(
            services.workspace_root(),
        )
        .unwrap();
        let mut required =
            resolver.compile_required_acceptance(&[], &["read:review-effect.txt".into()]);
        assert_eq!(required.evidence_obligations.len(), 1);
        if provider_model {
            crate::path_identity::require_provider_model_observation(&mut required);
        }
        assert_eq!(
            required.evidence_obligations[0].observation_requirement,
            if provider_model {
                EvidenceObservationRequirement::ProviderModel
            } else {
                EvidenceObservationRequirement::RuntimeAcquisition
            }
        );
        let run_id = uuid::Uuid::new_v4().to_string();
        let graph_id = format!("grade-graph:{run_id}");
        let node_id = format!("{graph_id}:agent");
        let task_id = format!("grade-task:{run_id}");
        let definition = services
            .definition_registry()
            .resolve_agent(
                &AgentDefinitionId::new(DefinitionScope::Workspace, "r1/controlled-process")
                    .unwrap(),
                RevisionSelector::LatestApprovedStable,
            )
            .unwrap();
        let intent = AgentTaskIntent {
            selected_agent_id: None,
            definition_ref: Some(definition.revision.revision_ref),
            granted_capabilities: vec![AgentCapability::Read],
            principal_id: "runtime.fixture".into(),
            source_turn_id: "turn-dispatch".into(),
            run_id: run_id.clone(),
            task_id: task_id.clone(),
            root_task_id: task_id,
            parent_task_id: None,
            session_id: "session-dispatch".into(),
            mission_id: services.mission_runtime().default_mission_id().into(),
            team_id: None,
            graph_id: graph_id.clone(),
            node_id: node_id.clone(),
            attempt: 1,
            expected_graph_revision: 1,
            objective: "Read only the admitted exact file".into(),
            required_acceptance: required.clone(),
            output_acceptance: vec![],
            acceptance: vec![],
            constraints: vec![],
            context_refs: vec![],
            agentic_binding: None,
            evidence_refs: vec![],
            resource_scopes: vec!["read:.".into()],
            allowed_tools: vec!["read_file".into()],
            allowed_skills: vec![],
            permission_ceiling: PermissionMode::ReadOnly,
            model_lease: "fixture".into(),
            budget_lease: ChildExecutionBudgetReservation::single(
                format!("grade-budget:{run_id}"),
                run_id,
                "fixture",
                4096,
                u64::MAX,
                1,
            ),
            deadline_at_ms: u64::MAX,
            managed_invocation: None,
            idempotency_key: format!("grade:{graph_id}"),
        };
        let mut graph = ExecutionGraph::new("Exact admitted observation grade");
        graph.id = graph_id.clone();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
            root_task_id: intent.root_task_id.clone(),
            task_id: intent.task_id.clone(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            AgentTaskExecutor::KIND,
            serde_json::to_string(&intent).unwrap(),
        );
        node.id = node_id;
        graph.nodes.push(node);
        let graph = services.compile_graph_agent_intents(graph).unwrap();
        services
            .execution_supervisor()
            .submit(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .unwrap();
        let session = wait_real_process(&services, &graph_id).await;
        assert_eq!(session.packet.required_acceptance, required);
        session
            .execute_tool(
                "exact-grade-read",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        let material = assert_process_delivery(&services, &session.packet, "exact-grade-read");
        assert_eq!(
            material.grade,
            crate::context_evidence::ReadDeliveryGrade::AgentChannelSend
        );
        let original_acquisition = material.acquisition_event_id.clone();
        session
            .execute_tool(
                "exact-grade-read",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        let replayed = process_materials(&services, &session.packet)
            .into_iter()
            .filter(|(_, material)| material.tool_invocation_id == "exact-grade-read")
            .map(|(_, material)| material)
            .collect::<Vec<_>>();
        assert_eq!(replayed.len(), 2);
        let sequences = replayed
            .iter()
            .map(|material| {
                assert_eq!(material.acquisition_event_id, original_acquisition);
                assert_eq!(
                    material.presentation_sha256,
                    replayed[0].presentation_sha256
                );
                assert_eq!(
                    material.grade,
                    crate::context_evidence::ReadDeliveryGrade::AgentChannelSend
                );
                let crate::context_evidence::ReadDeliveryTransport::ProcessChannelSend {
                    sequence,
                    ..
                } = &material.transport
                else {
                    panic!("only actual Process sends count")
                };
                *sequence
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(sequences.len(), 2);
        session.finish().await;
        let returned = services
            .agent_runtime()
            .terminal_return(session.packet.agent_id())
            .unwrap();
        let verdict = returned.acceptance_evaluation.as_ref().unwrap().verdict;
        if provider_model {
            assert_ne!(
                verdict,
                harness_contract::acceptance::AcceptanceVerdict::Satisfied
            );
            assert!(!returned
                .observed_acceptance
                .unresolved_obligation_ids
                .is_empty());
        } else {
            assert_eq!(
                verdict,
                harness_contract::acceptance::AcceptanceVerdict::Satisfied
            );
        }
        assert!(returned
            .observed_acceptance
            .observed_evidence
            .iter()
            .all(|evidence| evidence.model_observation.is_none()));
    }
}

#[inline(never)]
fn exercise_effect_observation_negatives<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    reviewer: &'fixture R1ScopedReviewer,
    envelope: &'fixture AgentActionEnvelope,
    content_ref: &'fixture str,
    root_actor: &'fixture AgentActorBinding,
) -> std::pin::Pin<Box<impl std::future::Future<Output = ()> + 'fixture>> {
    Box::pin(async move {
        let producer = start_r1_scoped_goal_worker(
            services,
            root_actor,
            content_ref,
            "f2-independent-B-producer",
            false,
            true,
        )
        .await;
        producer
            .reader
            .execute_tool(
                "f2-real-write-B",
                "edit",
                r#"{"path":"unrelated-review.txt","content":"Real independent effect B"}"#,
            )
            .await
            .unwrap();
        let (body, artifact) = publish_r1_material(
            services,
            &producer.actor,
            "f2-real-result-B",
            b"Result B describes its own governed write.",
            Vec::new(),
        )
        .await;
        let mut request = envelope.clone();
        request.action_id = "f2-public-multi-result-review".into();
        let AgentAction::ObjectiveReview(input) = &mut request.action else {
            unreachable!()
        };
        input.result_refs.push(artifact);
        reviewer
            .reader
            .execute_tool(
                "f2-read-result-B",
                "evidence_retrieve",
                &serde_json::json!({"evidence_ref":body}).to_string(),
            )
            .await
            .unwrap();
        reviewer
            .reader
            .execute_tool(
                "f2-only-read-B",
                "read_file",
                r#"{"path":"unrelated-review.txt","complete":true}"#,
            )
            .await
            .unwrap();
        assert_process_review_refused(services, &request, "write-A-read-only-B").await;
        reviewer
            .reader
            .execute_tool(
                "f2-partial-read-A",
                "read_file",
                r#"{"path":"review-effect.txt","offset":0,"limit":1}"#,
            )
            .await
            .unwrap();
        assert_process_review_refused(services, &request, "one-target-page-is-not-whole-witness")
            .await;
        reviewer
            .reader
            .execute_tool(
                "f2-full-read-A",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        assert_process_review_refused(services, &request, "full-A-still-misses-its-second-target")
            .await;
        reviewer
            .reader
            .execute_tool(
                "f2-full-second-target-A",
                "read_file",
                r#"{"path":"r1-optional-target.txt","complete":true}"#,
            )
            .await
            .unwrap();
        producer.finish(services).await;
        let program = services
            .agent_action_service()
            .project(&request.actor.program_id)
            .unwrap();
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let AgentAction::ObjectiveReview(input) = &request.action else {
            unreachable!()
        };
        let genuine = services
            .independent_result_review(
                &request.actor,
                &program,
                &input.result_refs,
                Some(&input.criterion_ref),
            )
            .await
            .expect("complete actual A and B witnesses must admit an independent review");
        let baseline = crate::agentic::review_evidence::proof_input_snapshot(
            services.event_store(),
            &program,
            Some(&goal),
            &genuine.verification,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .unwrap();
        assert!(genuine.verification.reads.len() >= 2);
        let read_a = genuine
            .verification
            .reads
            .iter()
            .find(|read| read.result_ref != *input.result_refs.last().unwrap())
            .unwrap();
        let read_b = genuine
            .verification
            .reads
            .iter()
            .find(|read| read.result_ref == *input.result_refs.last().unwrap())
            .unwrap();
        assert!(!read_a.effect_observation_refs.is_empty());
        assert!(!read_b.effect_observation_refs.is_empty());
        let mut variants = Vec::new();
        let mut borrowed = genuine.verification.clone();
        let borrowed_refs = read_b.effect_observation_refs.clone();
        borrowed
            .reads
            .iter_mut()
            .find(|read| read.result_ref == read_a.result_ref)
            .unwrap()
            .effect_observation_refs = borrowed_refs;
        variants.push(("borrow-B-reference-for-A", borrowed));
        let mut missing = genuine.verification.clone();
        missing
            .reads
            .iter_mut()
            .find(|read| read.result_ref == read_a.result_ref)
            .unwrap()
            .effect_observation_refs
            .clear();
        variants.push(("missing-target-witness", missing));
        let mut incomplete = genuine.verification.clone();
        let witnesses = &mut incomplete
            .reads
            .iter_mut()
            .find(|read| read.result_ref == read_a.result_ref)
            .unwrap()
            .effect_observation_refs;
        assert!(witnesses.len() >= 2);
        witnesses.truncate(1);
        variants.push(("nonempty-but-incomplete-multitarget-witness", incomplete));
        let mut counterfeit_kind = genuine.verification.clone();
        counterfeit_kind
            .reads
            .iter_mut()
            .find(|read| read.result_ref == read_a.result_ref)
            .unwrap()
            .result_kind = harness_contract::goal::GoalResultKind::Content;
        variants.push(("wrong-result-kind", counterfeit_kind));
        for (label, mut forged) in variants {
            forged.work_manifest_digest = baseline.work_digest.clone();
            forged.effect_manifest_digest = Some(baseline.effects.digest.clone());
            if !forged.independence_required {
                forged.review_policy_digest = Some(baseline.policy_digest.clone());
            }
            let inputs = forged.input_set.as_mut().unwrap();
            inputs.semantic_digest = baseline.semantic_digest.clone();
            inputs.acceptance_policy_digest = baseline.policy_digest.clone();
            inputs.source_revisions = baseline
                .sources
                .iter()
                .map(|source| (source.stream_id.clone(), source.expected_revision))
                .collect();
            assert_eq!(forged.input_set, genuine.verification.input_set);
            if let Ok(recomputed) = crate::agentic::review_evidence::proof_input_snapshot(
                services.event_store(),
                &program,
                Some(&goal),
                &forged,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref(),
                },
            ) {
                forged.work_manifest_digest = recomputed.work_digest;
                forged.effect_manifest_digest = Some(recomputed.effects.digest);
                if !forged.independence_required {
                    forged.review_policy_digest = Some(recomputed.policy_digest.clone());
                }
                let inputs = forged.input_set.as_mut().unwrap();
                inputs.semantic_digest = recomputed.semantic_digest;
                inputs.acceptance_policy_digest = recomputed.policy_digest;
                inputs.source_revisions = recomputed
                    .sources
                    .into_iter()
                    .map(|source| (source.stream_id, source.expected_revision))
                    .collect();
                inputs.task_refs = recomputed.task_refs;
                inputs.artifact_refs = recomputed.artifact_refs;
            }
            let before_goal = services.goal_store().get(&goal.id).unwrap().unwrap();
            let before_program = services
                .agent_action_service()
                .project(&request.actor.program_id)
                .unwrap();
            let refusal = crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &program,
                Some(&goal),
                &forged,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref(),
                },
            )
            .err()
            .expect("correct canonical summaries cannot authorize another result's observation");
            assert!(!refusal.contains("stale v2 proof"), "{label}: {refusal}");
            let mut internal = request.clone();
            internal.action_id = format!("f2-internal-negative:{label}");
            let applied = services
                .agent_action_service()
                .apply_verified_review(&internal, &forged);
            assert!(
                applied.is_err()
                    || applied.as_ref().is_ok_and(|observation| observation.status
                        == harness_contract::agent_action::AgentActionStatus::Rejected),
                "{label}"
            );
            let mut historical = before_goal.clone();
            let mut record = harness_contract::goal::ObjectiveReviewRecord {
                review_id: format!("negative:{label}"),
                criterion_ref: input.criterion_ref.clone(),
                spec_revision: goal.spec_revision,
                input_manifest_digest: baseline.work_digest.clone(),
                result_refs: input.result_refs.clone(),
                evidence_refs: input.evidence_refs.clone(),
                reviewer_actor: request.actor.actor_id.clone(),
                reviewer_execution_id: request.actor.execution_id.clone(),
                producer_refs: genuine.producers.clone(),
                decision: "satisfied".into(),
                reason_ref: input.reason_ref.clone(),
                verification: None,
            };
            record.criterion_ref = input.criterion_ref.clone();
            record.verification = Some(forged);
            historical.reviews.push(record);
            assert!(
                crate::agentic::review_evidence::committed_proof_sources(
                    services.event_store(),
                    &historical,
                    crate::agentic::review_evidence::ResultContentReaders {
                        process_inputs: None,
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: services.session_history_reader().as_deref()
                    }
                )
                .is_err(),
                "exact-A B proof-source gate must reject {label}"
            );
            assert_eq!(
                services.goal_store().get(&goal.id).unwrap().unwrap(),
                before_goal
            );
            assert_eq!(
                services
                    .agent_action_service()
                    .project(&request.actor.program_id)
                    .unwrap(),
                before_program
            );
            assert!(before_goal.terminal.is_none());
        }
        let applied = services.submit_agent_action(&request).await.unwrap();
        assert_eq!(
            applied.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
    })
}

fn assert_real_process_effect_proof_conclusion_guards(
    services: &ReviewedFixtureServices,
    program_id: &str,
) {
    let actions = services.agent_action_service();
    let program = actions.project(program_id).unwrap();
    assert!(program.completion_request.is_some());
    let before = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let primary_result = before
        .reviews
        .iter()
        .rev()
        .find(|review| review.criterion_ref == "source-backed-delivery")
        .and_then(|review| review.verification.as_ref())
        .unwrap()
        .reads
        .iter()
        .find(|read| read.effect_observation_refs.len() >= 2)
        .unwrap()
        .result_ref
        .clone();
    let foreign_refs = before
        .reviews
        .iter()
        .rev()
        .filter_map(|review| review.verification.as_ref())
        .flat_map(|proof| proof.reads.iter())
        .find(|read| read.result_ref != primary_result && !read.effect_observation_refs.is_empty())
        .expect("the earlier actual two-result review retains result B's own witness")
        .effect_observation_refs
        .clone();
    for label in [
        "borrow-other-result",
        "missing-target",
        "missing-page",
        "wrong-kind",
    ] {
        let mut prepared = services
            .goal_store()
            .prepare_program_conclusion(&program)
            .unwrap();
        assert_eq!(
            prepared.goal.completion,
            harness_contract::goal::GoalCompletion::Satisfied,
            "all original reviews must pass before injecting only a negative proof: {:?}",
            prepared.gaps
        );
        let record = prepared
            .goal
            .reviews
            .iter()
            .rposition(|review| review.criterion_ref == "source-backed-delivery")
            .unwrap();
        let mut forged = prepared.goal.reviews[record].verification.clone().unwrap();
        let read = forged
            .reads
            .iter_mut()
            .find(|read| read.effect_observation_refs.len() >= 2)
            .expect("the real producer A wrote two independently scoped targets");
        match label {
            "borrow-other-result" => read.effect_observation_refs = foreign_refs.clone(),
            "missing-target" => read.effect_observation_refs.clear(),
            "missing-page" => read.effect_observation_refs.truncate(1),
            "wrong-kind" => read.result_kind = harness_contract::goal::GoalResultKind::Content,
            _ => unreachable!(),
        }
        if let Ok(snapshot) = crate::agentic::review_evidence::proof_input_snapshot(
            services.event_store(),
            &program,
            Some(&prepared.goal),
            &forged,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        ) {
            forged.work_manifest_digest = snapshot.work_digest;
            forged.effect_manifest_digest = Some(snapshot.effects.digest);
            if !forged.independence_required {
                forged.review_policy_digest = Some(snapshot.policy_digest.clone());
            }
            let inputs = forged.input_set.as_mut().unwrap();
            inputs.semantic_digest = snapshot.semantic_digest;
            inputs.acceptance_policy_digest = snapshot.policy_digest;
            inputs.task_refs = snapshot.task_refs;
            inputs.artifact_refs = snapshot.artifact_refs;
            inputs.source_revisions = snapshot
                .sources
                .into_iter()
                .map(|source| (source.stream_id, source.expected_revision))
                .collect();
        }
        prepared.goal.reviews[record].verification = Some(forged);
        assert!(
            crate::agentic::review_evidence::committed_proof_sources(
                services.event_store(),
                &prepared.goal,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref()
                }
            )
            .is_err(),
            "the original exact-A B proof-source validator must reject {label}"
        );
        assert!(
            actions
                .commit_program_conclusion(&program, prepared)
                .is_err(),
            "the actual A transaction must reject {label} before appending either half"
        );
        assert_eq!(
            services.goal_store().get(&before.id).unwrap().unwrap(),
            before
        );
        assert_eq!(actions.project(program_id).unwrap(), program);
        assert!(before.terminal.is_none());
        assert!(program.objective_verdict.is_none());
    }
}

async fn wait_process_graph_terminal(
    services: &Arc<RuntimeServices>,
    session: &RealProcessSession,
) {
    session.wait_exit().await;
    wait_admitted_graph_terminal(services, &session.packet).await;
}

async fn wait_admitted_graph_terminal(services: &Arc<RuntimeServices>, packet: &AgentTaskPacket) {
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let exit = confirmed_run_exit_with_retry(services.event_store(), packet);
            let graph = services.graph_state_store().load(packet.graph_id()).unwrap();
            if exit.is_some() && graph.node_statuses.values().all(|status|status.is_terminal()) { break; }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }).await.expect("original retained Runtime owner and Runner must commit the admitted attempt's exit and terminal");
}

struct RealFixtureCleanup {
    services: Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
}

impl Drop for RealFixtureCleanup {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            return;
        }
        #[cfg(test)]
        diagnostic_1120::record("cleanup.drop.enter", serde_json::json!({}));
        if let Some(hold) = self.host.held_write.get() {
            hold.release.notify_one();
        }
        if let Some(hold) = self.host.read_hold.lock().unwrap().as_ref() {
            hold.release.notify_one();
        }
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let services = self.services.clone();
            #[cfg(test)]
            diagnostic_1120::record("cleanup.spawn.requested", serde_json::json!({}));
            runtime.spawn(async move {
                #[cfg(test)]
                let task_boundary = diagnostic_1120::Boundary::enter("task", serde_json::json!({}));
                for run in services.agent_runtime().list() {
                    if !run.status.is_terminal() {
                        #[cfg(test)]
                        let cancel_boundary = diagnostic_1120::Boundary::enter(
                            "cancel",
                            serde_json::json!({
                            "run":run.run_id,"agent":run.agent_id,"revision":run.revision}),
                        );
                        services
                            .agent_runtime()
                            .command(harness_contract::agent::AgentCommandRequest {
                                command_id: format!("fixture-panic:{}", run.run_id),
                                agent_id: run.agent_id,
                                expected_revision: run.revision,
                                command: harness_contract::agent::AgentCommand::Cancel,
                                input: None,
                            })
                            .await;
                        #[cfg(test)]
                        cancel_boundary.end();
                    }
                }
                #[cfg(test)]
                let shutdown_boundary =
                    diagnostic_1120::Boundary::enter("shutdown", serde_json::json!({}));
                services.shutdown_execution().await;
                #[cfg(test)]
                {
                    shutdown_boundary.end();
                    task_boundary.end();
                }
            });
            #[cfg(test)]
            diagnostic_1120::record("cleanup.spawned", serde_json::json!({}));
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProgramPublicationProbe {
    Metadata,
    Independent,
    Publisher,
    Recursive,
    AnchorLoss,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct ProgramPublicationCase {
    pub logical: bool,
    pub encoding: Option<&'static str>,
    pub probe: ProgramPublicationProbe,
}

#[cfg(test)]
async fn commit_existing_result_selector(
    services: &ReviewedFixtureServices,
    actor: &AgentActorBinding,
    action_id: &str,
    selector: &str,
    relates_to: Vec<String>,
) -> String {
    let committed = services
        .submit_agent_action(&AgentActionEnvelope {
            action_id: action_id.into(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: selector.into(),
                kind: "note".into(),
                title: "Register the original Tool selector without republishing".into(),
                relates_to,
            }),
        })
        .await
        .unwrap();
    assert_eq!(
        committed.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let logical = committed.changed_refs[0].clone();
    let program = services
        .agent_action_service()
        .project(&actor.program_id)
        .unwrap();
    assert_eq!(program.artifacts[&logical].content_ref, selector);
    logical
}

#[cfg(test)]
async fn assert_program_review_refused(
    services: &ReviewedFixtureServices,
    reviewer: &AgentActorBinding,
    result_refs: Vec<String>,
    reason_ref: &str,
    encoding: Option<&str>,
) {
    let before = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let program_before = services
        .agent_action_service()
        .project(&reviewer.program_id)
        .unwrap();
    let refusal = services
        .submit_agent_action(&AgentActionEnvelope {
            action_id: "program-publication-negative-review".into(),
            actor: reviewer.clone(),
            expected_revision: None,
            action: AgentAction::ObjectiveReview(
                harness_contract::agent_action::ObjectiveReviewInput {
                    criterion_ref: "source-backed-delivery".into(),
                    decision: harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                    result_refs,
                    evidence_refs: vec![reason_ref.into()],
                    reason_ref: reason_ref.into(),
                    reason: String::new(),
                },
            ),
        })
        .await
        .unwrap_err();
    if encoding.is_some() {
        assert!(
            refusal.contains("strong")
                || refusal.contains("immutable")
                || refusal.contains("raw")
                || refusal.contains("anchor")
                || refusal.contains("provenance"),
            "{refusal}"
        );
    } else {
        assert!(
            refusal.contains("reviewer")
                || refusal.contains("independen")
                || refusal.contains("producer"),
            "{refusal}"
        );
    }
    assert_eq!(
        services.goal_store().get(&before.id).unwrap().unwrap(),
        before
    );
    assert_ne!(
        before.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    assert!(before.terminal.is_none());
    assert_eq!(
        services
            .agent_action_service()
            .project(&reviewer.program_id)
            .unwrap(),
        program_before
    );
    assert!(program_before.objective_verdict.is_none());
    assert!(services
        .event_store()
        .list_stream("session-terminal:native-cold-b")
        .unwrap()
        .is_empty());
    assert!(services
        .event_store()
        .session_terminal("turn-terminal:native-cold-b")
        .unwrap()
        .is_none());
    println!("R1_PROGRAM_PUBLICATION_REFUSED encoding={encoding:?} reason={refusal}");
}

#[cfg(test)]
async fn exercise_same_native_publisher_review(
    services: &ReviewedFixtureServices,
    root_actor: &AgentActorBinding,
    native: &NativeSourcePair,
    result_refs: Vec<String>,
    read_refs: Vec<String>,
    reason_ref: &str,
    recursive: bool,
) {
    let envelope = |id: &str, action| AgentActionEnvelope {
        action_id: format!("program-native-review:{id}"),
        actor: root_actor.clone(),
        expected_revision: None,
        action,
    };
    let team = services
        .submit_agent_action(&envelope(
            "team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Original Native publisher new review attempt".into(),
                mission: "Test actor independence using the actual Native executor".into(),
                objective: None,
            }),
        ))
        .await
        .unwrap();
    let team_ref = team.changed_refs[0].clone();
    let original_agent = native
        .packet
        .agentic_binding
        .as_ref()
        .unwrap()
        .agent_id
        .clone();
    let invited = services
        .submit_agent_action(&envelope(
            "invite",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Reviewer".into(),
                mission: "Read then attempt independent review".into(),
                required_capabilities: vec!["read".into()],
                existing_agent_ref: Some(original_agent.clone()),
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: vec![],
                execution_requirements: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        invited.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let publication = envelope(
        "task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref,
            title: "Read and review under a new Native attempt".into(),
            objective: "Claim and read the real immutable result before attempting review".into(),
            acceptance: "A new physical attempt cannot erase the original publisher actor".into(),
            criterion_source: Default::default(),
            acceptance_checks: vec![],
            required_capabilities: vec!["read".into()],
            depends_on: vec![],
            obligation_refs: vec![],
            purpose: harness_contract::agent_action::TaskPurpose::Exploration,
            execution_requirements: vec![],
            expertise_hints: vec![],
        }),
    );
    let task = services.submit_agent_action(&publication).await.unwrap();
    assert_eq!(
        task.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let task_ref = task.changed_refs[0].clone();
    let reply = |id: &str, name: &str, input: serde_json::Value, content: Option<&str>| {
        serde_json::json!({
            "role":"assistant", "content":content,
            "tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}}],
        })
    };
    {
        let mut replies = native._wire.replies.lock().unwrap();
        replies.push_back(reply(
            "program-native-review-claim",
            "task_claim",
            serde_json::json!({"task_ref":task_ref}),
            None,
        ));
        for (index, reference) in read_refs.iter().enumerate() {
            replies.push_back(reply(
                &format!("program-native-review-read:{index}"),
                "evidence_retrieve",
                serde_json::json!({"evidence_ref":reference}),
                None,
            ));
        }
        replies.push_back(reply(
            "program-native-review-file",
            "read_file",
            serde_json::json!({"path":"review-effect.txt","complete":true}),
            None,
        ));
        replies.push_back(reply("program-native-review-objective", "objective_review",
            serde_json::json!({
                "criterion_ref":"source-backed-delivery", "decision":"satisfied",
                "result_refs":result_refs, "evidence_refs":[reason_ref], "reason_ref":reason_ref, "reason":"",
            }), None));
        replies.push_back(reply("program-native-review-submit", "task_submit",
            serde_json::json!({"task_ref":task_ref,"artifact_refs":[],"evidence_refs":[],"unresolved":[],
                "deliverable":{"block_index":0,"kind":"report","title":"Native review refusal observed"}}),
            Some("The result was fully read. Independent review must refuse my original publisher influence.")));
        replies.push_back(serde_json::json!({"role":"assistant","content":"The review refusal is preserved; the exploratory attempt is finished."}));
    }
    let before_goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let admitted = services
        .dispatch_agentic_followups(
            &publication,
            AgenticDispatchContext {
                session_id: root_actor.session_id.clone(),
                turn_id: root_actor.turn_id.clone(),
                model_lease: "r1-native-wire".into(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: vec!["workspace:.".into()],
            },
        )
        .await
        .unwrap();
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].agent_ref, original_agent);
    let graph = services
        .graph_state_store()
        .load(&admitted[0].graph_id)
        .unwrap();
    let node = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::AgentTask)
        .unwrap();
    let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
    assert_ne!(packet.graph_id(), native.packet.graph_id());
    assert_ne!(packet.run_id(), native.packet.run_id());
    assert_eq!(
        packet.agentic_binding.as_ref().unwrap().agent_id,
        original_agent
    );
    assert!(matches!(
        packet.binding.as_ref().unwrap().executor,
        harness_contract::agent::AgentExecutorPolicy::CowdNative
    ));
    assert!(
        packet
            .allowed_tools
            .iter()
            .any(|tool| tool == "objective_review"),
        "the original Native definition must actually authorize the review tool"
    );
    wait_admitted_graph_terminal(services, &packet).await;
    let run = services.agent_runtime().get(packet.agent_id()).unwrap();
    assert_eq!(
        run.backend,
        crate::agent_run_handle::AgentBackendKind::InProcess
    );
    let events = services
        .event_store()
        .list_stream(&format!("session:{}", root_actor.session_id))
        .unwrap();
    let owner = events
        .iter()
        .find(|event| {
            event.payload["tool_call_id"] == "program-native-review-objective"
                && matches!(
                    event.kind.as_str(),
                    "tool.invocation.completed" | "tool.invocation.failed"
                )
        })
        .unwrap();
    assert_eq!(
        owner.activity_binding().unwrap().agent_run_id.as_deref(),
        Some(packet.run_id())
    );
    let reference = owner.payload["full_output_ref"].as_str().unwrap();
    let access = services
        .session_evidence_access(
            &root_actor.session_id,
            reference.strip_prefix("tool://").unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let raw = services
        .artifact_store()
        .resolve(&access.retrieval_selector)
        .unwrap();
    let bytes = services
        .artifact_store()
        .read(&raw, &raw.visibility_scope, None)
        .await
        .unwrap();
    let refusal = String::from_utf8(bytes).unwrap();
    if recursive {
        assert!(
            refusal.contains(
                "derived material producer and reviewer must be different physical executions"
            ) || refusal.contains(
                "proof original primary result influence includes its independent reviewer"
            ),
            "{refusal}"
        );
    } else {
        assert!(
            refusal.contains("review producer and reviewer must be different physical executions")
                || refusal.contains(
                    "proof original primary result influence includes its independent reviewer"
                ),
            "{refusal}"
        );
    }
    assert_eq!(
        services.goal_store().get(&before_goal.id).unwrap().unwrap(),
        before_goal
    );
    assert!(services
        .agent_action_service()
        .project(&root_actor.program_id)
        .unwrap()
        .objective_verdict
        .is_none());
    assert_ne!(
        before_goal.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    assert!(before_goal.terminal.is_none());
    assert!(services
        .event_store()
        .list_stream("session-terminal:native-cold-b")
        .unwrap()
        .is_empty());
    assert!(services
        .event_store()
        .session_terminal("turn-terminal:native-cold-b")
        .unwrap()
        .is_none());
    let read = events
        .iter()
        .filter(|event| event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
        .filter_map(|event| crate::context_evidence::decode_read_delivery(&event.payload).ok())
        .find(|material| material.tool_invocation_id == "program-native-review-read:0")
        .unwrap();
    assert_native_task_material_wire(services, &packet, &read, &native._wire.requests).await;
    println!(
        "R1_PROGRAM_NATIVE_NEW_ATTEMPT_REFUSED actor={original_agent} original={} reviewer={}",
        native.packet.run_id(),
        packet.run_id()
    );
}

#[cfg(test)]
#[inline(never)]
fn exercise_program_publication_review<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    _host: &'fixture Arc<ReviewReadHost>,
    root_actor: &'fixture AgentActorBinding,
    reviewer: &'fixture R1ScopedReviewer,
    native: &'fixture NativeSourcePair,
    case: ProgramPublicationCase,
    reason_ref: &'fixture str,
    result_refs: &'fixture mut Vec<String>,
) -> std::pin::Pin<Box<impl std::future::Future<Output = bool> + 'fixture>> {
    Box::pin(async move {
        let (logical, selector) = native.registration.as_ref().unwrap();
        let selected = if case.logical { logical } else { selector };
        println!("R1_PROGRAM_PUBLICATION_CASE {case:?} actor={} execution={} selector={selector} logical={logical}",
        native.packet.agent_id(), native.packet.graph_id());
        assert_eq!(native.result_contents[1], *selector);
        if case.encoding.is_none() {
            services
                .assert_program_publication_for_test(root_actor, selected, selector, &native.packet)
                .await;
        }
        let mut recursive = None;
        let mut recursive_primary = None;
        if case.probe == ProgramPublicationProbe::Recursive {
            let producer = start_r1_scoped_goal_worker(
                services,
                root_actor,
                selector,
                "program-recursive-producer",
                false,
                false,
            )
            .await;
            let bytes = b"A distinct derived report produced after actually reading the original Tool publication.";
            std::fs::write(services.workspace_root().join("program-derived.txt"), bytes).unwrap();
            let output = producer
                .reader
                .execute_tool(
                    "program-derived-publish",
                    "artifact_publish",
                    &serde_json::json!({"source":"file", "path":"program-derived.txt",
                "sha256":crate::context_evidence::read_sha256(bytes), "media_type":"text/plain"})
                    .to_string(),
                )
                .await
                .unwrap();
            let publication: serde_json::Value = serde_json::from_str(&output).unwrap();
            assert_eq!(publication["status"], "published");
            let derived = publication["content_ref"].as_str().unwrap().to_string();
            let derived_logical = commit_existing_result_selector(
                services,
                &producer.actor,
                "program-derived-registration",
                &derived,
                if case.logical {
                    vec![logical.clone()]
                } else {
                    Vec::new()
                },
            )
            .await;
            let program = services
                .agent_action_service()
                .project(&root_actor.program_id)
                .unwrap();
            assert_eq!(
                crate::agentic::review_evidence::review_result_identity(&program, selector)
                    .unwrap(),
                *logical
            );
            assert_eq!(
                crate::agentic::review_evidence::review_result_identity(&program, logical).unwrap(),
                *logical
            );
            recursive_primary = Some(
                services
                    .program_primary_for_test(
                        root_actor,
                        &derived_logical,
                        selector,
                        &native.packet,
                    )
                    .await,
            );
            result_refs.retain(|reference| reference != logical && reference != selector);
            result_refs.push(derived_logical.clone());
            reviewer
                .reader
                .execute_tool(
                    "program-independent-derived-read",
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":derived}).to_string(),
                )
                .await
                .unwrap();
            recursive = Some((producer, derived, derived_logical));
        }
        reviewer
            .reader
            .execute_tool(
                "program-independent-effect-read",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        if case.encoding.is_some() {
            assert_program_review_refused(
                services,
                &reviewer.actor,
                result_refs.clone(),
                reason_ref,
                case.encoding,
            )
            .await;
            if let Some((producer, _, _)) = recursive {
                producer.finish(services).await;
            }
            if let Some(primary) = recursive_primary {
                primary.expect("recursive-only negative requires D metadata to resolve before rejecting original S; unknown-source fail-closed must not be misreported as recursive coverage");
            }
            return true;
        }
        if let Some(primary) = recursive_primary {
            primary.unwrap();
        }
        if matches!(
            case.probe,
            ProgramPublicationProbe::Publisher | ProgramPublicationProbe::Recursive
        ) {
            let (review_results, review_contents) =
                if let Some((_, derived, derived_logical)) = recursive.as_ref() {
                    (
                        vec![derived_logical.clone()],
                        vec![derived.clone(), selector.clone(), reason_ref.to_string()],
                    )
                } else {
                    (
                        vec![selected.clone()],
                        vec![selector.clone(), reason_ref.to_string()],
                    )
                };
            exercise_same_native_publisher_review(
                services,
                root_actor,
                native,
                review_results,
                review_contents,
                reason_ref,
                recursive.is_some(),
            )
            .await;
            if case.probe == ProgramPublicationProbe::Publisher {
                return true;
            }
        }
        if let Some((producer, _, _)) = recursive {
            producer.finish(services).await;
        }
        false
    })
}

struct NativeSourcePair {
    packet: AgentTaskPacket,
    result_refs: Vec<String>,
    result_contents: Vec<String>,
    _wire: RootWireServer,
    #[cfg(test)]
    registration: Option<(String, String)>,
}

async fn assert_native_task_material_wire(
    services: &RuntimeServices,
    packet: &AgentTaskPacket,
    material: &crate::context_evidence::ReadDeliveryMaterial,
    requests: &std::sync::Mutex<Vec<Vec<u8>>>,
) {
    use crate::context_evidence::{
        ReadDeliveryGrade, ReadDeliveryRecipient, ReadDeliveryTransport, READ_ACQUISITION_FIELD,
    };
    let ReadDeliveryRecipient::AgentTask { identity } = &material.recipient else {
        panic!("Native AgentTask evidence cannot be supplied by Root");
    };
    assert_eq!(identity, &packet.assignment.execution_identity);
    assert_ne!(
        packet.assignment.execution_identity.principal_id(),
        packet.agent_id(),
        "Native actor regression requires distinct principal and instance namespaces"
    );
    assert_eq!(
        material.acquisition.step.actor_ref.as_deref(),
        Some(packet.assignment.execution_identity.principal_id())
    );
    assert_eq!(material.grade, ReadDeliveryGrade::ProviderModelObserved);
    assert_eq!(material.acquisition.graph_id, packet.graph_id());
    assert_eq!(material.acquisition.node_id, packet.node_id());
    assert_eq!(material.acquisition.attempt, packet.attempt);
    assert_eq!(
        material.acquisition.step.run_ref.as_deref(),
        Some(packet.run_id())
    );
    let primary = services
        .event_store()
        .event_by_idempotency_key(
            &material.acquisition.primary_stream,
            &material.acquisition.primary_key,
        )
        .unwrap()
        .unwrap();
    let index = services
        .event_store()
        .event_by_idempotency_key(
            &material.acquisition.index_stream,
            &material.acquisition.index_key,
        )
        .unwrap()
        .unwrap();
    assert_eq!(primary.event_id, material.acquisition_event_id);
    assert_eq!(index.event_id, material.index_event_id);
    assert_eq!(primary.transaction_id, index.transaction_id);
    assert_eq!(
        primary.payload[READ_ACQUISITION_FIELD],
        index.payload[READ_ACQUISITION_FIELD]
    );
    let ReadDeliveryTransport::NativeProviderContinuation {
        request_id,
        wire_body_sha256,
        wire_artifact,
        packed_sequence,
        outcome_sequence,
        step_render,
        raw_ref,
        ..
    } = &material.transport
    else {
        panic!("Native Task must retain original provider transport")
    };
    let bytes = services
        .artifact_store()
        .read(
            wire_artifact,
            &format!("session:{}", packet.session_id()),
            None,
        )
        .await
        .unwrap();
    let record: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let body = &record["provider_request"]["wire_request"]["body"];
    let canonical = serde_json::to_vec(body).unwrap();
    assert_eq!(
        crate::context_evidence::read_sha256(&canonical).trim_start_matches("sha256:"),
        wire_body_sha256.as_str()
    );
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .any(|sent| sent == &canonical));
    let presentation = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| {
            message["role"] == "tool" && message["tool_call_id"] == material.tool_invocation_id
        })
        .expect("the exact ToolResult must occur in the actual provider request")["content"]
        .as_str()
        .unwrap();
    assert_eq!(
        crate::context_evidence::read_sha256(presentation.as_bytes()),
        material.presentation_sha256
    );
    assert_eq!(presentation.len() as u64, material.presentation_bytes);
    let history = services.session_history_reader().unwrap();
    let packed = history
        .domain_events_by_kind_blocking(
            packet.session_id(),
            "context.provider_request_packed",
            *packed_sequence,
            1,
        )
        .unwrap();
    assert_eq!(packed.len(), 1);
    assert_eq!(packed[0].sequence, *packed_sequence);
    assert_eq!(packed[0].payload["request_id"], *request_id);
    assert_eq!(packed[0].payload["body_sha256"], *wire_body_sha256);
    assert_eq!(
        packed[0].payload["step_render"]["step_id"],
        step_render.step_id
    );
    assert_ne!(step_render.step_id, material.acquisition.step.step_id);
    let outcome = history
        .domain_events_by_kind_blocking(
            packet.session_id(),
            "context.provider_attempt_outcome",
            *outcome_sequence,
            1,
        )
        .unwrap();
    assert_eq!(outcome.len(), 1);
    assert_eq!(outcome[0].sequence, *outcome_sequence);
    assert_eq!(outcome[0].payload["request_id"], *request_id);
    assert_eq!(outcome[0].payload["terminal_status"], "completed");
    assert!(outcome_sequence > packed_sequence);
    let access = services
        .session_evidence_access(packet.session_id(), raw_ref.id())
        .await
        .unwrap()
        .unwrap();
    let original = services
        .artifact_store()
        .resolve(&access.retrieval_selector)
        .unwrap();
    let original_bytes = services
        .artifact_store()
        .read(&original, &access.visibility_scope, None)
        .await
        .unwrap();
    assert_eq!(
        crate::context_evidence::read_sha256(&original_bytes),
        material.acquisition.body.output_sha256
    );
    assert_eq!(
        original_bytes.len() as u64,
        material.acquisition.body.output_bytes
    );
}

#[inline(never)]
fn exercise_native_task_sources<'fixture>(
    services: &'fixture ReviewedFixtureServices,
    host: &'fixture Arc<ReviewReadHost>,
    root_actor: &'fixture AgentActorBinding,
    proof_scenario: R1ProofScenario,
) -> std::pin::Pin<Box<impl std::future::Future<Output = NativeSourcePair> + 'fixture>> {
    Box::pin(async move {
        use harness_contract::agent::*;
        let _ = proof_scenario;
        let wire = RootWireServer::start().await;
        services
            .provider_registry()
            .replace_with_context_windows(
                crate::config::ProvidersConfig {
                    providers: std::collections::HashMap::from([(
                        "r1-native-loopback".into(),
                        crate::config::ProviderConfig {
                            name: "r1-native-loopback".into(),
                            base_url: wire.endpoint.clone(),
                            api_key: "local-fixture".into(),
                            models: vec!["r1-native-wire".into()],
                            protocol: Some("completions".into()),
                            parallel_tool_calls: Default::default(),
                            early_tool_start: Default::default(),
                        },
                    )]),
                },
                std::collections::BTreeMap::from([("r1-native-wire".into(), 262_144)]),
            )
            .unwrap();
        assert_eq!(
            services
                .provider_registry()
                .pin()
                .configured_context_window("r1-native-wire"),
            Some(262_144)
        );
        let resources = services.provider_resource_config().read().unwrap().clone();
        services
            .replace_provider_resource_config(resources)
            .unwrap();
        let registry = services.definition_registry();
        let base = registry
            .resolve_agent(
                &AgentDefinitionId::new(DefinitionScope::Builtin, "cowd/execute").unwrap(),
                RevisionSelector::LatestApprovedStable,
            )
            .unwrap();
        let mut manifest = base.revision.manifest.clone();
        manifest.definition_id =
            AgentDefinitionId::new(DefinitionScope::Workspace, "r1/native-sources").unwrap();
        manifest.executor = AgentExecutorPolicy::CowdNative;
        manifest.model_policy.allowed_models = vec!["r1-native-wire".into()];
        manifest.model_policy.fallback_allowed = false;
        let revision = registry
            .agents()
            .store_revision(manifest, &base.agent_markdown)
            .unwrap();
        registry
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: revision.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "fixture:approved-native-sources".into(),
                },
                content_digest: revision.revision.content_digest.clone(),
            })
            .unwrap();
        services.refresh_definition_catalog().unwrap();
        let envelope = |id: &str, action| AgentActionEnvelope {
            action_id: format!("native-sources:{id}"),
            actor: root_actor.clone(),
            expected_revision: None,
            action,
        };
        let team = services
            .submit_agent_action(&envelope(
                "team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Native source execution".into(),
                    mission: "Read with the original Native backend".into(),
                    objective: None,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            team.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let team_ref = team.changed_refs[0].clone();
        let member = services.submit_agent_action(&envelope("member", AgentAction::AgentInvite(AgentInviteInput {
        team_ref:team_ref.clone(), role:"Native source worker".into(),
        mission:"Read review-effect.txt and publish the exact selected file, then submit the authored report".into(),
        required_capabilities:vec!["read".into()], existing_agent_ref:None,
        definition_ref:Some(REAL_NATIVE_DEFINITION.into()), model_profile_ref:None,
        expertise_hints:vec![], execution_requirements:vec![],
    }))).await.unwrap();
        assert_eq!(
            member.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let review_team = services
            .submit_agent_action(&envelope(
                "independent-review-team",
                AgentAction::TeamCreate(TeamCreateInput {
                    name: "Native source independent review".into(),
                    mission:
                        "Review the Native deliverable without borrowing held fixture executions"
                            .into(),
                    objective: None,
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            review_team.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let independent_reviewer = services
            .submit_agent_action(&envelope(
                "independent-reviewer",
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: review_team.changed_refs[0].clone(),
                    role: "Native source verifier".into(),
                    mission:
                        "Independently inspect and review only the submitted Native deliverable"
                            .into(),
                    required_capabilities: vec!["read".into()],
                    existing_agent_ref: None,
                    definition_ref: Some(REAL_PROCESS_DEFINITION.into()),
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            independent_reviewer.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let independent_reviewer = independent_reviewer.changed_refs[0].clone();
        assert!(!services
            .agent_runtime()
            .running_agentic_members(&root_actor.program_id)
            .contains(&independent_reviewer));
        let mut publication = envelope("task", AgentAction::TaskPublish(TaskPublishInput {
        team_ref:team_ref.clone(), title:"Native immutable source proof".into(),
        objective:"Read review-effect.txt and publish the exact selected file, then submit the authored report".into(),
        acceptance:"A claimed, read-only Native attempt sends the full result through the actual provider".into(),
        criterion_source:Default::default(), acceptance_checks:vec!["read:review-effect.txt".into()],
        required_capabilities:vec!["read".into()], depends_on:vec![], obligation_refs:vec![],
        purpose:harness_contract::agent_action::TaskPurpose::Delivery,
        execution_requirements:vec![], expertise_hints:vec![],
    }));
        #[cfg(test)]
        let _ = NATIVE_TASK_CASE.try_with(|case| {
            if let AgentAction::TaskPublish(input) = &mut publication.action {
                if let Some(acceptance) = &case.acceptance {
                    input.acceptance = acceptance.clone();
                    input.acceptance_checks.clear();
                }
                input.criterion_source = case.criterion_source.clone();
            }
        });
        let task = services.submit_agent_action(&publication).await.unwrap();
        assert_eq!(
            task.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let task_ref = task.changed_refs[0].clone();
        let reply = |id: &str, name: &str, input: serde_json::Value, content: Option<&str>| {
            serde_json::json!({
                "role":"assistant", "content":content,
                "tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":name,"arguments":input.to_string()}}],
            })
        };
        let selected_bytes =
            std::fs::read(services.workspace_root().join("review-effect.txt")).unwrap();
        let selected_hash = crate::context_evidence::read_sha256(&selected_bytes);
        let publications = [(
            "native-publication-small",
            format!("text/plain;fixture={}", "s".repeat(1024)),
        )];
        let mut submitted_unresolved: Vec<String> = Vec::new();
        #[cfg(test)]
        if let Ok(unresolved) = NATIVE_TASK_CASE.try_with(|case| case.unresolved.clone()) {
            submitted_unresolved = unresolved;
        }
        {
            let mut replies = wire.replies.lock().unwrap();
            replies.push_back(reply("native-claim", "task_claim",
            serde_json::json!({"task_ref":task_ref,"reason":"This original read-only Task fits my approved role"}), None));
            replies.push_back(reply(
                "native-preview-read",
                "read_file",
                serde_json::json!({"path":"review-effect.txt","offset":0,"limit":1}),
                None,
            ));
            replies.push_back(reply(
                "native-full-read",
                "read_file",
                serde_json::json!({"path":"review-effect.txt","complete":true}),
                None,
            ));
            for (call, media_type) in &publications {
                replies.push_back(reply(call, "artifact_publish", serde_json::json!({
                "source":"file","path":"review-effect.txt","sha256":selected_hash,"media_type":media_type,
            }), None));
            }
            #[cfg(test)]
            if NATIVE_TASK_CASE
                .try_with(|case| case.commit_report)
                .unwrap_or(false)
            {
                replies.push_back(reply("native-first-claim-artifact", "artifact_commit", serde_json::json!({
                "content_ref":"current_message_block:0","kind":"report",
                "title":"First-claim explicit report","relates_to":[task_ref],
            }), Some("A real first-claim artifact authored and committed by the active Native producer after its complete source read.")));
            }
            replies.push_back(reply("native-submit", "task_submit", serde_json::json!({
            "task_ref":task_ref,"artifact_refs":[],"evidence_refs":[],"unresolved":submitted_unresolved,
            "deliverable":{"block_index":0,"kind":"report","title":"Actual Native source report"},
        }), Some("I inspected the complete current review-effect.txt through the original Native provider ToolResult. The bounded file was published without changes; this report records the actual source-backed result. No unresolved items.")));
            replies.push_back(serde_json::json!({"role":"assistant",
            "content":"The claimed Task has been submitted. The full source was inspected and the immutable report is complete. No unresolved items."}));
        }
        {
            let mut replies = wire.replies.lock().unwrap();
            replies.push_back(reply(
                "native-rework-claim",
                "task_claim",
                serde_json::json!({"task_ref":task_ref,
                "reason":"The gap review reopened this Task; I claim the rework attempt"}),
                None,
            ));
            replies.push_back(reply(
                "native-rework-preview-read",
                "read_file",
                serde_json::json!({"path":"review-effect.txt","offset":0,"limit":1}),
                None,
            ));
            replies.push_back(reply(
                "native-rework-full-read",
                "read_file",
                serde_json::json!({"path":"review-effect.txt","complete":true}),
                None,
            ));
            replies.push_back(reply("native-rework-publication-small", "artifact_publish", serde_json::json!({
            "source":"file","path":"review-effect.txt","sha256":selected_hash,"media_type":"text/plain;fixture=rework",
        }), None));
            replies.push_back(reply("native-rework-submit", "task_submit", serde_json::json!({
            "task_ref":task_ref,"artifact_refs":[],"evidence_refs":[],"unresolved":submitted_unresolved,
            "deliverable":{"block_index":0,"kind":"report","title":"Actual Native source rework report"},
        }), Some("The reopened Task was re-claimed and the complete current review-effect.txt was inspected again through the original Native provider ToolResult. The rework publication records the same source-backed result. No unresolved items.")));
            replies.push_back(serde_json::json!({"role":"assistant",
            "content":"The rework attempt has been submitted. The full source was inspected again and the immutable report is complete. No unresolved items."}));
        }
        #[cfg(test)]
        let encoding_guard = if let R1ProofScenario::ProgramPublication(case) = proof_scenario {
            case.encoding.map(|field| {
                RuntimeServices::program_raw_encoding_for_test(
                    services.event_store().clone(),
                    &root_actor.session_id,
                    "native-publication-small",
                    field,
                )
            })
        } else {
            None
        };
        #[cfg(test)]
        let setup = NATIVE_TASK_SETUP
            .try_with(|slot| slot.borrow_mut().take())
            .ok()
            .flatten();
        #[cfg(test)]
        let submit_pause = setup.as_ref().map(|_| {
            let arrived = Arc::new(tokio::sync::Notify::new());
            let resume = Arc::new(tokio::sync::Notify::new());
            *wire.pause.lock().unwrap() =
                Some(("native-submit".into(), arrived.clone(), resume.clone()));
            (arrived, resume)
        });
        let admitted = services
            .dispatch_agentic_followups(
                &publication,
                AgenticDispatchContext {
                    session_id: root_actor.session_id.clone(),
                    turn_id: root_actor.turn_id.clone(),
                    model_lease: "r1-native-wire".into(),
                    permission_ceiling: PermissionMode::ReadOnly,
                    resource_scopes: vec!["workspace:.".into()],
                },
            )
            .await
            .unwrap();
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].agent_ref, member.changed_refs[0]);
        let graph = services
            .graph_state_store()
            .load(&admitted[0].graph_id)
            .unwrap();
        let node = graph
            .nodes
            .iter()
            .find(|node| node.kind == ExecutionNodeKind::AgentTask)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref).unwrap();
        packet.validate_cohort_prompt_package().unwrap();
        let binding = packet.binding.as_ref().unwrap();
        binding.validate().unwrap();
        assert_eq!(binding.definition_ref, revision.revision.revision_ref);
        assert!(matches!(binding.executor, AgentExecutorPolicy::CowdNative));
        assert!(binding.release.is_none());
        let approved = registry
            .resolve_agent(
                &binding.definition_ref.definition_id,
                RevisionSelector::LatestApprovedStable,
            )
            .unwrap();
        assert_eq!(approved.revision.revision_ref, binding.definition_ref);
        assert_eq!(
            approved.revision.content_digest,
            revision.revision.content_digest
        );
        let assignments = registry
            .agents()
            .release_assignments(&binding.definition_ref.definition_id)
            .unwrap();
        assert!(assignments.iter().any(|assignment| assignment.revision_ref
            == binding.definition_ref
            && assignment.channel == ReleaseChannel::Stable
            && assignment.status == ReleaseAssignmentStatus::Active
            && assignment.content_digest == revision.revision.content_digest));
        assert!(binding
            .effective_capabilities
            .iter()
            .all(|capability| matches!(
                capability,
                AgentCapability::Read | AgentCapability::Search
            )));
        let parent = graph
            .parent_execution
            .as_ref()
            .expect("Native dispatch must retain the original parent");
        assert_eq!(
            Some(parent.execution_id.as_str()),
            root_actor.root_execution_id.as_deref()
        );
        let parent_graph = services
            .graph_state_store()
            .load(&parent.execution_id)
            .unwrap();
        assert!(parent_graph
            .nodes
            .iter()
            .any(|node| node.id == parent.node_id && node.kind == ExecutionNodeKind::InlineModel));
        assert!(matches!(packet.agentic_binding.as_ref().unwrap().focus,
        AgenticExecutionFocus::TaskExecute { ref task_ref } if task_ref == &task.changed_refs[0]));
        assert_eq!(packet.permission_ceiling, PermissionMode::ReadOnly);
        assert!(!packet
            .allowed_tools
            .iter()
            .any(|name| name == "edit" || name == "execute_code"));
        assert!(packet.allowed_tools.iter().any(|name| name == "read_file"));
        assert!(packet
            .allowed_tools
            .iter()
            .any(|name| name == "task_submit"));
        #[cfg(test)]
        if let Some(setup) = setup {
            let (arrived, resume) = submit_pause.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(30), arrived.notified())
                .await
                .unwrap_or_else(|elapsed| {
                    panic!(
                        "original TaskSubmit wire pause did not arrive within 30s: {elapsed:?}; {}",
                        RealProcessSession {
                            services: services.0.clone(),
                            packet: packet.clone()
                        }
                        .failure_diagnostic(),
                    );
                });
            let actor = services
                .resolve_agent_action_actor(
                    &ExecutionParentBinding {
                        execution_id: packet.graph_id().into(),
                        node_id: packet.node_id().into(),
                    },
                    None,
                )
                .await
                .unwrap();
            let input: TaskSubmitInput = {
                let replies = wire.replies.lock().unwrap();
                serde_json::from_str(
                    replies.front().unwrap()["tool_calls"][0]["function"]["arguments"]
                        .as_str()
                        .unwrap(),
                )
                .unwrap()
            };
            let worker = AgentActionEnvelope {
                action_id: "native-source-setup".into(),
                actor,
                expected_revision: None,
                action: AgentAction::TaskSubmit(input.clone()),
            };
            let input = setup(services.0.clone(), publication.clone(), worker, input).await;
            wire.replies.lock().unwrap().front_mut().unwrap()["tool_calls"][0]["function"]
                ["arguments"] = serde_json::to_string(&input).unwrap().into();
            resume.notify_one();
        }
        wait_admitted_graph_terminal(services, &packet).await;
        let native_run = services.agent_runtime().get(packet.agent_id()).unwrap();
        assert_eq!(
            native_run.backend,
            crate::agent_run_handle::AgentBackendKind::InProcess
        );
        let native_graph = services
            .graph_state_store()
            .load(packet.graph_id())
            .unwrap();
        if native_run.status != harness_contract::agent::AgentStatus::Completed
            || native_graph.node_statuses.get(packet.node_id())
                != Some(&ExecutionNodeStatus::Completed)
        {
            let program = services
                .agent_action_service()
                .project(&root_actor.program_id);
            let exit = crate::agent_runtime::confirmed_run_exit(services.event_store(), &packet);
            let lifecycle = services
                .event_store()
                .list_stream(&format!("agent:{}", packet.agent_id()));
            eprintln!(
                "R1_NATIVE_SOURCE_FAILURE {}",
                serde_json::json!({
                    "run":native_run,
                    "graph_id":native_graph.id,
                    "graph_revision":native_graph.revision,
                    "node_status":native_graph.node_statuses.get(packet.node_id()),
                    "node_result":native_graph.node_results.get(packet.node_id()),
                    "recovery_cursor":native_graph.recovery_cursor,
                    "task":program.as_ref().ok().and_then(|program| program.tasks.get(&task_ref)),
                    "program_error":program.as_ref().err().map(ToString::to_string),
                    "authenticated_exit":format!("{exit:?}"),
                    "agent_lifecycle":format!("{lifecycle:?}"),
                })
            );
        }
        assert_eq!(
        native_run.status,
        harness_contract::agent::AgentStatus::Completed,
        "native source requires original successful execution: failure={:?}; graph_revision={}; node_status={:?}; node_result={:?}",
        native_run.failure,
        native_graph.revision,
        native_graph.node_statuses.get(packet.node_id()),
        native_graph.node_results.get(packet.node_id()),
    );
        assert_eq!(
            native_graph.node_statuses[packet.node_id()],
            ExecutionNodeStatus::Completed,
            "native source graph result={:?}",
            native_graph.node_results.get(packet.node_id()),
        );
        let native_index_source = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt,
        );
        let native_index = services
            .event_store()
            .list_stream(&native_index_source)
            .unwrap();
        let native_session = services
            .event_store()
            .list_stream(&format!("session:{}", root_actor.session_id))
            .unwrap();
        let mut native_completed_calls = std::collections::BTreeSet::new();
        for receipt in native_index
            .iter()
            .filter(|event| event.kind == "execution.agent_tool.receipt")
        {
            let outcome = &receipt.payload["outcome"];
            let call = outcome["tool_use_id"].as_str().unwrap();
            assert_eq!(outcome["status"], "executed");
            let key = receipt.payload["idempotency_key"].as_str().unwrap();
            let phase = if receipt.payload["effect_kind"] == "read" {
                "read-receipt"
            } else {
                "receipt"
            };
            let primary = services
                .event_store()
                .event_by_idempotency_key(
                    &format!("execution-effect:{key}"),
                    &format!("{key}:{phase}"),
                )
                .unwrap()
                .unwrap();
            let delegated_parent = &primary.payload["replay"]["parent_execution"];
            let delegated = !delegated_parent.is_null();
            let terminal = native_session.iter().find(|event| {
                event.kind == "tool.invocation.completed" && event.payload["tool_call_id"] == call
            });
            if delegated {
                let attempt = receipt
                    .refs
                    .iter()
                    .find(|reference| reference.kind == "agent_attempt")
                    .and_then(|reference| reference.id.parse::<u64>().ok())
                    .expect("a delegated receipt carries the attempt of its owner");
                assert_eq!(
                    receipt.stream_id,
                    format!(
                        "execution-agent-receipts:{}:{}:{attempt}",
                        delegated_parent["execution_id"].as_str().unwrap(),
                        delegated_parent["node_id"].as_str().unwrap(),
                    ),
                    "the per-attempt index must name the delegated owner of the receipt"
                );
                assert_eq!(
                    delegated_parent["execution_id"].as_str().unwrap(),
                    packet.graph_id(),
                    "the delegated owner is this agent task's graph"
                );
                assert_eq!(
                    delegated_parent["node_id"].as_str().unwrap(),
                    packet.node_id(),
                    "the delegated owner is this agent task's node"
                );
                assert_eq!(
                    attempt,
                    u64::from(packet.attempt),
                    "the delegated owner is this attempt"
                );
            } else {
                let terminal =
                    terminal.expect("a root receipt requires its original Session callback");
                let plan_id = terminal.payload["governed_plan_id"].as_str().unwrap();
                assert!(native_session.iter().any(|event| {
                    event.kind == "tool.execution_plan.created"
                        && event.payload["plan_id"] == plan_id
                        && event.payload["tasks"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|task| task["tool_call_id"] == call)
                }));
            }
            assert_eq!(primary.schema_version, 1);
            assert_eq!(primary.transaction_id, receipt.transaction_id);
            assert_eq!(primary.commit_cursor, receipt.commit_cursor);
            assert_eq!(primary.payload["outcome"], receipt.payload["outcome"]);
            assert!(
                primary.commit_cursor
                    <= terminal.map_or(u64::MAX, |terminal| terminal.commit_cursor)
            );
            if !delegated {
                let owner = &primary.payload["root_effect_owner"];
                let expected_stream = format!(
                    "execution-agent-receipts:{}:{}:{}",
                    owner["binding"]["root_execution_id"]
                        .as_str()
                        .unwrap_or_default(),
                    owner["binding"]["node_id"].as_str().unwrap_or_default(),
                    owner["attempt"].as_u64().unwrap_or_default(),
                );
                assert_eq!(
                    receipt.stream_id, expected_stream,
                    "the per-attempt receipt index must name the real producer of the receipt"
                );
            }
            if call.starts_with("native-") {
                let terminal =
                    terminal.expect("scripted Native call requires its original Session callback");
                assert!(native_session.iter().any(|event| {
                    event.kind == "tool.execution_plan.created"
                        && event.payload["plan_id"] == terminal.payload["governed_plan_id"]
                        && event.payload["tasks"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .any(|task| task["tool_call_id"] == call)
                }));
                native_completed_calls.insert(call.to_string());
            }
        }
        let mut expected_native_calls = std::collections::BTreeSet::from([
            "native-claim".to_string(),
            "native-preview-read".to_string(),
            "native-full-read".to_string(),
            "native-publication-small".to_string(),
            "native-submit".to_string(),
        ]);
        // A commit_report case scripts one extra model-driven artifact_commit
        // (fixture reply queue, native-first-claim-artifact); its receipt is
        // model-started, so it legitimately joins the completed-call set.
        #[cfg(test)]
        if NATIVE_TASK_CASE
            .try_with(|case| case.commit_report)
            .unwrap_or(false)
        {
            expected_native_calls.insert("native-first-claim-artifact".to_string());
        }
        assert_eq!(native_completed_calls, expected_native_calls,
        "native receipt completeness: packet={}:{}:{}; host_calls={:?}; session_calls={:?}; wire_requests={}",
        packet.graph_id(), packet.node_id(), packet.attempt,
        host.calls.lock().unwrap().iter()
            .filter(|request| request.tool_use_id.starts_with("native-"))
            .map(|request| (&request.tool_use_id, &request.parent_execution, request.parent_execution_attempt, &request.idempotency_key))
            .collect::<Vec<_>>(),
        native_session.iter().filter(|event| event.payload["tool_call_id"].as_str().is_some_and(|call| call.starts_with("native-")))
            .map(|event| (&event.kind, &event.payload["tool_call_id"], event.commit_cursor, &event.payload["governed_plan_id"]))
            .collect::<Vec<_>>(),
        wire.requests.lock().unwrap().len(),
    );
        let program = services
            .agent_action_service()
            .project(&root_actor.program_id)
            .unwrap();
        let automatic_review_packet = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        async {
            loop {
                for run in services.agent_runtime().list() {
                    let Ok(review_graph) = services.graph_state_store().load(&run.graph_id)
                        else { continue };
                    for review_node in &review_graph.nodes {
                        if review_node.kind != ExecutionNodeKind::AgentTask {
                            continue;
                        }
                        let Ok(review_packet) =
                            serde_json::from_str::<AgentTaskPacket>(&review_node.payload_ref)
                            else { continue };
                        if matches!(review_packet.agentic_binding.as_ref().map(|bound| &bound.focus),
                            Some(AgenticExecutionFocus::TaskReview { task_ref: bound })
                                if bound == &task_ref)
                        {
                            return review_packet;
                        }
                    }
                }
                tokio::task::yield_now().await;
            }
        },
    ).await.unwrap_or_else(|error| {
        own832_diagnostics::capture(services, &root_actor.program_id, &task_ref,
            "automatic_review_packet_timeout", Some(&packet));
        panic!("capture the actual automatic independent review packet: {error}; see OWN832 snapshot");
    });
        let automatic_member = automatic_review_packet.agentic_binding.as_ref().unwrap();
        assert_eq!(automatic_member.agent_id, independent_reviewer);
        assert_eq!(automatic_member.team_id, review_team.changed_refs[0]);
        assert_ne!(
            automatic_member.agent_id,
            packet.agentic_binding.as_ref().unwrap().agent_id
        );
        let review_binding = automatic_review_packet.binding.as_ref().unwrap();
        assert!(matches!(
            &review_binding.executor,
            AgentExecutorPolicy::ProcessJsonl { .. }
        ));
        assert_ne!(automatic_review_packet.agent_id(), packet.agent_id());
        assert_ne!(automatic_review_packet.graph_id(), packet.graph_id());
        assert_eq!(
            automatic_review_packet.permission_ceiling,
            PermissionMode::ReadOnly
        );
        assert_eq!(program.permission_ceiling, PermissionMode::ReadOnly);
        assert_eq!(program.resource_scopes, root_actor.resource_scopes);
        assert_eq!(program.resource_scopes, vec!["workspace:.".to_string()]);
        assert!(automatic_review_packet
            .resource_scopes
            .contains(&"workspace:.".to_string()));
        assert!(review_binding
            .effective_capabilities
            .contains(&AgentCapability::Read));
        let resolver = services.path_identity_resolver();
        assert!(crate::agent_in_process_worker::resource_path_is_authorized(
            resolver,
            ".",
            &automatic_review_packet.resource_scopes,
            false,
        ));
        let policy = services
            .session_execution_policy(automatic_review_packet.session_id())
            .unwrap();
        assert_eq!(automatic_review_packet.policy_revision, policy.revision);
        println!(
            "R1_734_ACTUAL_AUTOMATIC_REVIEW {}",
            serde_json::json!({
                "packet": &automatic_review_packet,
                "definition_grant": review_binding,
                "program_scopes": &program.resource_scopes,
                "session_policy": policy,
                "whole_workspace_read_authorized": true,
                "capture": "new candidate execution; not the missing original packet",
            })
        );
        #[cfg(test)]
        {
            services
                .agent_runtime()
                .validate_process_fixture_boundary(&automatic_review_packet)
                .expect(
                    "the actual compiled independent Process review must pass the original guard",
                );
            let default_scopes = root("734-default-empty-grant", publication.action.clone())
                .actor
                .resource_scopes;
            assert!(default_scopes.is_empty());
            for scopes in [default_scopes, vec!["read:review-effect.txt".into()]] {
                let mut denied = automatic_review_packet.clone();
                denied.resource_scopes = scopes;
                assert!(
                    !crate::agent_in_process_worker::resource_path_is_authorized(
                        resolver,
                        ".",
                        &denied.resource_scopes,
                        false,
                    )
                );
                let error = services
                    .agent_runtime()
                    .validate_process_fixture_boundary(&denied)
                    .expect_err("default empty and narrow grants must still refuse a whole mount");
                assert_eq!(
                    error,
                    "physical_grant: Process whole workspace mount requires whole-read grant"
                );
            }
        }
        let submitted = &program.tasks[&task_ref];
        assert_eq!(
            submitted.status,
            crate::AgenticTaskStatus::Submitted,
            "{submitted:?}"
        );
        assert_eq!(
            submitted.claimant.as_deref(),
            Some(member.changed_refs[0].as_str())
        );
        assert_eq!(
            submitted.claim_execution_id.as_deref(),
            Some(packet.graph_id())
        );
        assert_eq!(submitted.claim_generation, u64::from(packet.attempt));
        assert_eq!(submitted.artifact_refs.len(), 1);
        let mut result_refs = submitted.artifact_refs.clone();
        let mut result_contents = vec![program.artifacts[&result_refs[0]].content_ref.clone()];
        let events = services
            .event_store()
            .list_stream(&format!("session:{}", packet.session_id()))
            .unwrap();
        {
            let material = events
                .iter()
                .filter(|event| event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
                .filter_map(|event| {
                    crate::context_evidence::decode_read_delivery(&event.payload).ok()
                })
                .find(|material| {
                    material.tool_invocation_id == "native-full-read"
                        || material.tool_invocation_id == "native-rework-full-read"
                })
                .expect("the original InProcess backend must confirm the real Native Agent read");
            let preview = events.iter().filter(|event|event.kind == crate::context_evidence::NATIVE_DELIVERY_KIND)
            .filter_map(|event|crate::context_evidence::decode_read_delivery(&event.payload).ok())
            .find(|material|material.tool_invocation_id == "native-preview-read"
                || material.tool_invocation_id == "native-rework-preview-read")
            .expect("the separate partial Native read must retain its own delivery, not borrow the full read");
            assert_eq!(preview.recipient, material.recipient);
            assert_eq!(
                preview.acquisition.body.source_sha256,
                material.acquisition.body.source_sha256
            );
            assert_eq!(
                preview.acquisition.body.source_bytes,
                material.acquisition.body.source_bytes
            );
            assert_eq!(
                preview.grade,
                crate::context_evidence::ReadDeliveryGrade::ProviderModelObserved
            );
            assert!(matches!(
                preview.delivered_extent,
                Some(crate::context_evidence::ReadExtent::FileLines {
                    start_line: 1,
                    line_count: 1,
                    total_lines: 3,
                    ..
                })
            ));
            let crate::context_evidence::ReadDeliveryRecipient::AgentTask { identity } =
                &material.recipient
            else {
                panic!("Native Task coverage cannot be substituted with a Root Host read");
            };
            assert_eq!(identity, &packet.assignment.execution_identity);
            assert_eq!(
                material.grade,
                crate::context_evidence::ReadDeliveryGrade::ProviderModelObserved
            );
            assert_eq!(material.acquisition.graph_id, packet.graph_id());
            assert_eq!(material.acquisition.node_id, packet.node_id());
            assert_eq!(material.acquisition.attempt, packet.attempt);
            assert_eq!(
                material.acquisition.step.run_ref.as_deref(),
                Some(packet.run_id())
            );
            assert!(!material.body_omitted);
            assert!(material.delivered_extent.is_some());
            let primary = services
                .event_store()
                .event_by_idempotency_key(
                    &material.acquisition.primary_stream,
                    &material.acquisition.primary_key,
                )
                .unwrap()
                .unwrap();
            let index = services
                .event_store()
                .event_by_idempotency_key(
                    &material.acquisition.index_stream,
                    &material.acquisition.index_key,
                )
                .unwrap()
                .unwrap();
            assert_eq!(primary.transaction_id, index.transaction_id);
            assert_eq!(primary.event_id, material.acquisition_event_id);
            assert_eq!(index.event_id, material.index_event_id);
            let claim = services.event_store().list_stream(&format!("agentic-program:{}",root_actor.program_id)).unwrap()
            .into_iter().find(|event|serde_json::from_value::<AgentActionEnvelope>(event.payload["envelope"].clone())
                .is_ok_and(|envelope|matches!(&envelope.action, AgentAction::TaskClaim(input)
                    if input.task_ref == task_ref && envelope.actor.execution_id.as_deref() == Some(packet.graph_id()))))
            .expect("Native worker must actually claim through its original action host");
            assert!(claim.commit_cursor < primary.commit_cursor);
            assert_eq!(
                primary.payload[crate::context_evidence::READ_ACQUISITION_FIELD],
                index.payload[crate::context_evidence::READ_ACQUISITION_FIELD]
            );
            assert_native_task_material_wire(services, &packet, &preview, &wire.requests).await;
            assert_native_task_material_wire(services, &packet, &material, &wire.requests).await;
            #[cfg(test)]
            services.assert_read_delivery_actor_matrix_for_test(
                &program,
                &packet,
                "native-full-read",
                false,
            );
            for (call_id, _) in &publications {
                let owner = events
                    .iter()
                    .find(|event| {
                        event.kind == "tool.invocation.completed"
                            && event.payload["tool_call_id"] == *call_id
                    })
                    .expect("actual Native publication lifecycle");
                assert_eq!(owner.payload["session_id"], packet.session_id());
                assert_eq!(owner.payload["tool_name"], "artifact_publish");
                assert_eq!(
                    owner.activity_binding().unwrap().agent_run_id.as_deref(),
                    Some(packet.run_id())
                );
                let full_ref = owner.payload["full_output_ref"].as_str().unwrap();
                let access = services
                    .session_evidence_access(
                        packet.session_id(),
                        full_ref.strip_prefix("tool://").unwrap(),
                    )
                    .await
                    .unwrap()
                    .unwrap();
                let raw = services
                    .artifact_store()
                    .resolve(&access.retrieval_selector)
                    .unwrap();
                let bytes = services
                    .artifact_store()
                    .read(&raw, &raw.visibility_scope, None)
                    .await
                    .unwrap();
                assert!(bytes.len() > 500 && bytes.len() <= 256 * 1024);
                if *call_id == "native-publication-small" {
                    assert!(bytes.len() < 16000);
                    assert_eq!(
                    full_ref
                        .strip_prefix(&format!("tool://tool-raw-{call_id}-"))
                        .unwrap()
                        .len(),
                    16,
                    "legacy injection must exercise ordinary16, never a complete staged64 carrier"
                );
                    assert!(owner
                        .payload
                        .get("output_ref")
                        .is_none_or(serde_json::Value::is_null));
                    assert!(owner.payload["output_preview"].as_str().unwrap().len() < bytes.len());
                    let expected_anchor = serde_json::json!({
                        "version":1, "ref_id":full_ref, "tool_call_id":call_id,
                        "sha256":raw.sha256, "byte_count":raw.bytes, "selector":raw.selector,
                        "visibility_scope":raw.visibility_scope, "media_type":raw.media_type,
                    });
                    #[allow(unused_mut)]
                    let mut encoding: Option<&str> = None;
                    #[cfg(test)]
                    if let R1ProofScenario::ProgramPublication(case) = proof_scenario {
                        encoding = case.encoding;
                    }
                    match encoding {
                        None => {
                            assert_eq!(owner.payload["runtime_raw_content_v1"], expected_anchor)
                        }
                        Some("legacy") => {
                            assert!(owner.payload.get("runtime_raw_content_v1").is_none())
                        }
                        Some(field) => {
                            let mut actual = owner.payload["runtime_raw_content_v1"].clone();
                            assert_ne!(actual[field], expected_anchor[field]);
                            actual[field] = expected_anchor[field].clone();
                            assert_eq!(actual, expected_anchor);
                        }
                    }
                    let request =
                        host.calls
                            .lock()
                            .unwrap()
                            .iter()
                            .find(|request| {
                                request.tool_use_id == *call_id
                                    && request.parent_execution.as_ref().is_some_and(|parent| {
                                        parent.execution_id == packet.graph_id()
                                    })
                            })
                            .cloned()
                            .unwrap();
                    let step = request.step_binding.as_ref().unwrap();
                    assert_eq!(
                        step.snapshot.identity.run_ref.as_deref(),
                        Some(packet.run_id())
                    );
                    assert_eq!(
                        owner.payload["input_hash"],
                        crate::tool_invocation::stable_hash(&request.input)
                    );
                    let key = step.effect_idempotency_key();
                    let primaries = ["read-receipt", "receipt"]
                        .into_iter()
                        .filter_map(|suffix| {
                            services
                                .event_store()
                                .event_by_idempotency_key(
                                    &format!("execution-effect:{key}"),
                                    &format!("{key}:{suffix}"),
                                )
                                .unwrap()
                        })
                        .collect::<Vec<_>>();
                    assert_eq!(primaries.len(), 1, "publication primary must be unique");
                    let primary = &primaries[0];
                    assert_ne!(primary.payload["outcome_truncated"], true);
                    assert_eq!(primary.payload["outcome"]["status"], "executed");
                    assert!(primary.payload["outcome"]["error"].is_null());
                    assert_eq!(
                    primary.payload["outcome"]["output"].as_str().unwrap().as_bytes(),
                    bytes.as_slice(),
                    "the complete primary and writer raw anchor must carry identical original bytes"
                );
                    assert!(primary.commit_cursor < owner.commit_cursor);
                }
                let published: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(published["status"], "published");
                assert_eq!(published["bytes"], selected_bytes.len());
                assert_eq!(published["sha256"], selected_hash);
                let selector = published["content_ref"].as_str().unwrap().to_string();
                result_refs.push(selector.clone());
                result_contents.push(selector);
            }
        }
        #[cfg(test)]
        let registration = if let R1ProofScenario::ProgramPublication(case) = proof_scenario {
            let selector = result_contents[1].clone();
            let before = services
                .event_store()
                .list_stream(&format!("session:{}", root_actor.session_id))
                .unwrap();
            assert!(!before.iter().any(|event| event
                .activity_binding()
                .is_some_and(|binding| binding.agent_run_id.is_none())
                && event.kind == "tool.invocation.completed"
                && event.payload["tool_name"] == "evidence_retrieve"
                && event.payload.to_string().contains(&selector)));
            let logical = commit_existing_result_selector(
                services,
                root_actor,
                "program-small-registration",
                &selector,
                Vec::new(),
            )
            .await;
            let committed = services
                .event_store()
                .list_stream(&format!("agentic-program:{}", root_actor.program_id))
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.payload["envelope"]["action_id"] == "program-small-registration"
                })
                .unwrap();
            assert!(before
                .iter()
                .all(|event| event.commit_cursor < committed.commit_cursor));
            assert_eq!(
                crate::agentic::review_evidence::review_result_identity(
                    &services
                        .agent_action_service()
                        .project(&root_actor.program_id)
                        .unwrap(),
                    &selector
                )
                .unwrap(),
                logical
            );
            result_refs[1] = if case.logical {
                logical.clone()
            } else {
                selector.clone()
            };
            Some((logical, selector))
        } else {
            None
        };
        #[cfg(test)]
        drop(encoding_guard);
        let (_, root_runtime) =
            root_reads_generic_results(services, host.clone(), &result_contents[0], false, false)
                .await;
        root_review_input_tool(
            &root_runtime,
            "root-publication-large",
            "artifact_publish",
            &serde_json::json!({"source":"file","path":"review-effect.txt","sha256":selected_hash,
            "media_type":format!("text/plain;fixture={}", "多".repeat(20000))})
            .to_string(),
        )
        .await;
        let root_events = services
            .event_store()
            .list_stream(&format!("session:{}", packet.session_id()))
            .unwrap();
        let large_owner = root_events
            .iter()
            .find(|event| {
                event.kind == "tool.invocation.completed"
                    && event.payload["tool_call_id"] == "root-publication-large"
            })
            .unwrap();
        let full_ref = large_owner.payload["full_output_ref"].as_str().unwrap();
        let access = services
            .session_evidence_access(
                packet.session_id(),
                full_ref.strip_prefix("tool://").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let raw = services
            .artifact_store()
            .resolve(&access.retrieval_selector)
            .unwrap();
        let raw_bytes = services
            .artifact_store()
            .read(&raw, &raw.visibility_scope, None)
            .await
            .unwrap();
        assert!(raw_bytes.len() > 16000 && raw_bytes.len() <= 256 * 1024);
        assert!(!large_owner.payload["output_ref"].is_null());
        assert!(
            raw_bytes.len()
                > String::from_utf8(raw_bytes.clone())
                    .unwrap()
                    .chars()
                    .count()
        );
        let publication: serde_json::Value = serde_json::from_slice(&raw_bytes).unwrap();
        assert_eq!(publication["status"], "published");
        let selected = publication["content_ref"].as_str().unwrap().to_string();
        let request = host
            .calls
            .lock()
            .unwrap()
            .iter()
            .find(|request| request.tool_use_id == "root-publication-large")
            .cloned()
            .unwrap();
        let key = request
            .step_binding
            .as_ref()
            .unwrap()
            .effect_idempotency_key();
        let primary = services
            .event_store()
            .event_by_idempotency_key(
                &format!("execution-effect:{key}"),
                &format!("{key}:read-receipt"),
            )
            .unwrap()
            .unwrap();
        assert_eq!(primary.payload["outcome_truncated"], true,
        "the real large publication must require immutable full-output recovery rather than reuse a complete primary");
        result_refs.push(selected.clone());
        result_contents.push(selected);
        let verifier = wait_real_process(services, automatic_review_packet.graph_id()).await;
        assert_eq!(verifier.packet, automatic_review_packet);
        let agentic = verifier
            .packet
            .agentic_binding
            .as_ref()
            .expect("typed automatic independent review binding");
        let reviewer_team = agentic.team_id.clone();
        let program = services
            .agent_action_service()
            .project(&root_actor.program_id)
            .unwrap();
        assert!(program.agent_is_active_in(&agentic.agent_id, &reviewer_team));
        assert_eq!(agentic.task_team_id, team_ref);
        assert!(
            matches!(&agentic.focus, AgenticExecutionFocus::TaskReview { task_ref: bound } if bound == &task_ref)
        );
        if proof_scenario.mechanical_cross_team() {
            let author_team = &packet.agentic_binding.as_ref().unwrap().team_id;
            assert_eq!(author_team, &team_ref);
            assert_ne!(
                &reviewer_team, author_team,
                "cross-Team review must use a different Team from the real Native author"
            );
        }
        for (position, content) in result_contents.iter().enumerate() {
            verifier
                .execute_tool(
                    &format!("native-task-independent-read:{position}"),
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":content}).to_string(),
                )
                .await
                .unwrap();
        }
        let actor = services
            .resolve_agent_action_actor(
                &ExecutionParentBinding {
                    execution_id: verifier.packet.graph_id().into(),
                    node_id: verifier.packet.node_id().into(),
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(actor.team_id.as_deref(), Some(reviewer_team.as_str()));
        let mut seen_review_refs = std::collections::BTreeSet::new();
        let review_evidence_refs: Vec<_> = result_refs
            .iter()
            .map(|reference| {
                reference
                    .strip_prefix("artifact:")
                    .filter(|logical| !logical.starts_with("//"))
                    .map_or_else(
                        || reference.clone(),
                        |logical| format!("artifact://{logical}"),
                    )
            })
            .chain(result_contents.iter().skip(1).cloned())
            .filter(|reference| seen_review_refs.insert(reference.clone()))
            .collect();
        let envelope = AgentActionEnvelope {
            action_id: "native-sources:task-review".into(),
            actor,
            expected_revision: None,
            action: AgentAction::TaskReview(TaskReviewInput {
                operator_requirement: None,
                task_ref: task_ref.clone(),
                decision: TaskReviewDecision::Accept,
                reason:
                    "The actual immutable Native report and selected source were independently read"
                        .into(),
                evidence_refs: review_evidence_refs,
            }),
        };
        let mut probed = false;
        #[cfg(test)]
        if proof_scenario == R1ProofScenario::TaskReviewAdmission {
            let current = services
                .agent_action_service()
                .project(&root_actor.program_id)
                .unwrap();
            assert_eq!(
                current.tasks[&task_ref].status,
                crate::AgenticTaskStatus::Submitted
            );
            let donor = services
                .independent_result_review(
                    &envelope.actor,
                    &current,
                    &current.tasks[&task_ref].artifact_refs,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(donor.verification.input_set.as_ref().unwrap().version, 2);
            assert!(!donor
                .verification
                .input_set
                .as_ref()
                .unwrap()
                .material_events
                .is_empty());
            let goal = services
                .goal_store()
                .get(&format!(
                    "goal:{}",
                    root_actor.root_execution_id.as_ref().unwrap()
                ))
                .unwrap()
                .unwrap();
            let _ = NATIVE_TASK_CASE.try_with(|case| {
                if let Some(objective) = &case.objective {
                    assert_eq!(&goal.objective, objective);
                }
                if let Some(acceptance) = &case.acceptance {
                    assert_eq!(&current.tasks[&task_ref].acceptance, acceptance);
                }
                assert_eq!(
                    current.tasks[&task_ref].criterion_source,
                    case.criterion_source
                );
                assert_eq!(current.tasks[&task_ref].unresolved, case.unresolved);
            });
            let history = services.session_history_reader();
            crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &current,
                Some(&goal),
                &donor.verification,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: history.as_deref(),
                },
            )
            .unwrap();
            let probe = NATIVE_TASK_ASYNC_PROBE.with(|slot| slot.borrow_mut().take().unwrap());
            NATIVE_REWORK_WIRE
                .scope(
                    NativeReworkWire {
                        packet: packet.clone(),
                        replies: wire.replies.clone(),
                        requests: wire.requests.clone(),
                        pause: wire.pause.clone(),
                    },
                    probe(services.0.clone(), envelope.clone(), donor),
                )
                .await;
            probed = true;
        }
        if !probed {
            let reviewed = services.submit_agent_action(&envelope).await.unwrap();
            assert_eq!(
                reviewed.status,
                harness_contract::agent_action::AgentActionStatus::Applied,
                "{reviewed:?}"
            );
        }
        if probed {
            let current = services
                .agent_action_service()
                .project(&root_actor.program_id)
                .unwrap();
            if current.tasks[&task_ref].status == crate::AgenticTaskStatus::Submitted {
                let retired = services
                    .submit_agent_action(&AgentActionEnvelope {
                        action_id: "native-negative-probe-retire-original-task".into(),
                        actor: root_actor.clone(),
                        expected_revision: None,
                        action: AgentAction::TaskWithdraw(
                            harness_contract::agent_action::TaskWithdrawInput {
                                task_ref: task_ref.clone(),
                                reason_ref: result_contents[0].clone(),
                                evidence_refs: result_contents.clone(),
                            },
                        ),
                    })
                    .await
                    .unwrap();
                assert_eq!(
                    retired.status,
                    harness_contract::agent_action::AgentActionStatus::Applied
                );
            }
        }
        if !services
            .graph_state_store()
            .load(verifier.packet.graph_id())
            .unwrap()
            .node_statuses
            .values()
            .all(|status| status.is_terminal())
        {
            verifier.finish().await;
        }
        wait_process_graph_terminal(services, &verifier).await;
        verifier.wait_exit().await;
        assert!(confirmed_run_exit_with_retry(services.event_store(), &verifier.packet).is_some());
        assert!(!services
            .agent_runtime()
            .running_agentic_members(&root_actor.program_id)
            .contains(&independent_reviewer));
        if !probed {
            assert_eq!(
                services
                    .agent_action_service()
                    .project(&root_actor.program_id)
                    .unwrap()
                    .tasks[&task_ref]
                    .status,
                crate::AgenticTaskStatus::Accepted
            );
        }
        assert!(host.calls.lock().unwrap().iter().any(|request| request
            .parent_execution
            .as_ref()
            .is_some_and(|parent| parent.execution_id == packet.graph_id())
            && request.tool_name == "read_file"));
        NativeSourcePair {
            packet,
            result_refs,
            result_contents,
            _wire: wire,
            #[cfg(test)]
            registration,
        }
    })
}

#[cfg(test)]
pub(crate) async fn native_raw_owner_fixture_for_test() -> (
    tempfile::TempDir,
    Arc<RuntimeServices>,
    crate::DurableRuntimeEvent,
) {
    let workspace = tempfile::tempdir().unwrap();
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services.clone(),
        host,
        false,
        false,
        DelegatedEffectCase::Review,
        false,
        PhysicalDrainTiming::AfterGoal,
        R1ProofScenario::ReadDelivery(RealProcessProofCase::NativeSources),
    ))
    .await;
    let owner = services
        .event_store()
        .list_stream("session:session-dispatch")
        .unwrap()
        .into_iter()
        .find(|event| {
            event.kind == "tool.invocation.completed"
                && event.payload["tool_call_id"] == "native-publication-small"
        })
        .unwrap();
    (workspace, services, owner)
}

#[cfg(test)]
pub(super) async fn exercise_program_publication_case(case: ProgramPublicationCase) {
    if case.encoding.is_none() && case.probe != ProgramPublicationProbe::Publisher {
        exercise_native_sources_with_raw_origin(
            case.probe == ProgramPublicationProbe::AnchorLoss,
            Some(case),
        )
        .await;
        return;
    }
    let workspace = tempfile::tempdir().unwrap();
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services.clone(),
        host,
        false,
        false,
        DelegatedEffectCase::Review,
        false,
        PhysicalDrainTiming::AfterGoal,
        R1ProofScenario::ProgramPublication(case),
    ))
    .await;
    services.shutdown_maintenance().await;
}

#[cfg(test)]
pub(super) async fn exercise_native_sources_with_cold_readers() {
    exercise_native_sources_with_raw_origin(false, None).await;
}

#[cfg(test)]
pub(super) async fn exercise_native_sources_with_changed_anchor_source() {
    exercise_native_sources_with_raw_origin(true, None).await;
}

#[cfg(test)]
async fn exercise_native_sources_with_raw_origin(
    change_source_before_b: bool,
    case: Option<ProgramPublicationCase>,
) {
    let workspace = tempfile::tempdir().unwrap();
    let blobs = workspace.path().join("immutable-fixture-blobs");
    let repository = Arc::new(crate::EphemeralArtifactRepository::default());
    let artifacts = Arc::new(
        crate::ArtifactStore::new(
            &blobs,
            repository.clone(),
            crate::ArtifactStoreConfig::default(),
        )
        .unwrap(),
    );
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .artifact_store(artifacts.clone())
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    let session_store = Arc::new(crate::test_support::session_store());
    session_store
        .create_session(&session::SessionRecord {
            session_id: "session-dispatch".into(),
            platform: "test".into(),
            chat_id: "native-cold-source".into(),
            user_id: None,
            model: None,
            created_at: "2026-10-17T00:00:00Z".into(),
            last_activity: "2026-10-17T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .unwrap();
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(session_store.clone());
    services
        .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports.clone())
        .unwrap();
    Box::pin(exercise_reviewed_goal_proof_scenario(
        services.clone(),
        host.clone(),
        false,
        false,
        DelegatedEffectCase::Review,
        false,
        PhysicalDrainTiming::AfterGoal,
        case.map_or(
            R1ProofScenario::ReadDelivery(RealProcessProofCase::NativeSources),
            R1ProofScenario::ProgramPublication,
        ),
    ))
    .await;
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    let store = services.event_store().clone();
    let aggregates = services.task_aggregate_service().clone();
    let query = services.session_query_port().unwrap();
    let history = query
        .history_reader()
        .expect("cold historical reader must be installed");
    let events = store.list_stream("session:session-dispatch").unwrap();
    let mut publication_bodies = Vec::new();
    for call_id in ["native-publication-small", "root-publication-large"] {
        let event = events
            .iter()
            .find(|event| {
                event.kind == "tool.invocation.completed"
                    && event.payload["tool_call_id"] == call_id
            })
            .unwrap();
        let reference = event.payload["full_output_ref"].as_str().unwrap();
        let access = services
            .session_evidence_access(
                "session-dispatch",
                reference.strip_prefix("tool://").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let raw = artifacts.resolve(&access.retrieval_selector).unwrap();
        let bytes = artifacts
            .read(&raw, "session:session-dispatch", None)
            .await
            .unwrap();
        let duplicate = artifacts
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: raw.media_type.clone(),
                    visibility_scope: raw.visibility_scope.clone(),
                    expected_bytes: Some(raw.bytes),
                    original_name: None,
                },
                &bytes,
            )
            .await
            .unwrap();
        assert_ne!(duplicate.selector, raw.selector);
        assert_eq!(duplicate.sha256, raw.sha256);
        publication_bodies.push((raw, bytes, event.clone()));
    }
    for run in services.agent_runtime().list() {
        if !run.status.is_terminal() {
            let graph = services.graph_state_store().load(&run.graph_id).unwrap();
            let packet: AgentTaskPacket = serde_json::from_str(
                &graph
                    .nodes
                    .iter()
                    .find(|node| node.kind == ExecutionNodeKind::AgentTask)
                    .unwrap()
                    .payload_ref,
            )
            .unwrap();
            services
                .agent_runtime()
                .command(harness_contract::agent::AgentCommandRequest {
                    command_id: format!("native-cold-drain:{}", run.run_id),
                    agent_id: run.agent_id,
                    expected_revision: run.revision,
                    command: harness_contract::agent::AgentCommand::Cancel,
                    input: None,
                })
                .await;
            wait_admitted_graph_terminal(&services, &packet).await;
        }
    }
    services.shutdown_maintenance().await;
    drop(services);
    assert!(
        host.services.get().unwrap().upgrade().is_none(),
        "cold consumers must not retain hot RuntimeServices through a fixture handle"
    );
    drop(host);
    drop(artifacts);
    for name in ["review-effect.txt", "unrelated-review.txt"] {
        let path = workspace.path().join(name);
        if path.exists() {
            std::fs::remove_file(path).unwrap();
        }
    }
    let reopened = Arc::new(
        crate::ArtifactStore::new(&blobs, repository, crate::ArtifactStoreConfig::default())
            .unwrap(),
    );
    for (raw, bytes, owner) in &publication_bodies {
        let canonical = owner.payload["full_output_ref"]
            .as_str()
            .unwrap()
            .strip_prefix("tool://")
            .unwrap();
        let mut from_sequence = 0;
        let mut original = None;
        loop {
            let page = history
                .domain_events_by_kind_blocking(
                    "session-dispatch",
                    "evidence.raw.persisted",
                    from_sequence,
                    128,
                )
                .unwrap();
            if page.is_empty() {
                break;
            }
            for event in page {
                assert!(event.sequence >= from_sequence);
                from_sequence = event.sequence.checked_add(1).unwrap();
                if event.payload["evidence_id"] == canonical {
                    assert_eq!(event.payload["tool_call_id"], owner.payload["tool_call_id"]);
                    assert_eq!(event.payload["tool_name"], "artifact_publish");
                    assert_eq!(event.payload["session_id"], "session-dispatch");
                    let access = crate::context_evidence::raw::access_from_persisted_payload(
                        canonical,
                        &event.payload,
                    )
                    .unwrap();
                    assert_eq!(access.retrieval_selector, raw.selector);
                    assert!(
                        original.replace(access).is_none(),
                        "exact canonical raw mapping must be unique"
                    );
                }
            }
        }
        assert!(
            original.is_some(),
            "the cold original Session reader must resolve the exact raw locator"
        );

        assert_eq!(
            reopened
                .read_session_content_by_digest_blocking("session-dispatch", &raw.sha256, raw.bytes)
                .unwrap(),
            *bytes
        );
        assert!(reopened
            .read_session_content_by_digest_blocking("foreign-session", &raw.sha256, raw.bytes)
            .is_err());
        assert!(reopened
            .read_session_content_by_digest_blocking(
                "session-dispatch",
                &raw.sha256,
                raw.bytes.saturating_add(1)
            )
            .is_err());
    }
    let cold_goal =
        crate::execution_core::GoalStore::new(store.clone()).with_artifact_store(reopened.clone());
    let cold_actions =
        crate::AgentActionService::new(store.clone()).with_artifact_store(reopened.clone());
    let cold_commits = crate::execution_core::graph::ExecutionCommitService::new(store.clone())
        .with_artifact_store(reopened.clone());
    assert_eq!(cold_goal.get(&goal.id).unwrap().unwrap(), goal);
    let cold = RuntimeServices::test_builder(workspace.path().join("home"), workspace.path())
        .runtime_event_store(store.clone())
        .artifact_store(reopened.clone())
        .task_aggregate_service(aggregates)
        .build()
        .unwrap();
    let empty_store = Arc::new(crate::test_support::session_store());
    let empty_ports = crate::session_runtime_port::TestSessionPortAdapter::new(empty_store);
    cold.install_session_ports(
        empty_ports.clone(),
        empty_ports.clone(),
        empty_ports.clone(),
        empty_ports,
    )
    .unwrap();
    let binding = goal.execution_binding.as_ref().unwrap();
    let program = cold
        .agent_action_service()
        .project(&binding.agentic_program_id)
        .unwrap();
    assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
    assert_eq!(
        cold_actions.project(&binding.agentic_program_id).unwrap(),
        program
    );
    let readers = crate::agentic::review_evidence::ResultContentReaders {
        process_inputs: None,
        artifacts: Some(reopened.as_ref()),
        history: Some(history.as_ref()),
    };
    assert!(crate::agentic::review_evidence::committed_proof_sources(
        &store, &goal, crate::agentic::review_evidence::ResultContentReaders::default(),
    ).is_err(), "cold semantic consumers must not downgrade missing immutable-reader capability to no effects");
    let without_session = crate::agentic::review_evidence::committed_proof_sources(
        &store,
        &goal,
        crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(reopened.as_ref()),
            history: None,
        },
    )
    .expect("the genuine Native writer anchor must survive missing Session mappings");
    assert!(without_session
        .iter()
        .any(|source| source.stream_id == "session:session-dispatch"));
    let mut actor = root(
        "cold-direct-result",
        AgentAction::TeamCreate(
            serde_json::from_value(serde_json::json!({"name":"unused", "mission":"unused"}))
                .unwrap(),
        ),
    )
    .actor;
    actor.root_execution_id = Some(binding.root_execution_id.clone());
    for (raw, _, owner) in &publication_bodies {
        let reference = owner.payload["full_output_ref"].as_str().unwrap();
        assert!(cold
            .session_evidence_access(
                "session-dispatch",
                reference.strip_prefix("tool://").unwrap()
            )
            .await
            .unwrap()
            .is_none());
        let resolved = cold
            .raw_origin_content_for_test(&actor, &program, reference)
            .await
            .unwrap();
        assert_eq!(resolved, *raw);
    }
    let sources =
        crate::agentic::review_evidence::committed_proof_sources(&store, &goal, readers).unwrap();
    assert!(sources
        .iter()
        .any(|source| source.stream_id == "session:session-dispatch"));
    assert_native_sources_final_b(&cold, &cold_commits, &goal, change_source_before_b);
    assert_eq!(cold.goal_store().get(&goal.id).unwrap().unwrap(), goal);
    cold.shutdown_maintenance().await;
}

pub(crate) fn assert_native_sources_final_b(
    services: &Arc<RuntimeServices>,
    commits: &crate::execution_core::graph::ExecutionCommitService,
    goal: &harness_contract::goal::GoalContract,
    change_source_before_b: bool,
) {
    assert_native_sources_b_case(services, commits, goal, change_source_before_b, None);
}

#[cfg(test)]
pub(crate) fn assert_native_sources_b_rejected(
    services: &Arc<RuntimeServices>,
    goal: &harness_contract::goal::GoalContract,
    expected: &str,
) {
    assert_native_sources_b_case(
        services,
        services.commit_service(),
        goal,
        false,
        Some(expected),
    );
}

fn assert_native_sources_b_case(
    services: &Arc<RuntimeServices>,
    commits: &crate::execution_core::graph::ExecutionCommitService,
    goal: &harness_contract::goal::GoalContract,
    change_source_before_b: bool,
    expected_error: Option<&str>,
) {
    let binding = goal.execution_binding.as_ref().unwrap();
    let program = services
        .agent_action_service()
        .project(&binding.agentic_program_id)
        .unwrap();
    let verdict = program
        .objective_verdict
        .as_ref()
        .expect("the real A owner must have committed");
    let mut graph = services
        .graph_state_store()
        .load(&binding.root_execution_id)
        .unwrap();
    let node_id = graph
        .nodes
        .iter()
        .find(|node| node.kind == ExecutionNodeKind::InlineModel)
        .unwrap()
        .id
        .clone();
    graph = bind_review_fixture_root(services, commits, graph, &node_id);
    let result = program
        .artifacts
        .values()
        .find(|artifact| artifact.kind == "report")
        .unwrap();
    let mut terminal = crate::SessionTerminalInput {
        terminal_id: "turn-terminal:native-cold-b".into(),
        message_id: "assistant:native-cold-b".into(),
        session_id: binding.session_id.clone(),
        execution_id: Some(graph.id.clone()),
        turn_id: Some(binding.turn_id.clone()),
        request_id: Some("native-cold-b".into()),
        session_generation: Some(1),
        input_sequence: Some(1),
        input_claim_owner: Some("r1-b-only-test-owner".into()),
        input_claim_token: Some("r1-b-only-test-token".into()),
        input_claim_revision: Some(1),
        controlled_recovery_claim_fingerprints: vec![],
        payload_ref: result.content_ref.clone(),
    };
    r1_prepare_fixture_terminal_payload(services, &mut terminal, "native-cold-b:terminal");
    let event = crate::RuntimeTransactionEventInput {
        event: crate::RuntimeEventInput {
            stream_id: "session-terminal:native-cold-b".into(),
            scope: crate::RuntimeEventScope::SessionInput,
            kind: "runtime.session.terminal_requested".into(),
            status: Some("pending_delivery".into()),
            actor: Some("r1_b_commit_fixture".into()),
            refs: vec![
                crate::RuntimeEventRef {
                    kind: "execution_graph".into(),
                    id: graph.id.clone(),
                },
                crate::RuntimeEventRef {
                    kind: "session".into(),
                    id: binding.session_id.clone(),
                },
            ],
            payload: serde_json::to_value(&terminal).unwrap(),
        },
        idempotency_key: Some("native-cold-b:terminal".into()),
        schema_version: 1,
    };
    assert!(event.event.payload.get("objective_verdict").is_none());
    if change_source_before_b {
        let owner = services
            .event_store()
            .list_stream(&format!("session:{}", binding.session_id))
            .unwrap()
            .into_iter()
            .find(|owner| {
                owner.kind == "tool.invocation.completed"
                    && owner.payload["tool_call_id"] == "native-publication-small"
            })
            .unwrap();
        let raw = &owner.payload["runtime_raw_content_v1"];
        let artifact = services
            .artifact_store()
            .resolve(raw["selector"].as_str().unwrap())
            .unwrap();
        let evidence_id = raw["ref_id"]
            .as_str()
            .unwrap()
            .strip_prefix("tool://")
            .unwrap();
        services
            .artifact_store()
            .unpin(
                &artifact,
                &format!("evidence:{}:{evidence_id}", binding.session_id),
            )
            .unwrap();
        services
            .artifact_store()
            .delete(&artifact, &artifact.visibility_scope)
            .unwrap();
    }
    let before = services.graph_state_store().load(&graph.id).unwrap();
    let completed = harness_contract::execution_graph::ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref: Some(result.content_ref.clone()),
        wait_class: None,
        summary: Some("exact A terminal".into()),
        evidence_refs: vec![],
        failure: None,
        usage: Default::default(),
        finished_at_ms: crate::tool_invocation_now_ms(),
    };
    for (field, value) in [
        ("session_id", "foreign-session"),
        ("turn_id", "foreign-turn"),
        ("execution_id", "foreign-root"),
    ] {
        let mut wrong_binding = event.clone();
        wrong_binding.event.payload[field] = serde_json::json!(value);
        let error = commits
            .transition_node(
                &graph,
                &node_id,
                ExecutionNodeStatus::Completed,
                Some(completed.clone()),
                vec![wrong_binding],
            )
            .err()
            .expect("foreign terminal binding must fail");
        assert!(
            error
                .to_string()
                .contains("Session terminal does not match its Graph, Session and Turn"),
            "{error}"
        );
        assert!(services
            .event_store()
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .is_none());
        assert_eq!(
            services
                .event_store()
                .stream_revision("session-terminal:native-cold-b")
                .unwrap(),
            0
        );
        assert_eq!(
            services.graph_state_store().load(&graph.id).unwrap(),
            before
        );
    }
    let result = commits.transition_node(
        &graph,
        &node_id,
        ExecutionNodeStatus::Completed,
        Some(harness_contract::execution_graph::ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some(result.content_ref.clone()),
            wait_class: None,
            summary: Some(
                "Native Task sources consumed by the original cold B commit owner".into(),
            ),
            evidence_refs: vec![],
            failure: None,
            usage: Default::default(),
            finished_at_ms: crate::tool_invocation_now_ms(),
        }),
        vec![event],
    );
    if change_source_before_b || expected_error.is_some() {
        let error = result
            .err()
            .expect("actual B must reject invalid proof or source");
        if let Some(expected) = expected_error {
            assert!(
                error.to_string().contains(expected),
                "expected {expected}, got {error}"
            );
        }
        assert_eq!(
            services.graph_state_store().load(&graph.id).unwrap(),
            before
        );
        assert!(services
            .event_store()
            .list_stream("session-terminal:native-cold-b")
            .unwrap()
            .is_empty());
        assert!(services
            .event_store()
            .session_terminal(&terminal.terminal_id)
            .unwrap()
            .is_none());
        return;
    }
    let receipt =
        result.expect("cold B must independently consume original immutable content and exact A");
    let published = services
        .event_store()
        .list_stream("session-terminal:native-cold-b")
        .unwrap();
    assert_eq!(published.len(), 1);
    let a_event = services
        .event_store()
        .event_by_idempotency_key(
            &format!("agentic-program:{}", program.program_id),
            &format!("program-conclusion:{}", verdict.authority_revision),
        )
        .unwrap()
        .or_else(|| {
            services
                .event_store()
                .event_by_idempotency_key(
                    &format!("agentic-program:{}", program.program_id),
                    &format!("agentic-objective-verdict:{}", verdict.terminal_fence),
                )
                .unwrap()
        })
        .unwrap();
    assert_eq!(
        published[0].payload["objective_verdict"]["a_commit_cursor"],
        a_event.commit_cursor
    );
    assert_eq!(
        published[0].payload["objective_verdict"]["goal_spec_revision"],
        goal.spec_revision
    );
    assert_eq!(
        published[0].payload["objective_verdict"]["goal_spec_digest"],
        goal.spec_digest
    );
    let expected_digests = if goal.completion == harness_contract::goal::GoalCompletion::Satisfied {
        crate::agentic::review_evidence::required_review_conditions(goal)
            .iter()
            .map(|condition| {
                goal.reviews
                    .iter()
                    .rev()
                    .find(|review| &review.criterion_ref == condition)
                    .expect("every required condition must retain its latest review")
            })
            .map(|review| {
                review
                    .verification
                    .as_ref()
                    .and_then(|proof| proof.input_set.as_ref())
                    .expect("every required review must retain its proof input set")
                    .semantic_digest
                    .clone()
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let published_digests: Vec<String> = serde_json::from_value(
        published[0].payload["objective_verdict"]["proof_input_digests"].clone(),
    )
    .unwrap();
    assert!(
        published_digests.windows(2).all(|pair| pair[0] < pair[1]),
        "B must publish strictly ordered digests without duplicates: {published_digests:?}"
    );
    assert_eq!(published_digests, expected_digests);
    assert_eq!(
        published[0].payload["objective_verdict"]["program_stream_revision"],
        program.revision
    );
    for (stream_id, expected_revision) in [
        (
            format!("agentic-program:{}", program.program_id),
            program.revision,
        ),
        (
            format!("goal:{}", goal.id),
            services
                .goal_store()
                .projection(&goal.id)
                .unwrap()
                .unwrap()
                .stream_revision,
        ),
    ] {
        assert!(receipt
            .transaction
            .stream_revisions
            .iter()
            .any(|source| source.stream_id == stream_id
                && source.expected_revision == expected_revision));
    }
    assert_eq!(
        published[0].payload["objective_verdict"]["verdict"],
        serde_json::to_value(verdict).unwrap()
    );
    assert_eq!(
        published[0].transaction_id,
        receipt.transaction.transaction_id
    );
    let outbox = services
        .event_store()
        .session_terminal(&terminal.terminal_id)
        .unwrap()
        .unwrap();
    assert_eq!(outbox.commit_cursor, receipt.transaction.commit_cursor);
    assert_eq!(outbox.payload_ref, terminal.payload_ref);
}

#[cfg(test)]
#[tokio::test]
async fn r1_772_original_self_withdrawal_761_retires_without_control_graph() {
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "772-semantic-retirement-discriminator",
    ).await;
    let services = &fixture.services;
    let _cleanup = RealFixtureCleanup { services: services.clone(), host: fixture.host.clone() };
    let worker = fixture.dispatch("772-self-withdraw", vec!["read:.".into()], false, false, true).await;
    let goal_id = format!("goal:{}", worker.root_actor.root_execution_id.as_ref().unwrap());
    let goal_before = services.goal_store().get(&goal_id).unwrap();
    let goal_present = goal_before.is_some();
    if let Some(goal) = &goal_before {
        assert_eq!(goal.completion, harness_contract::goal::GoalCompletion::Open);
        assert!(goal.terminal.is_none());
    }
    let original_io = "772-original-governed-read";
    assert!(!worker.read(original_io, "out/a").await.is_empty());
    worker.assert_host_read(original_io);
    worker.finish_readonly_control().await;
    worker.assert_host_read(original_io);
    let program = services.agent_action_service().project(&worker.root_actor.program_id).unwrap();
    let task = &program.tasks[&worker.task_ref];
    assert_eq!(task.status, crate::AgenticTaskStatus::Withdrawn);
    assert_eq!(task.claim_generation, 2);
    assert!(task.active_attempts.is_empty());
    assert!(task.pending_retirement.is_none());
    assert!(task.claimant.is_none() && task.claim_execution_id.is_none());
    assert!(program.objective_verdict.is_none());
    assert_eq!(services.goal_store().get(&goal_id).unwrap(), goal_before);
    assert!(worker.lifecycle().no_direct_effects());
    assert!(confirmed_run_exit_with_retry(services.event_store(), &worker.packet).is_some());
    let links = services.graph_state_store().child_links(worker.packet.graph_id()).unwrap();
    let mut original_io_children = 0;
    for link in &links {
        let child = services.graph_state_store().load(&link.child_execution_id).unwrap();
        if crate::execution_core::graph::executors::agent_tool::is_bound_agent_tool_graph(&child) {
            let bound = crate::execution_core::graph::executors::agent_tool::decode_bound_agent_tool_graph(&child).unwrap();
            assert_ne!(bound.request.tool_name, "task_withdraw");
            assert_ne!(bound.request.tool_name, "task_claim");
            if bound.request.tool_use_id == original_io {
                original_io_children += 1;
                assert!(child.node_statuses.values().all(|status| *status == ExecutionNodeStatus::Completed));
            }
        }
    }
    assert_eq!(original_io_children, 1);
    eprintln!("R1_772_761_RETIREMENT {}", serde_json::json!({
        "task": task.task_id, "status": task.status, "claim_generation": task.claim_generation,
        "run": worker.packet.run_id(), "graph": worker.packet.graph_id(),
        "original_io_children": original_io_children, "control_graphs": 0,
        "goal_present": goal_present,
        "goal_unchanged": services.goal_store().get(&goal_id).unwrap() == goal_before,
    }));
}

#[cfg(test)]
#[tokio::test]
async fn r1_known_exited_process_helpers_reject_without_recreating_control() {
    use std::time::{Duration, Instant};
    let fixture = PhysicalProcessFixture::new(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "known-exited-helper-regression-319",
    )
    .await;
    let services = &fixture.services;
    let _cleanup = RealFixtureCleanup {
        services: services.clone(),
        host: fixture.host.clone(),
    };
    let worker = fixture
        .dispatch(
            "known-exited-helper-regression-319",
            vec!["read:.".into()],
            false,
            false,
            true,
        )
        .await;
    let directory = worker.directory();
    worker.finish_readonly_control().await;
    assert!(
        confirmed_run_exit_with_retry(services.event_store(), &worker.packet)
            .is_some()
    );
    let program = services
        .agent_action_service()
        .project(&worker.root_actor.program_id)
        .unwrap();
    let task = &program.tasks[&worker.task_ref];
    assert_eq!(task.status, crate::AgenticTaskStatus::Withdrawn);
    assert_eq!(task.claim_generation, 2);
    assert!(task.claimant.is_none() && task.claim_execution_id.is_none());
    assert!(task.active_attempts.is_empty());
    assert!(!directory.exists());
    let applied = services
        .event_store()
        .event_by_idempotency_key(
            &format!("agentic-program:{}", program.program_id),
            &format!(
                "agent-action:readonly-control-withdraw:{}",
                worker.packet.run_id()
            ),
        )
        .unwrap()
        .expect("original Process withdrawal must be durable");
    assert_eq!(applied.kind, "agentic.action_applied");
    let withdrawal: AgentActionEnvelope =
        serde_json::from_value(applied.payload["envelope"].clone()).unwrap();
    assert!(
        matches!(&withdrawal.action, AgentAction::TaskWithdraw(input)
        if input.task_ref == worker.task_ref)
    );
    assert_eq!(
        applied.actor.as_deref(),
        Some(withdrawal.actor.actor_id.as_str())
    );
    worker.assert_original_control_action(&withdrawal);
    let original_request_id = format!("{}:invocation:retirement", withdrawal.action_id);
    let mut original_requests = Vec::new();
    for stream in services
        .event_store()
        .stream_ids_for_scope(crate::RuntimeEventScope::Agent)
        .unwrap()
    {
        if !stream.starts_with("agent-process-transport:") {
            continue;
        }
        for event in services.event_store().list_stream(&stream).unwrap() {
            if event.kind != "agent.process_request_completed" {
                continue;
            }
            let Some(output) = event.payload["response"]["output"].as_str() else {
                continue;
            };
            let Ok(observation) = serde_json::from_str::<
                harness_contract::agent_action::AgentActionObservation,
            >(output) else {
                continue;
            };
            if observation.action_id == withdrawal.action_id && !observation.duplicate {
                original_requests.push(event.payload["request_id"].as_str().unwrap().to_string());
            }
        }
    }
    assert!(original_requests.len() <= 1);
    if let Some(completed_request) = original_requests.first() {
        assert_eq!(completed_request, &original_request_id);
        worker.assert_original_control_transport(&withdrawal);
    }
    let original_request = &original_request_id;
    let fresh_request = format!("{}:late-regression", original_request);
    services.shutdown_maintenance().await;
    let cursor = services.event_store().durable_commit_cursor().unwrap();
    let host_calls = fixture.host.calls.lock().unwrap().len();
    let program_before = serde_json::to_value(&program).unwrap();
    let graph_before = serde_json::to_value(
        services
            .graph_state_store()
            .load(worker.packet.graph_id())
            .unwrap(),
    )
    .unwrap();
    let sentinel = services.workspace_root().join("out/a");
    let sentinel_before = std::fs::read(&sentinel).unwrap();
    let effects_before = services
        .event_store()
        .stream_ids_for_scope(crate::RuntimeEventScope::ExecutionNode)
        .unwrap();
    let unchanged = || {
        assert!(!directory.exists());
        assert!(!directory.join("actionmeta").exists());
        assert_eq!(
            services.event_store().durable_commit_cursor().unwrap(),
            cursor
        );
        assert_eq!(fixture.host.calls.lock().unwrap().len(), host_calls);
        assert_eq!(
            serde_json::to_value(
                services
                    .agent_action_service()
                    .project(&program.program_id)
                    .unwrap(),
            )
            .unwrap(),
            program_before
        );
        assert_eq!(
            serde_json::to_value(
                services
                    .graph_state_store()
                    .load(worker.packet.graph_id())
                    .unwrap(),
            )
            .unwrap(),
            graph_before
        );
        assert_eq!(
            services
                .event_store()
                .stream_ids_for_scope(crate::RuntimeEventScope::ExecutionNode,)
                .unwrap(),
            effects_before
        );
        assert_eq!(std::fs::read(&sentinel).unwrap(), sentinel_before);
    };
    let expected = "tool_effect_authority: original Process has physically exited";
    unchanged();
    let denied = tokio::time::timeout(Duration::from_secs(2), worker.process.started())
        .await
        .expect("known exit must not wait for a deleted startup marker")
        .unwrap_err();
    assert_eq!(denied, expected);
    unchanged();
    let denied = tokio::time::timeout(
        Duration::from_secs(2),
        worker.process.command(serde_json::json!({"finish":true})),
    )
    .await
    .expect("known exit must not publish a command")
    .unwrap_err();
    assert_eq!(denied, expected);
    unchanged();
    let denied = tokio::time::timeout(
        Duration::from_secs(2),
        worker.process.execute_tool(
            "late-after-exit-regression",
            "edit",
            r#"{"action":"write","path":"out/a","content":"must not execute"}"#,
        ),
    )
    .await
    .expect("known exit must reject before Process IPC")
    .unwrap_err();
    assert_eq!(denied, expected);
    unchanged();
    for request in [&fresh_request, original_request] {
        let denied = tokio::time::timeout(
            Duration::from_secs(2),
            worker.process.action_with_request_id(&withdrawal, request),
        )
        .await
        .expect("late action and transport replay must reject before metadata")
        .unwrap_err();
        assert_eq!(denied, expected);
        unchanged();
        let started = Instant::now();
        let denied = worker
            .process
            .prepare_action_metadata_239(&withdrawal, request)
            .unwrap_err();
        assert_eq!(denied, expected);
        assert!(started.elapsed() < Duration::from_secs(2));
        unchanged();
    }
    tokio::time::timeout(Duration::from_secs(2), worker.process.finish())
        .await
        .expect("known-exited finish must use original exit, not recreate startup state");
    unchanged();
}

fn r1_goal_proof_diagnostic(services: &ReviewedFixtureServices, program_id: &str) -> String {
    let program_stream = format!("agentic-program:{program_id}");
    crate::failure_evidence::collect_failure_evidence(
        services.event_store(),
        &[
            crate::failure_evidence::FailureEvidenceSource {
                stream_id: &program_stream,
                after_sequence: 0,
                through_sequence: u64::MAX,
            },
            crate::failure_evidence::FailureEvidenceSource {
                stream_id: "goal:goal:root-agentic-execution",
                after_sequence: 0,
                through_sequence: u64::MAX,
            },
        ],
        "Original failure is reported by caller; no auxiliary proof revalidation",
    ).to_string()
}

#[cfg(test)]
mod fixture_protocol_557_tests {
    use super::r1_root_review_target_input;

    fn assert_registered_shape(name: &str, input: &serde_json::Value) {
        let specification = tools::mvp_tool_specs()
            .into_iter()
            .find(|specification| specification.name == name)
            .unwrap();
        let schema = &specification.input_schema;
        assert_eq!(schema["additionalProperties"], false);
        let properties = schema["properties"].as_object().unwrap();
        let object = input.as_object().unwrap();
        assert!(object.keys().all(|key| properties.contains_key(key)));
        for required in schema["required"].as_array().unwrap() {
            assert!(object.contains_key(required.as_str().unwrap()));
        }
    }

    #[test]
    fn r1_fixture_root_read_uses_registered_eof_shape_without_write_fields() {
        let input = r1_root_review_target_input("read_file", "review-effect.txt", "");
        assert_eq!(
            input,
            serde_json::json!({"path":"review-effect.txt","complete":true})
        );
        assert_registered_shape("read_file", &input);
    }

    #[test]
    fn r1_fixture_root_write_uses_registered_action_without_read_fields() {
        let input = r1_root_review_target_input("edit", "review-effect.txt", "actual contents");
        assert_eq!(
            input,
            serde_json::json!({
                "action":"write","path":"review-effect.txt","content":"actual contents",
            })
        );
        assert_registered_shape("edit", &input);
        let specification = tools::mvp_tool_specs()
            .into_iter()
            .find(|specification| specification.name == "edit")
            .unwrap();
        assert!(specification.input_schema["properties"]["action"]["enum"]
            .as_array()
            .unwrap()
            .contains(&input["action"]));
    }
}

fn r1_prepare_fixture_terminal_payload(
    services: &RuntimeServices,
    terminal: &mut crate::SessionTerminalInput,
    event_key: &str,
) {
    let scope = format!("session:{}", terminal.session_id);
    let artifacts = services.artifact_store();
    let original_result = terminal.payload_ref.clone();
    let source = artifacts.resolve(&original_result).unwrap();
    let text = String::from_utf8(
        artifacts.read_blocking(&source, &scope, None).unwrap(),
    ).expect("commit-owner fixture terminal must retain actual UTF-8 result content");
    let body = serde_json::to_vec(&serde_json::json!({
        "schema_version":crate::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,
        "text":text,
        "source_result_ref":original_result,
    })).unwrap();
    let origin = crate::PreparedArtifactOrigin::TerminalBody {
        terminal_id: terminal.terminal_id.clone(),
        session_id: terminal.session_id.clone(),
        execution_id: terminal.execution_id.clone().expect("original terminal execution"),
        turn_id: terminal.turn_id.clone().expect("original terminal turn"),
        event_stream: format!("session-terminal:{}", terminal.request_id.as_deref().unwrap()),
        event_key: event_key.into(),
    };
    let artifact = artifacts.write_pre_pinned_with_media_blocking(
        &scope,
        &format!("prepared:terminal:{}", terminal.terminal_id),
        "application/vnd.cowd.session-terminal+json",
        &body,
        &origin,
    ).unwrap();
    terminal.payload_ref = crate::encode_session_terminal_artifact_ref(&artifact).unwrap();
    assert_eq!(crate::decode_session_terminal_artifact_ref(&terminal.payload_ref).unwrap(), artifact);
    assert_eq!(artifacts.read_prepared_blocking(&artifact, &scope, &origin).unwrap(), body);
}

fn r1_original_process_read_659(
    services: &RuntimeServices,
    packet: &AgentTaskPacket,
    call_id: &str,
) -> (String, crate::context_evidence::ReadDeliveryMaterial, String) {
    use crate::context_evidence::{ReadDeliveryRecipient, ReadDeliveryTransport, READ_ACQUISITION_FIELD};
    let material = assert_process_delivery(services, packet, call_id);
    assert_eq!(material.recipient, ReadDeliveryRecipient::AgentTask {
        identity: packet.assignment.execution_identity.clone(),
    });
    assert_eq!(material.acquisition.node_id, packet.node_id());
    assert_eq!(material.session_id, packet.session_id());
    assert_eq!(Some(material.turn_id.as_str()), packet.assignment.execution_identity.turn_id());
    assert!(!material.body_omitted);
    assert_eq!(material.delivered_extent.as_ref(), Some(&material.acquisition.body.extent));
    let seal = &material.acquisition;
    let primary = services.event_store()
        .event_by_idempotency_key(&seal.primary_stream, &seal.primary_key).unwrap().unwrap();
    let index = services.event_store()
        .event_by_idempotency_key(&seal.index_stream, &seal.index_key).unwrap().unwrap();
    assert_eq!(primary.kind, "execution.read.receipt");
    assert_eq!(index.kind, "execution.agent_tool.receipt");
    assert_eq!(primary.scope, crate::RuntimeEventScope::ExecutionNode);
    assert_eq!(index.scope, primary.scope);
    assert_eq!(primary.actor.as_deref(), Some("governed_tool"));
    assert_eq!(index.actor, primary.actor);
    assert_eq!(primary.payload["outcome"]["status"], "executed");
    assert_eq!(index.payload["outcome"]["status"], "executed");
    assert_eq!(primary.payload["tool_use_id"], seal.invocation_id);
    assert_eq!(primary.payload["tool_name"], seal.tool_name);
    assert_eq!(primary.payload["input_sha256"], seal.input_sha256);
    assert_eq!(primary.event_id, material.acquisition_event_id);
    assert_eq!(primary.commit_cursor, material.acquisition_cursor);
    assert_eq!(index.event_id, material.index_event_id);
    assert_eq!(primary.transaction_id, index.transaction_id);
    assert_eq!(primary.commit_cursor, index.commit_cursor);
    assert!(primary.transaction_index < index.transaction_index);
    assert_eq!(primary.payload[READ_ACQUISITION_FIELD], serde_json::to_value(seal).unwrap());
    assert_eq!(index.payload[READ_ACQUISITION_FIELD], primary.payload[READ_ACQUISITION_FIELD]);
    assert_eq!(index.stream_id, format!("execution-agent-receipts:{}:{}:{}",
        packet.graph_id(), packet.node_id(), packet.attempt));
    let sequence = index.payload["sequence"].as_u64().unwrap();
    assert!(sequence > 0);
    let receipt_ref = format!("{}#{sequence}", index.stream_id);
    let ReadDeliveryTransport::ProcessChannelSend {
        worker_id, request_id, step_id, request_fingerprint, ..
    } = &material.transport else { panic!("only the original Process delivery can supply this receipt"); };
    assert_eq!(worker_id, packet.run_id());
    assert_eq!(request_id, call_id);
    assert_eq!(request_id, &seal.invocation_id);
    assert_eq!(step_id, &seal.step.step_id);
    assert_eq!(seal.step.run_ref.as_deref(), Some(packet.run_id()));
    let identity = serde_json::json!([
        packet.session_id(), packet.graph_id(), packet.node_id(), packet.run_id(), packet.agent_id(),
        packet.attempt, packet.binding.as_ref().map(|binding| binding.binding_digest.as_str()),
    ]);
    let stream_hash = crate::context_evidence::read_sha256(identity.to_string().as_bytes());
    let key_hash = crate::context_evidence::read_sha256(request_id.as_bytes());
    let completed = services.event_store().event_by_idempotency_key(
        &format!("agent-process-transport:{}", stream_hash.trim_start_matches("sha256:")),
        &format!("completed:{}", key_hash.trim_start_matches("sha256:")),
    ).unwrap().expect("the actual original Process response must remain durable");
    assert_eq!(completed.kind, "agent.process_request_completed");
    assert_eq!(completed.payload["fingerprint"], *request_fingerprint);
    let output = completed.payload["response"]["output"].as_str().unwrap().to_string();
    assert_eq!(crate::context_evidence::read_sha256(output.as_bytes()), material.presentation_sha256);
    assert_eq!(output.len() as u64, material.presentation_bytes);
    let delivery = process_materials(services, packet).into_iter()
        .find(|(_, candidate)| candidate.tool_invocation_id == call_id).unwrap().0;
    assert!(completed.commit_cursor < delivery.commit_cursor);
    (receipt_ref, material, output)
}
