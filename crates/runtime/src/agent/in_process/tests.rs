use std::collections::BTreeSet;

use super::*;

#[test]
fn managed_escalation_recovery_input_contains_only_semantic_delta() {
    let input = managed_escalation_recovery_input(&test_agent_packet(Vec::new()));
    let value: serde_json::Value = serde_json::from_str(&input).expect("recovery input");
    assert!(value.get("base_revision").is_none());
    assert!(value.get("digest").is_none());
    assert!(value.get("reason").is_some());
    assert!(value
        .pointer("/requested_add_team/semantic_node_id")
        .is_some());
    assert!(value.pointer("/requested_add_team/objective").is_some());
}
use harness_contract::agent::AgentCommand;
use harness_contract::turn::TurnId;
use sha2::{Digest, Sha256};

fn test_authorization_lease(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    ceiling: PermissionMode,
    idempotency_key: &str,
) -> harness_contract::policy::AuthorizationLease {
    harness_contract::policy::AuthorizationLease {
        lease_id: format!("test-lease:{idempotency_key}"),
        principal_id: "test-agent".to_string(),
        parent_lease_id: None,
        capability: descriptor.tool_id.clone(),
        scopes: descriptor.scopes.clone(),
        ceiling,
        issued_at_ms: 0,
        expires_at_ms: u64::MAX,
        max_uses: 1,
        remaining_uses: 1,
        idempotency_key: idempotency_key.to_string(),
        policy_revision: 1,
        effect_descriptor_hash: descriptor.descriptor_hash.clone(),
        signature: "test-signature".to_string(),
        status: harness_contract::policy::AuthorizationLeaseStatus::Active,
    }
}

fn test_capability_assessment(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    required_mode: PermissionMode,
) -> harness_contract::policy::CapabilityAssessment {
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(descriptor, "{}");
    harness_contract::policy::CapabilityAssessment {
        assessment_id: "test-assessment".to_string(),
        capability: descriptor.tool_id.clone(),
        effect: effective.descriptor.assessment,
        requested_scopes: effective.descriptor.scopes,
        required_mode,
        active_ceiling: PermissionMode::DangerFullAccess,
        parent_ceiling: PermissionMode::DangerFullAccess,
        risk: harness_contract::policy::RiskLevel::Low,
        path: harness_contract::policy::AuthorizationPath::PolicyAutoGrant,
        lease: None,
        gap: None,
        evidence_refs: Vec::new(),
        assessed_at_ms: 0,
    }
}

fn scoped_receipt(
    sequence: u64,
    effect_kind: harness_contract::tool::ToolEffectKind,
    path: &str,
    before: Option<&str>,
    after: Option<&str>,
) -> ScopedToolExecutionReceipt {
    ScopedToolExecutionReceipt {
        sequence,
        provider_invocation_id: None,
        tool_name: "test_tool".to_string(),
        effect_kind,
        resource_scopes: vec![format!(
            "{}:{path}",
            if effect_kind == harness_contract::tool::ToolEffectKind::Write {
                "write"
            } else {
                "read"
            }
        )],
        paths: vec![path.to_string()],
        prior_states: before
            .map(|sha256| {
                BTreeMap::from([(
                    path.to_string(),
                    harness_contract::context::WorkspacePriorState::Existing {
                        sha256: sha256.to_string(),
                    },
                )])
            })
            .unwrap_or_else(|| {
                BTreeMap::from([(
                    path.to_string(),
                    harness_contract::context::WorkspacePriorState::Absent,
                )])
            }),
        after_digests: BTreeMap::from([(path.to_string(), after.map(str::to_string))]),
        observed_bytes: BTreeMap::new(),
        observed_evidence: Vec::new(),
    }
}

fn test_agent_packet(
    evidence_refs: Vec<harness_contract::context::EvidenceAccessRef>,
) -> AgentTaskPacket {
    AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            "agent",
            "run",
            "task",
            "session",
            "mission",
            Some("team"),
            "graph",
            "node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "review".into(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        requires_managed_collaboration_escalation: false,
        acceptance: Vec::new(),
        team_role_identity: None,
        team_role: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs,
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget",
            "agent",
            "agent",
            1,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "key".into(),
    }
}

#[test]
fn change_and_source_verification_require_digest_delta_and_post_write_read() {
    let unchanged = vec![scoped_receipt(
        1,
        harness_contract::tool::ToolEffectKind::Write,
        "src/lib.rs",
        Some("same"),
        Some("same"),
    )];
    assert!(materialized_change_receipts(&unchanged).is_empty());

    let read_before_write = vec![
        scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Read,
            "src/lib.rs",
            Some("before"),
            Some("before"),
        ),
        scoped_receipt(
            2,
            harness_contract::tool::ToolEffectKind::Write,
            "src/lib.rs",
            Some("before"),
            Some("after"),
        ),
    ];
    let change = materialized_change_receipts(&read_before_write)
        .pop()
        .expect("real digest change");
    assert!(has_matching_pre_write_evidence(&change, &read_before_write));
    assert!(!has_matching_read_receipt(
        &change,
        &read_before_write,
        true
    ));

    let write_then_read = vec![
        scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Write,
            "src/lib.rs",
            Some("before"),
            Some("after"),
        ),
        scoped_receipt(
            2,
            harness_contract::tool::ToolEffectKind::Read,
            "src/lib.rs",
            Some("after"),
            Some("after"),
        ),
    ];
    let ungrounded = materialized_change_receipts(&write_then_read)
        .pop()
        .expect("digest changed");
    assert_eq!(ungrounded.reread_sequence, Some(2));
    assert!(!has_matching_pre_write_evidence(
        &ungrounded,
        &write_then_read
    ));
    assert!(has_matching_read_receipt(
        &ungrounded,
        &write_then_read,
        true
    ));
    assert_eq!(
        tool_output_byte_length(
            r#"Tool completed. Evidence: tool://read. {"file":{"filePath":"src/lib.rs","byteLength":56262}}"#
        ),
        Some(56_262)
    );

    let mut verified = read_before_write;
    verified.push(scoped_receipt(
        3,
        harness_contract::tool::ToolEffectKind::Read,
        "src/lib.rs",
        Some("after"),
        Some("after"),
    ));
    assert!(has_matching_pre_write_evidence(&change, &verified));
    assert!(has_matching_read_receipt(&change, &verified, true));
}

#[test]
fn new_file_source_verification_uses_runtime_absence_proof_and_post_write_read() {
    let write_then_read = vec![
        scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Write,
            "evidence/report.html",
            None,
            Some("created"),
        ),
        scoped_receipt(
            2,
            harness_contract::tool::ToolEffectKind::Read,
            "evidence/report.html",
            Some("created"),
            Some("created"),
        ),
    ];
    let change = materialized_change_receipts(&write_then_read)
        .pop()
        .expect("new file is a materialized change");
    assert!(has_matching_pre_write_evidence(&change, &write_then_read));
    assert!(has_matching_read_receipt(&change, &write_then_read, true));

    let mut missing_absence_proof = write_then_read.clone();
    missing_absence_proof[0]
        .prior_states
        .remove("evidence/report.html");
    assert!(!has_matching_pre_write_evidence(
        &change,
        &missing_absence_proof
    ));

    assert!(!has_matching_read_receipt(
        &change,
        &write_then_read[..1],
        true
    ));
}

#[test]
fn upstream_review_matches_normalized_receipt_path_and_its_digest_key() {
    let change = harness_contract::agent::AgentChangeReceipt {
        path: "fixtures/auto-strategy-write/target.txt".to_string(),
        before_sha256: Some("before".to_string()),
        after_sha256: "after".to_string(),
        write_sequence: 3,
        bytes: None,
        reread_sequence: None,
        reread_evidence_ref: None,
    };
    let receipt = ScopedToolExecutionReceipt {
        sequence: 1,
        provider_invocation_id: None,
        tool_name: "read_file".to_string(),
        effect_kind: harness_contract::tool::ToolEffectKind::Read,
        resource_scopes: vec!["read:./fixtures/auto-strategy-write/target.txt".to_string()],
        paths: vec!["./fixtures/auto-strategy-write/target.txt".to_string()],
        prior_states: BTreeMap::from([(
            "./fixtures/auto-strategy-write/target.txt".to_string(),
            harness_contract::context::WorkspacePriorState::Existing {
                sha256: "after".to_string(),
            },
        )]),
        after_digests: BTreeMap::from([(
            "./fixtures/auto-strategy-write/target.txt".to_string(),
            Some("after".to_string()),
        )]),
        observed_bytes: BTreeMap::new(),
        observed_evidence: Vec::new(),
    };

    assert!(has_matching_read_receipt(&change, &[receipt], false));
}

#[test]
fn fresh_tool_receipt_is_evidence_even_when_content_ref_matches_upstream() {
    let upstream = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("tool", "same-content"),
        "sha256:same",
        1,
        "text/plain",
        "artifact://art_worker_upstream",
        "session:session",
    );
    let packet = test_agent_packet(vec![upstream.clone()]);
    assert!(!produced_runtime_evidence(
        &packet,
        &[upstream.clone()],
        &[]
    ));
    assert!(produced_runtime_evidence(
        &packet,
        &[upstream],
        &[scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Read,
            "fixtures/target.txt",
            Some("same"),
            Some("same"),
        )],
    ));
}

#[test]
fn network_receipts_satisfy_only_the_network_evidence_lease() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("resolver");
    let required = resolver.compile_obligation_or_unresolved("network:*");
    let observed = resolver
        .observe_tool_scope("web_search", "network:*", None, 1)
        .expect("network receipt");
    assert!(crate::path_identity::observed_evidence_satisfies(
        &required, &observed
    ));
}

#[test]
fn unqualified_team_scope_matches_typed_runtime_receipts() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("scope");
    std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "checked").expect("file");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("resolver");
    let required = resolver.compile_obligation_or_unresolved("read:crates/runtime");
    let observed = resolver
        .observe_tool_scope(
            "read_file",
            "read:crates/runtime/src/lib.rs",
            Some("sha256:checked"),
            1,
        )
        .expect("receipt");
    assert!(crate::path_identity::observed_evidence_satisfies(
        &required, &observed
    ));
}

#[test]
fn upstream_change_receipt_is_recovered_from_durable_evidence_binding() {
    let change = harness_contract::agent::AgentChangeReceipt {
        path: "fixtures/target.txt".to_string(),
        before_sha256: Some("before".to_string()),
        after_sha256: "after".to_string(),
        write_sequence: 3,
        bytes: None,
        reread_sequence: None,
        reread_evidence_ref: None,
    };
    let encoded = serde_json::to_string(&change).expect("change receipt JSON");
    let evidence = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("runtime_change", encoded),
        "sha256:change",
        1,
        "application/json",
        "artifact://art_worker_change",
        "session:session",
    );
    let packet = test_agent_packet(vec![evidence]);

    assert_eq!(packet_upstream_change_receipts(&packet), vec![change]);
}

#[test]
fn upstream_change_receipt_uses_the_final_digest_after_repeated_writes() {
    let changes = [
        harness_contract::agent::AgentChangeReceipt {
            path: "fixtures/target.txt".to_string(),
            before_sha256: Some("before".to_string()),
            after_sha256: "intermediate".to_string(),
            write_sequence: 3,
            bytes: None,
            reread_sequence: None,
            reread_evidence_ref: None,
        },
        harness_contract::agent::AgentChangeReceipt {
            path: "fixtures/target.txt".to_string(),
            before_sha256: Some("intermediate".to_string()),
            after_sha256: "terminal".to_string(),
            write_sequence: 8,
            bytes: None,
            reread_sequence: None,
            reread_evidence_ref: None,
        },
    ];
    let evidence = changes
        .iter()
        .map(|change| {
            let encoded = serde_json::to_string(change).expect("change receipt JSON");
            harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed("runtime_change", encoded),
                "sha256:change",
                1,
                "application/json",
                "artifact://art_worker_change",
                "session:session",
            )
        })
        .collect();
    let packet = test_agent_packet(evidence);

    assert_eq!(
        packet_upstream_change_receipts(&packet),
        vec![changes[1].clone()]
    );
}

#[test]
fn explicit_empty_risk_list_is_a_materialized_review_result() {
    use harness_contract::team::TeamStructuredOutputField;

    assert!(structured_field_materialized(
        TeamStructuredOutputField::Risks,
        Some(&serde_json::json!([])),
    ));
    assert!(!structured_field_materialized(
        TeamStructuredOutputField::Risks,
        None,
    ));
    assert!(!structured_field_materialized(
        TeamStructuredOutputField::Review,
        Some(&serde_json::json!([])),
    ));
}

struct NoopRuntimeExecutionHost;

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for NoopRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        _request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        panic!("the capability advertisement test must not execute a tool")
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

struct EchoRuntimeExecutionHost;

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for EchoRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        if request.authorization.is_none() {
            return crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some("missing propagated authorization".to_string()),
                evidence_ref: format!("agent-tool:{}", request.tool_use_id),
                observed_evidence: Vec::new(),
            };
        }
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(format!("authorized:{}", request.tool_name)),
            error: None,
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

struct ConcurrencyTrackingRuntimeExecutionHost {
    active: std::sync::atomic::AtomicUsize,
    max_active: std::sync::atomic::AtomicUsize,
}

impl ConcurrencyTrackingRuntimeExecutionHost {
    fn new() -> Self {
        Self {
            active: std::sync::atomic::AtomicUsize::new(0),
            max_active: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.active.store(0, Ordering::SeqCst);
        self.max_active.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for ConcurrencyTrackingRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some("{}".to_string()),
            error: None,
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

fn concurrency_test_executor(
    root: &std::path::Path,
    host: Arc<ConcurrencyTrackingRuntimeExecutionHost>,
    scope_locks: Arc<ScopeLockManager>,
) -> ScopedRuntimeToolExecutor {
    ScopedRuntimeToolExecutor {
        host,
        allowed_tools: BTreeSet::from(["write_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root)
                .expect("path identities"),
        ),
        scope_locks,
        commit_service: None,
        resource_scopes: None,
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    }
}

#[tokio::test]
async fn delegated_leaf_effects_serialize_conflicts_and_parallelize_unrelated_paths() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("fixtures/sub")).expect("fixture directories");
    let host = Arc::new(ConcurrencyTrackingRuntimeExecutionHost::new());
    let locks = Arc::new(ScopeLockManager::new());
    let first = Arc::new(concurrency_test_executor(
        root.path(),
        Arc::clone(&host),
        Arc::clone(&locks),
    ));
    let second = Arc::new(concurrency_test_executor(
        root.path(),
        Arc::clone(&host),
        Arc::clone(&locks),
    ));

    let same = tokio::join!(
        first.execute_scoped(
            "write_file",
            r#"{"path":"fixtures/sub/target.txt","content":"one"}"#,
            None,
            None,
        ),
        second.execute_scoped(
            "write_file",
            r#"{"path":"fixtures/sub/target.txt","content":"two"}"#,
            None,
            None,
        )
    );
    same.0.expect("first same-path effect");
    same.1.expect("second same-path effect");
    assert_eq!(host.max_active.load(Ordering::SeqCst), 1);

    host.reset();
    let parent_child = tokio::join!(
        first.execute_scoped(
            "write_file",
            r#"{"path":"fixtures/sub","content":"parent"}"#,
            None,
            None,
        ),
        second.execute_scoped(
            "write_file",
            r#"{"path":"fixtures/sub/target.txt","content":"child"}"#,
            None,
            None,
        )
    );
    parent_child.0.expect("parent-path effect");
    parent_child.1.expect("child-path effect");
    assert_eq!(host.max_active.load(Ordering::SeqCst), 1);

    host.reset();
    let unrelated = tokio::join!(
        first.execute_scoped(
            "write_file",
            r#"{"path":"fixtures/left.txt","content":"left"}"#,
            None,
            None,
        ),
        second.execute_scoped(
            "write_file",
            r#"{"path":"fixtures/right.txt","content":"right"}"#,
            None,
            None,
        )
    );
    unrelated.0.expect("left effect");
    unrelated.1.expect("right effect");
    assert_eq!(host.max_active.load(Ordering::SeqCst), 2);
}

struct ManagedEscalationRecoveryHost {
    received_bound_recovery: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for ManagedEscalationRecoveryHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let accepted = request.tool_name == "request_collaboration_escalation"
            && request.authorization.is_none()
            && request
                .parent_execution
                .as_ref()
                .is_some_and(|parent| parent.execution_id == "graph" && parent.node_id == "node")
            && request.parent_execution_attempt == Some(1)
            && request.managed_invocation.is_none();
        self.received_bound_recovery
            .store(accepted, Ordering::SeqCst);
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if accepted {
                crate::RuntimeToolExecutionStatus::Executed
            } else {
                crate::RuntimeToolExecutionStatus::BlockedPermission
            },
            category: request.category,
            output: accepted.then(|| "escalation accepted".to_string()),
            error: (!accepted).then(|| "invalid escalation recovery request".to_string()),
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

#[tokio::test]
async fn managed_escalation_recovery_is_bound_and_durably_receipted() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("source scope");
    std::fs::write(
        root.path().join("crates/runtime/src/lib.rs"),
        "source evidence",
    )
    .expect("source file");
    let resolver = Arc::new(
        crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities"),
    );
    let source_evidence = resolver
        .observe_tool_scope(
            "read_file",
            "read:crates/runtime/src/lib.rs",
            Some("sha256:source-evidence"),
            1,
        )
        .expect("source evidence receipt");
    let host = Arc::new(ManagedEscalationRecoveryHost {
        received_bound_recovery: std::sync::atomic::AtomicBool::new(false),
    });
    let executor = ScopedRuntimeToolExecutor {
        host: host.clone(),
        allowed_tools: BTreeSet::from(["request_collaboration_escalation".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: resolver,
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:crates/runtime".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(1),
        receipts: Mutex::new(vec![ScopedToolExecutionReceipt {
            sequence: 1,
            provider_invocation_id: None,
            tool_name: "read_file".to_string(),
            effect_kind: harness_contract::tool::ToolEffectKind::Read,
            resource_scopes: vec!["read:crates/runtime/src/lib.rs".to_string()],
            paths: vec!["crates/runtime/src/lib.rs".to_string()],
            prior_states: BTreeMap::new(),
            after_digests: BTreeMap::new(),
            observed_bytes: BTreeMap::new(),
            observed_evidence: vec![source_evidence],
        }]),
        provider_model_obligations: Vec::new(),
    };

    executor
            .execute_managed_escalation_recovery(
                r#"{"reason":"need a second team","requested_add_team":{"semantic_node_id":"follow-up","objective":"verify"}}"#,
            )
            .await
            .expect("Runtime-owned recovery succeeds");

    assert!(host.received_bound_recovery.load(Ordering::SeqCst));
    let receipts = executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert!(receipts.iter().any(|receipt| {
        receipt.tool_name == "request_collaboration_escalation"
            && receipt.resource_scopes == ["runtime:collaboration_escalation"]
    }));
}

struct InputSensitiveRuntimeExecutionHost;

impl InputSensitiveRuntimeExecutionHost {
    fn descriptor(
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        let mut descriptor = test_tool_descriptor_for_input(tool_name, input)?;
        let encoded = serde_json::to_vec(input).ok()?;
        descriptor.descriptor_hash = format!("input:{:x}", Sha256::digest(encoded));
        Some(descriptor)
    }
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for InputSensitiveRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let parsed = serde_json::from_str::<serde_json::Value>(&request.input).ok();
        let current_hash = parsed.as_ref().and_then(|input| {
            Self::descriptor(&request.tool_name, input).map(|descriptor| descriptor.descriptor_hash)
        });
        let authorized_hash = request
            .authorization
            .as_ref()
            .map(|authorization| authorization.descriptor_hash.as_str());
        let authorized = current_hash.as_deref() == authorized_hash;
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if authorized {
                crate::RuntimeToolExecutionStatus::Executed
            } else {
                crate::RuntimeToolExecutionStatus::BlockedPermission
            },
            category: request.category,
            output: authorized.then(|| format!("authorized:{}", request.tool_name)),
            error: (!authorized).then(|| "tool authorization is stale".to_string()),
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        Self::descriptor(tool_name, input)
    }
}

fn test_tool_descriptor_for_input(
    tool_name: &str,
    input: &serde_json::Value,
) -> Option<harness_contract::tool::ToolEffectDescriptor> {
    use harness_contract::policy::{
        DataClassification, EffectAssessment, EffectBlastRadius, EffectExternality, EffectNovelty,
        EffectReversibility, PermissionOperation, PermissionResource, PermissionScope,
    };
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
        ToolPermissionMode,
    };

    if tool_name == "execute_code" {
        return Some(ToolEffectDescriptor {
            tool_id: tool_name.to_string(),
            descriptor_hash: "test-host:execute_code".to_string(),
            effect_kind: ToolEffectKind::Process,
            idempotency: ToolIdempotency::Unknown,
            scopes: vec![PermissionScope {
                resource: PermissionResource::Shell,
                operation: PermissionOperation::Execute,
                target: None,
            }],
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment {
                reversibility: EffectReversibility::Reversible,
                externality: EffectExternality::Workspace,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::NewTarget,
                blast_radius: EffectBlastRadius::Workspace,
            },
        });
    }
    let (effect_kind, operation, required_permission, resource) = match tool_name {
        "read_file" | "grep_search" | "glob_search" => (
            ToolEffectKind::Read,
            PermissionOperation::Read,
            ToolPermissionMode::ReadOnly,
            PermissionResource::File,
        ),
        "request_collaboration_escalation" => (
            ToolEffectKind::Read,
            PermissionOperation::Read,
            ToolPermissionMode::ReadOnly,
            PermissionResource::Tool,
        ),
        "checkpoint_create" | "write_file" => (
            ToolEffectKind::Write,
            PermissionOperation::Write,
            ToolPermissionMode::WorkspaceWrite,
            PermissionResource::File,
        ),
        _ => return None,
    };
    Some(ToolEffectDescriptor {
        tool_id: tool_name.to_string(),
        descriptor_hash: format!("test-host:{tool_name}"),
        effect_kind,
        idempotency: ToolIdempotency::Idempotent,
        scopes: vec![PermissionScope {
            resource,
            operation,
            target: input
                .get("path")
                .or_else(|| input.get("file_path"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        }],
        required_permission,
        approval_class: ToolApprovalClass::None,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: harness_contract::policy::EffectAssessment::default(),
    })
}

#[test]
fn read_only_ceiling_never_escalates_for_a_write_tool() {
    let tools = BTreeSet::from(["write_file".to_string()]);
    let policy = permission_policy(None, PermissionMode::ReadOnly, &tools);
    assert_eq!(policy.active_mode(), PermissionMode::ReadOnly);
    assert_eq!(
        policy.required_mode_for("write_file"),
        PermissionMode::WorkspaceWrite
    );
}

#[test]
fn workspace_change_contract_retains_the_required_write_scope() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("fixtures")).expect("fixture directory");
    std::fs::write(root.path().join("fixtures/target.txt"), "target").expect("fixture file");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let mut packet = test_agent_packet(Vec::new());
    packet.required_acceptance = resolver.compile_required_acceptance(
        &[
            "implementation".to_string(),
            "source_verification".to_string(),
        ],
        &[
            "write:fixtures/target.txt".to_string(),
            "verify_after_write:fixtures/target.txt".to_string(),
        ],
    );

    assert_eq!(
        packet_focus_acceptance_scopes(&packet),
        [
            "verify_after_write:fixtures/target.txt",
            "write:fixtures/target.txt"
        ]
    );
    packet.required_acceptance = resolver.compile_required_acceptance(
        &["review".to_string()],
        &["verify_upstream_change:fixtures/target.txt".to_string()],
    );
    assert_eq!(
        packet_focus_acceptance_scopes(&packet),
        ["verify_upstream_change:fixtures/target.txt"]
    );
}

#[test]
fn exact_model_delivery_policy_is_scoped_to_the_matching_invocation() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("fixtures")).expect("fixtures");
    std::fs::write(root.path().join("fixtures/target.txt"), "target").expect("target");
    std::fs::write(root.path().join("fixtures/other.txt"), "other").expect("other");
    let resolver = Arc::new(
        crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities"),
    );
    let mut required =
        resolver.compile_required_acceptance(&[], &["read:fixtures/target.txt".to_string()]);
    crate::path_identity::require_provider_model_observation(&mut required);
    let obligation_id = required.evidence_obligations[0].obligation_id.clone();
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(InputSensitiveRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: resolver,
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:fixtures".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: required.evidence_obligations,
    };

    assert_eq!(
        executor.model_delivery_requirement(
            "read_file",
            r#"{"path":"fixtures/target.txt","complete":true}"#,
        ),
        crate::ToolModelDeliveryRequirement::exact(vec![obligation_id])
    );
    assert_eq!(
        executor.model_delivery_requirement(
            "read_file",
            r#"{"path":"fixtures/other.txt","complete":true}"#,
        ),
        crate::ToolModelDeliveryRequirement::Bounded
    );
    assert_eq!(
        executor.model_delivery_requirement("grep_search", r#"{"path":"fixtures"}"#),
        crate::ToolModelDeliveryRequirement::Bounded
    );
}

#[test]
fn evidence_scope_contract_projects_a_typed_read_resource_scope() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime")).expect("fixture directory");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let mut packet = test_agent_packet(Vec::new());
    packet.required_acceptance = resolver.compile_required_acceptance(
        &["evidence_scope:crates/runtime".to_string()],
        &["read:crates/runtime".to_string()],
    );

    assert_eq!(
        packet_focus_acceptance_scopes(&packet),
        ["read:crates/runtime"]
    );
}

#[test]
fn network_evidence_scope_preserves_its_resource_kind() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let mut packet = test_agent_packet(Vec::new());
    packet.required_acceptance = resolver.compile_required_acceptance(
        &["evidence_scope:network:*".to_string()],
        &["network:*".to_string()],
    );

    assert_eq!(packet_focus_acceptance_scopes(&packet), ["network:*"]);
}

#[test]
fn acceptance_contract_projects_materialized_output_fields_to_the_host() {
    let mut packet = test_agent_packet(Vec::new());
    packet.output_acceptance = vec![
        harness_contract::team::TeamAcceptanceRequirement {
            criterion: "evidence".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::ScopedEvidence {
                scopes: vec!["read:fixtures/target.txt".to_string()],
            },
        },
        harness_contract::team::TeamAcceptanceRequirement {
            criterion: "review".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::UpstreamReview,
        },
        harness_contract::team::TeamAcceptanceRequirement {
            criterion: "risks".to_string(),
            check: harness_contract::team::TeamAcceptanceCheck::StructuredField {
                field: harness_contract::team::TeamStructuredOutputField::Risks,
            },
        },
    ];

    assert_eq!(
        packet_required_output_fields(&packet),
        ["review".to_string(), "risks".to_string()]
    );
}

#[test]
fn workspace_internal_absolute_resource_path_is_normalized_once() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let target = root.path().join("fixtures/target.txt");
    let input = serde_json::json!({
        "path": target,
        "content": format!("do not rewrite {}", root.path().display()),
    })
    .to_string();

    let normalized =
        normalize_delegated_resource_paths("write_file", &input, root.path(), &resolver, None)
            .expect("normalize internal absolute path");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "fixtures/target.txt");
    assert!(normalized["content"]
        .as_str()
        .is_some_and(|content| content.contains(&root.path().display().to_string())));
}

#[test]
fn sole_directory_scope_normalizes_a_bare_delegated_read_path() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("external-app")).expect("project directory");
    std::fs::write(
        root.path().join("external-app/Cargo.toml"),
        "[package]\nname='external-app'\n",
    )
    .expect("fixture");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let input = serde_json::json!({"path": "Cargo.toml"}).to_string();
    assert_eq!(
        resolver
            .resolve_existing("external-app")
            .expect("scope directory")
            .object_kind,
        harness_contract::context::WorkspaceObjectKind::Directory
    );
    let candidate = resolver
        .resolve_existing("external-app/Cargo.toml")
        .expect("candidate");
    let scope = resolver.resolve_existing("external-app").expect("scope");
    assert!(
        resource_path_is_authorized(
            &resolver,
            "external-app/Cargo.toml",
            &["read:external-app".to_string()],
            false,
        ),
        "candidate={candidate:?}; scope={scope:?}"
    );
    let direct = normalize_single_scope_relative_read_value(
        "read_file",
        serde_json::json!({"path": "Cargo.toml"}),
        &resolver,
        Some(&["read:external-app".to_string()]),
    );
    assert_eq!(direct["path"], "external-app/Cargo.toml");

    let normalized = normalize_delegated_resource_paths(
        "read_file",
        &input,
        root.path(),
        &resolver,
        Some(&["read:external-app".to_string()]),
    )
    .expect("normalize sole scoped read");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "external-app/Cargo.toml");
}

#[test]
fn ambiguous_or_existing_bare_read_path_is_never_retargeted() {
    let root = tempfile::tempdir().expect("workspace");
    for project in ["one", "two"] {
        std::fs::create_dir_all(root.path().join(project)).expect("project directory");
        std::fs::write(root.path().join(project).join("Cargo.toml"), project).expect("fixture");
    }
    std::fs::write(root.path().join("Cargo.toml"), "root").expect("root fixture");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let input = serde_json::json!({"path": "Cargo.toml"}).to_string();

    let normalized = normalize_delegated_resource_paths(
        "read_file",
        &input,
        root.path(),
        &resolver,
        Some(&["read:one".to_string(), "read:two".to_string()]),
    )
    .expect("preserve ambiguous path");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "Cargo.toml");
}

#[test]
fn whole_workspace_lease_bounds_to_workspace_but_never_escapes() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    std::fs::create_dir_all(root.path().join("evidence")).expect("evidence directory");
    let root_input = serde_json::json!({
        "pattern": "**/*.rs",
        "path": root.path(),
    })
    .to_string();
    let normalized = normalize_delegated_resource_paths(
        "glob_search",
        &root_input,
        root.path(),
        &resolver,
        Some(&["write:.".to_string()]),
    )
    .expect("normalize workspace root");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], ".");
    // `write:.` is a whole-workspace lease issued only to full-trust
    // Teams; it authorizes any path inside the workspace.
    assert!(resource_path_is_authorized(
        &resolver,
        "evidence/new-report.html",
        &["write:.".to_string()],
        true,
    ));
    // Traversal outside the workspace is never authorized, even under a
    // whole-workspace lease.
    assert!(!resource_path_is_authorized(
        &resolver,
        "../outside.html",
        &["write:.".to_string()],
        true,
    ));
}

#[test]
fn exact_new_artifact_scope_remains_narrow_and_writable() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    std::fs::create_dir_all(root.path().join("evidence")).expect("evidence directory");
    assert!(resource_path_is_authorized(
        &resolver,
        "evidence/report.html",
        &["write:evidence/report.html".to_string()],
        true,
    ));
    assert!(!resource_path_is_authorized(
        &resolver,
        "evidence/other.html",
        &["write:evidence/report.html".to_string()],
        true,
    ));
}

#[test]
fn absolute_escape_and_parent_traversal_remain_unauthorized() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let allowed = root.path().join("fixtures/target.txt");
    std::fs::create_dir_all(allowed.parent().expect("parent")).expect("scope directory");
    std::fs::write(&allowed, "before").expect("scope file");
    let outside = root.path().parent().expect("parent").join("outside.txt");
    let outside_input = serde_json::json!({"path": outside}).to_string();
    assert_eq!(
        normalize_delegated_resource_paths(
            "read_file",
            &outside_input,
            root.path(),
            &resolver,
            None,
        )
        .expect("unchanged outside input"),
        outside_input
    );
    assert!(!resource_path_is_authorized(
        &resolver,
        outside.to_string_lossy().as_ref(),
        &["read:fixtures/target.txt".into()],
        false,
    ));
    assert!(!resource_path_is_authorized(
        &resolver,
        "fixtures/../outside.txt",
        &["read:fixtures/target.txt".into()],
        false,
    ));
}

#[test]
fn permission_policy_uses_the_explicit_packet_ceiling() {
    let tools = BTreeSet::from(["write_file".to_string()]);
    let policy = permission_policy(None, PermissionMode::WorkspaceWrite, &tools);
    assert_eq!(policy.active_mode(), PermissionMode::WorkspaceWrite);
    assert_eq!(
        policy.required_mode_for("write_file"),
        PermissionMode::WorkspaceWrite
    );
}

#[test]
fn sandboxed_process_requires_and_accepts_only_a_whole_workspace_read_lease() {
    let root = tempfile::tempdir().expect("scoped workspace");
    let build = |scopes: Vec<String>| ScopedRuntimeToolExecutor {
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["execute_code".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: Some(crate::execution_core::graph::ExecutionCommitService::new(
            Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("effect ledger")),
        )),
        resource_scopes: Some(scopes),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let input = r#"{"language":"python","code":"print(1)"}"#;

    let whole_workspace = build(vec!["read:.".to_string()]);
    assert!(whole_workspace.owns_durable_tool_effect("execute_code"));
    assert!(!whole_workspace.owns_durable_tool_effect("team_board"));
    whole_workspace
        .enforce_resource_ceiling("execute_code", input)
        .expect("whole-workspace read lease admits the read-only sandbox");
    assert!(build(vec!["read:src".to_string()])
        .enforce_resource_ceiling("execute_code", input)
        .is_err());
}

#[tokio::test]
async fn team_tool_boundary_enforces_the_exact_focus_scope() {
    let root = tempfile::tempdir().expect("scoped workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("runtime scope");
    std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "checked").expect("runtime file");
    std::fs::create_dir_all(root.path().join("crates/gateway")).expect("gateway scope");
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from([
            "read_file".to_string(),
            "grep_search".to_string(),
            "glob_search".to_string(),
            "context_retrieve".to_string(),
        ]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:crates/runtime".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    executor
        .enforce_resource_ceiling("read_file", r#"{"path":"crates/runtime/src/lib.rs"}"#)
        .expect("in-scope read");
    assert!(executor
        .enforce_resource_ceiling("read_file", r#"{"path":"crates/gateway/src/lib.rs"}"#,)
        .is_err());
    assert!(executor
        .enforce_resource_ceiling("read_file", r#"{"path":"../secret"}"#)
        .is_err());
    assert!(executor
        .enforce_resource_ceiling("grep_search", r#"{"pattern":"unsafe"}"#)
        .is_err());
    executor
        .enforce_resource_ceiling(
            "context_retrieve",
            r#"{"source":"session_history","scope":"current"}"#,
        )
        .expect("Runtime-bound context retrieval is not a filesystem escape");

    let normalized = normalize_delegated_resource_paths(
        "glob_search",
        r#"{"pattern":"crates/runtime/**/*.rs","path":"."}"#,
        root.path(),
        &executor.path_identity_resolver,
        executor.resource_scopes.as_deref(),
    )
    .expect("bounded glob normalization");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&normalized).expect("normalized input"),
        serde_json::json!({"pattern":"**/*.rs","path":"crates/runtime"})
    );
    executor
        .enforce_resource_ceiling("glob_search", &normalized)
        .expect("the narrowed glob remains inside the exact focus scope");

    let outside_glob = normalize_delegated_resource_paths(
        "glob_search",
        r#"{"pattern":"crates/gateway/**/*.rs","path":"."}"#,
        root.path(),
        &executor.path_identity_resolver,
        executor.resource_scopes.as_deref(),
    )
    .expect("outside glob stays representable");
    assert!(executor
        .enforce_resource_ceiling("glob_search", &outside_glob)
        .is_err());

    let descriptor = test_tool_descriptor_for_input(
        "read_file",
        &serde_json::json!({"path": "crates/runtime/src/lib.rs"}),
    )
    .expect("read descriptor");
    let authorization = harness_contract::tool::ToolExecutionAuthorization {
        request_id: "absolute-read".into(),
        tool_id: "read_file".into(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        policy_revision: 1,
        scope: descriptor.scopes[0].clone(),
        authorization_lease: harness_contract::policy::AuthorizationLease {
            lease_id: "permission:read_only".into(),
            principal_id: "test-agent".into(),
            parent_lease_id: None,
            capability: "read_file".into(),
            scopes: descriptor.scopes.clone(),
            ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: "absolute-read".into(),
            policy_revision: 1,
            effect_descriptor_hash: descriptor.descriptor_hash.clone(),
            signature: "test-signature".into(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        },
        timeout_lease: "timeout:30".into(),
        idempotency_key: None,
    };
    let absolute_input = serde_json::json!({
        "path": root.path().join("crates/runtime/src/lib.rs"),
    })
    .to_string();
    executor
        .execute_authorized_output(&authorization, "read_file", &absolute_input)
        .await
        .expect("workspace-internal absolute read is normalized and executed");
    let receipts = executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].paths, ["crates/runtime/src/lib.rs"]);
    assert!(
            receipts[0].observed_evidence.is_empty(),
            "a successful delegated read with unstructured adapter output must not be promoted into exact-content evidence"
        );
}

#[tokio::test]
async fn absolute_path_authorization_and_execution_share_the_normalized_descriptor() {
    let root = tempfile::tempdir().expect("scoped workspace");
    let target = root.path().join("fixtures/target.txt");
    std::fs::create_dir_all(target.parent().expect("target parent")).expect("scope directory");
    std::fs::write(&target, "checked").expect("scope file");
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(InputSensitiveRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:fixtures/target.txt".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let absolute_value = serde_json::json!({"path": target});
    let descriptor = executor
        .registered_tool_effect("read_file", &absolute_value)
        .expect("normalized effect descriptor");
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
        &descriptor,
        &absolute_value.to_string(),
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::ReadOnly),
            "absolute-agent-read",
            test_authorization_lease(&descriptor, PermissionMode::ReadOnly, "absolute-agent-read"),
            30,
        )
        .expect("normalized read authorization")
        .authorization;

    assert_eq!(
        executor
            .execute_authorized_output(&authorization, "read_file", &absolute_value.to_string(),)
            .await
            .expect("same normalized descriptor must remain current"),
        harness_contract::context::ToolOutputDraft::bounded_inline("authorized:read_file")
    );
    let receipts = executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(receipts[0].paths, ["fixtures/target.txt"]);
}

#[cfg(unix)]
#[test]
fn team_tool_boundary_rejects_symlink_escape_for_existing_and_new_targets() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().expect("scoped workspace");
    let outside = tempfile::tempdir().expect("outside workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime")).expect("runtime scope");
    std::fs::write(outside.path().join("secret.txt"), "secret").expect("outside fixture");
    symlink(outside.path(), root.path().join("crates/runtime/escape")).expect("workspace symlink");
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string(), "write_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec![
            "read:crates/runtime".to_string(),
            "write:crates/runtime".to_string(),
        ]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    assert!(executor
        .enforce_resource_ceiling(
            "read_file",
            r#"{"path":"crates/runtime/escape/secret.txt"}"#,
        )
        .is_err());
    assert!(executor
        .enforce_resource_ceiling(
            "write_file",
            r#"{"path":"crates/runtime/escape/new.txt","content":"denied"}"#,
        )
        .is_err());
}

#[tokio::test]
async fn scoped_executor_advertises_only_packet_authorized_tools() {
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(NoopRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string(), "grep_search".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: None,
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    assert!(executor.has_registered_tools());
    assert_eq!(
        executor.available_tool_names(),
        vec![
            "tool_search".to_string(),
            "grep_search".to_string(),
            "read_file".to_string(),
        ]
    );
    assert!(executor.classify_tool_safety("read_file", "{}").is_some());
    assert!(executor.classify_tool_safety("write_file", "{}").is_none());
    let discovery: harness_contract::tool::ToolDiscoveryReceipt = serde_json::from_str(
        &executor
            .execute_output("tool_search", r#"{"query":"read"}"#)
            .await
            .expect("bootstrap search should return the canonical receipt")
            .model_text(),
    )
    .expect("canonical discovery receipt");
    assert_eq!(discovery.query, "read");
    assert_eq!(
        discovery.activation_candidates,
        vec!["grep_search", "read_file"]
    );
    assert!(executor.has_tool("checkpoint_create"));
    assert!(!executor
        .available_tool_names()
        .contains(&"checkpoint_create".to_string()));
    assert!(executor
        .execute_output("checkpoint_create", r#"{"label":"model"}"#)
        .await
        .is_err());
}

#[tokio::test]
async fn scoped_executor_routes_hidden_checkpoint_for_runtime_guard_only() {
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec![
            "read:README.md".to_string(),
            "write:fixtures/target.txt".to_string(),
        ]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let descriptor = executor
        .registered_tool_effect("checkpoint_create", &serde_json::json!({"label": "guard"}))
        .expect("Runtime guard must see the hidden checkpoint descriptor");
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
        &descriptor,
        r#"{"label":"guard"}"#,
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::WorkspaceWrite),
            "agent-checkpoint-test",
            test_authorization_lease(
                &descriptor,
                PermissionMode::WorkspaceWrite,
                "agent-checkpoint-test",
            ),
            30,
        )
        .expect("Runtime should authorize its internal checkpoint")
        .authorization;

    assert_eq!(
        executor
            .execute_authorized_output(&authorization, "checkpoint_create", r#"{"label":"guard"}"#,)
            .await
            .expect("hidden checkpoint should reach the pinned Runtime host"),
        harness_contract::context::ToolOutputDraft::bounded_inline("authorized:checkpoint_create")
    );
    assert!(executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_empty());
    assert_eq!(
        executor
            .internal_checkpoint_input(serde_json::json!({
                "label": "guard",
                "paths": ["model-must-not-control-this"]
            }))
            .expect("bounded checkpoint input")["paths"],
        serde_json::json!(["fixtures/target.txt"])
    );
}

#[test]
fn root_write_scope_compiles_to_the_checkpoint_whole_workspace_form() {
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(NoopRuntimeExecutionHost),
        allowed_tools: BTreeSet::new(),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::WorkspaceWriteSandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["write:.".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    let input = executor
        .internal_checkpoint_input(serde_json::json!({
            "label": "guard",
            "paths": ["model-must-not-control-this"]
        }))
        .expect("whole-workspace checkpoint input");
    assert_eq!(input.get("label"), Some(&serde_json::json!("guard")));
    assert!(input.get("paths").is_none());
}

#[test]
fn structured_agent_output_accepts_fenced_json_without_trusting_prose() {
    let exact = r#"{"implementation":"done","source_verification":"receipt"}"#;
    let fenced = format!("```json\n{exact}\n```\n\nHuman-readable evidence summary.");

    assert_eq!(
        structured_agent_output(exact),
        structured_agent_output(&fenced)
    );
    assert_eq!(
        structured_agent_output(&fenced).and_then(|object| object.get("implementation").cloned()),
        Some(serde_json::Value::String("done".to_string()))
    );
    assert!(structured_agent_output("implementation completed in prose").is_none());
    assert!(structured_agent_output(r#"prefix {"unrelated":"claim"} suffix"#).is_none());
    assert!(structured_agent_output(r#"{"unrelated":"claim"}"#).is_none());
}

#[test]
fn structured_agent_output_tolerates_safe_provider_shape_drift() {
    let wrapped = r#"{"output":{"Conclusion":"done","证据":["tool://read"]}}"#;
    let output = structured_agent_output(wrapped).expect("known wrapped contract");
    assert_eq!(output["summary"], "done");
    assert_eq!(output["evidence"][0], "tool://read");

    let encoded = r#"{"data":"{\"finding\":\"one finding\",\"gaps\":[]}"}"#;
    let output = structured_agent_output(encoded).expect("encoded contract object");
    assert_eq!(output["findings"], "one finding");
    assert_eq!(output["unresolved"], serde_json::json!([]));

    let localized = "### 总结\n完成读取。\n\n**风险**\n无。";
    let output = structured_agent_output(localized).expect("localized headings");
    assert_eq!(output["summary"], "完成读取。");
    assert_eq!(output["risks"], "无。");

    let trailing = "{\"findings\":\"verified\",\"risks\":[],}";
    let output = structured_agent_output(trailing).expect("trailing comma repair");
    assert_eq!(output["findings"], "verified");
    assert_eq!(output["risks"], serde_json::json!([]));

    let labeled = "Summary: verified result\nRisks: none identified";
    let output = structured_agent_output(labeled).expect("exact labeled contract");
    assert_eq!(output["summary"], "verified result");
    assert_eq!(output["risks"], "none identified");

    let legacy = "**Field: retired_acceptance_field**\nACCEPTED";
    assert!(
        structured_agent_output(legacy).is_none(),
        "retired presentation labels cannot become acceptance evidence"
    );

    let current =
        "## Key Decisions\nKeep the fenced Program binding.\n\n## Unresolved Or Risks\nNone.";
    let output = structured_agent_output(current).expect("current Team fields");
    assert_eq!(output["key_decisions"], "Keep the fenced Program binding.");
    assert_eq!(output["unresolved_or_risks"], "None.");

    assert!(structured_agent_output(r#"{"output":{"unrelated":"claim"}}"#).is_none());
}

#[test]
fn structured_agent_output_normalizes_only_exact_contract_headings() {
    let markdown =
        "intro\n\n## Review\nverified from fresh receipts\n\n## Risks\nNone identified.\n";
    let output = structured_agent_output(markdown).expect("heading contract");
    assert_eq!(output["review"], "verified from fresh receipts");
    assert_eq!(output["risks"], "None identified.");
    assert!(structured_agent_output("Review complete; no risks.").is_none());

    let quoted_upstream_then_terminal = concat!(
        "upstream: {\"implementation\":\"done\",\"source_verification\":\"old\"}\n",
        "terminal: {\"review\":\"verified\",\"risks\":\"none\"}"
    );
    let output = structured_agent_output(quoted_upstream_then_terminal)
        .expect("last embedded contract object");
    assert!(output.get("implementation").is_none());
    assert_eq!(output["review"], "verified");
}

#[test]
fn explicit_terminal_sections_outrank_embedded_contract_examples() {
    let report = concat!(
        "# Reviewer terminal report\n\n",
        "## findings\n",
        "The reviewed parser accepts examples such as `",
        "{\"evidence\":\"embedded source example\",\"review\":\"not the terminal\"}",
        "`, but the source example is not this report's contract.\n\n",
        "## evidence\n",
        "tool://runtime-read-receipt\n\n",
        "## summary\n",
        "Independent review completed with no contradiction.\n\n",
        "## unresolved\n",
        "[]\n",
    );

    let output = structured_agent_output(report).expect("explicit terminal contract");
    assert_eq!(
        output["summary"],
        "Independent review completed with no contradiction."
    );
    assert_eq!(output["unresolved"], "[]");
    assert!(output["findings"]
        .as_str()
        .is_some_and(|value| value.contains("embedded source example")));
    assert_ne!(output["evidence"], "embedded source example");
}

#[test]
fn disclosure_fields_share_explicit_empty_list_semantics() {
    for field in ["risks", "unresolved", "unresolved_or_risks"] {
        assert!(structured_contract_field_materialized(
            field,
            Some(&serde_json::json!([])),
        ));
        assert!(!structured_contract_field_materialized(field, None));
        assert!(!structured_contract_field_materialized(
            field,
            Some(&serde_json::Value::Null),
        ));
        assert!(!structured_contract_field_materialized(
            field,
            Some(&serde_json::json!("")),
        ));
    }
}

#[test]
fn declared_custom_artifact_accepts_exact_json_or_markdown_only() {
    let required = vec![
        "applications_survey".to_string(),
        "evidence".to_string(),
        "summary".to_string(),
    ];
    let markdown = concat!(
        "## applications_survey\n",
        "Survey body.\n\n",
        "### Vision / 3D\n",
        "Nested details remain part of the custom artifact.\n\n",
        "## evidence\n",
        "tool://read-1\n\n",
        "## summary\n",
        "Survey complete.\n",
    );
    let output = structured_agent_output_for_fields(markdown, &required)
        .expect("declared custom Markdown artifact");
    assert!(output["applications_survey"]
        .as_str()
        .is_some_and(|value| value.contains("### Vision / 3D")));
    assert_eq!(output["evidence"], "tool://read-1");
    assert_eq!(output["summary"], "Survey complete.");

    let json = r#"{"applications_survey":{"vision":"verified"},"summary":"done"}"#;
    let output =
        structured_agent_output_for_fields(json, &required).expect("declared custom JSON artifact");
    assert_eq!(output["applications_survey"]["vision"], "verified");
    assert_eq!(output["summary"], "done");

    assert!(
        structured_agent_output_for_fields("## undeclared_artifact\nself reported", &required,)
            .is_none()
    );
    assert!(structured_agent_output(r#"{"applications_survey":"self reported"}"#).is_none());
}

#[test]
fn verified_narrative_terminal_accepts_prose_without_accepting_tool_markup() {
    use harness_contract::team::TeamStructuredOutputField;

    let prose = normalized_narrative_terminal_body(
        "Cargo.toml declares the workspace package metadata.",
        &[TeamStructuredOutputField::Findings],
    )
    .expect("verified bounded prose should be a findings carrier");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&prose).expect("normalized JSON")["findings"],
        "Cargo.toml declares the workspace package metadata."
    );
    assert!(
        normalized_narrative_terminal_body(
            "Both researchers confirmed the workspace metadata.",
            &[
                TeamStructuredOutputField::Summary,
                TeamStructuredOutputField::Unresolved,
            ],
        )
        .is_none(),
        "Runtime must not invent an unresolved conclusion that the Agent omitted"
    );
    let mixed_contract = normalized_narrative_terminal_body(
        "Terminal review completed from three fresh source receipts.\n\n{\"unresolved\":[]}",
        &[
            TeamStructuredOutputField::Review,
            TeamStructuredOutputField::Unresolved,
        ],
    )
    .expect("an explicit unresolved declaration may coexist with a Runtime-normalized review");
    let mixed_output =
        serde_json::from_str::<serde_json::Value>(&mixed_contract).expect("normalized JSON");
    assert_eq!(mixed_output["unresolved"], serde_json::json!([]));
    assert!(structured_field_materialized(
        TeamStructuredOutputField::Unresolved,
        mixed_output.get("unresolved"),
    ));
    assert!(mixed_output["review"]
        .as_str()
        .is_some_and(|review| review.contains("Terminal review completed")));
    assert!(normalized_narrative_terminal_body(
        "<synthesized_terminal evidence_committed=1 />",
        &[TeamStructuredOutputField::Findings],
    )
    .is_none());
    assert!(normalized_narrative_terminal_body(
        "<tool_call>read_file</tool_call>",
        &[TeamStructuredOutputField::Findings],
    )
    .is_none());
    assert!(
        normalized_narrative_terminal_body("reviewed", &[TeamStructuredOutputField::Review],)
            .is_some(),
        "a verified review may use ordinary terminal prose"
    );
}

#[test]
fn verified_narrative_terminal_accepts_technical_prose_but_not_risk_declarations() {
    use harness_contract::team::TeamStructuredOutputField;

    let prose = "Updated the parser and verified the changed file with a fresh read.";
    let normalized = normalized_narrative_terminal_body(
        prose,
        &[
            TeamStructuredOutputField::Implementation,
            TeamStructuredOutputField::SourceVerification,
            TeamStructuredOutputField::Review,
        ],
    )
    .expect("receipt-verified technical prose should not require JSON syntax");
    let output = serde_json::from_str::<serde_json::Value>(&normalized).expect("JSON carrier");
    assert_eq!(output["implementation"], prose);
    assert_eq!(output["source_verification"], prose);
    assert_eq!(output["review"], prose);

    assert!(
        normalized_narrative_terminal_body(prose, &[TeamStructuredOutputField::Risks],).is_none(),
        "Runtime must not infer that risks were considered"
    );
    assert!(
        normalized_narrative_terminal_body(prose, &[TeamStructuredOutputField::UnresolvedOrRisks],)
            .is_none(),
        "Runtime must not infer unresolved work from generic prose"
    );
}

#[test]
fn terminal_structured_acceptance_is_single_pass_and_never_invents_missing_fields() {
    let terminal = r#"{"implementation":"done"}"#;
    let first = structured_agent_output(terminal).expect("deterministic terminal JSON");
    let second = structured_agent_output(terminal).expect("repeat deterministic parse");

    assert_eq!(first, second);
    assert_eq!(first["implementation"], "done");
    assert!(first.get("source_verification").is_none());
    assert!(first.get("review").is_none());
    assert!(structured_agent_output("implementation completed in prose").is_none());
}

#[tokio::test]
async fn scoped_executor_propagates_runtime_authorization_for_normal_agent_tools() {
    let executor = ScopedRuntimeToolExecutor {
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: None,
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let descriptor = executor
        .registered_tool_effect("read_file", &serde_json::json!({"path": "README.md"}))
        .expect("allow-listed delegated tool must describe its effect");
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
        &descriptor,
        r#"{"path":"README.md"}"#,
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::ReadOnly),
            "agent-test",
            test_authorization_lease(&descriptor, PermissionMode::ReadOnly, "agent-test"),
            30,
        )
        .expect("read tool should be authorized")
        .authorization;
    assert_eq!(
        executor
            .execute_authorized_output(&authorization, "read_file", r#"{"path":"README.md"}"#)
            .await
            .expect("authorized tool should execute"),
        harness_contract::context::ToolOutputDraft::bounded_inline("authorized:read_file")
    );
    assert!(executor
        .execute_output("read_file", r#"{"path":"README.md"}"#)
        .await
        .is_err());
    assert!(executor
        .execute_authorized_output(&authorization, "write_file", r#"{"path":"README.md"}"#)
        .await
        .is_err());
}

#[test]
fn durable_audits_are_promoted_to_agent_evidence_refs() {
    let packet = AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None, "agent", "run", "task", "session", "mission", None, "graph", "node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "inspect".into(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        requires_managed_collaboration_escalation: false,
        acceptance: Vec::new(),
        team_role_identity: None,
        team_role: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: vec![harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed("upstream", "frame"),
            "sha256:frame",
            1,
            "text/plain",
            "artifact://art_worker_fixture_1",
            "session:session",
        )],
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget",
            "agent",
            "agent",
            1,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "key".into(),
    };
    let tool_access = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("tool", "tool-1"),
        "sha256:tool",
        1,
        "text/plain",
        "artifact://art_worker_fixture_2",
        "session:session",
    );
    let audits = vec![harness_contract::context::EvidenceAuditProjection {
        evidence_ref: tool_access.evidence_ref.clone(),
        content_kind: harness_contract::context::EvidenceContentKind::Text,
        raw_tokens: 1,
        receipt_tokens: 1,
        omitted_tokens: 0,
        raw_available: true,
        access: Some(tool_access),
    }];

    assert_eq!(
        agent_evidence_refs(&packet, &audits, &[])
            .into_iter()
            .map(|reference| reference.evidence_ref.id)
            .collect::<Vec<_>>(),
        vec!["tool-1".to_string(), "frame".to_string()]
    );
}

#[test]
fn exact_acceptance_evidence_requires_a_complete_model_receipt() {
    use harness_contract::context::{
        EvidenceCoverageKind, EvidenceObligation, EvidenceObligationKind,
        EvidenceObservationRequirement, EvidenceTargetIdentity, ObservedEvidence,
        ObservedEvidenceProvenance, ProviderModelObservationAttestation, RequiredAcceptance,
        WorkspaceAccessMode, WorkspaceObjectKind, WorkspacePathIdentity, WorkspaceScopeIdentity,
    };

    let access = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("tool", "exact-read"),
        "sha256:exact-read",
        42,
        "application/json",
        "artifact://art_worker_exact_read",
        "session:session",
    );
    let observed = ObservedEvidence {
        obligation_id: "read:src/lib.rs".to_string(),
        target: EvidenceTargetIdentity::Workspace {
            scope: WorkspaceScopeIdentity {
                access_mode: WorkspaceAccessMode::Read,
                path: WorkspacePathIdentity {
                    workspace_id: "workspace".to_string(),
                    repository_id: "repository".to_string(),
                    workspace_relative_path: "src/lib.rs".to_string(),
                    repository_relative_path: "src/lib.rs".to_string(),
                    object_kind: WorkspaceObjectKind::File,
                    observed_revision_or_digest: Some("d".repeat(64)),
                },
                coverage: EvidenceCoverageKind::ExactContent,
            },
        },
        observed_at_sequence: 1,
        tool_name: "read_file".to_string(),
        provenance: ObservedEvidenceProvenance::FreshExecution,
        evidence_ref: Some(access.clone()),
        model_observation: None,
        workspace_prior_state: None,
    };
    let receipt = ScopedToolExecutionReceipt {
        sequence: 1,
        provider_invocation_id: Some("tool-call-1".to_string()),
        tool_name: "read_file".to_string(),
        effect_kind: harness_contract::tool::ToolEffectKind::Read,
        resource_scopes: vec!["read:src/lib.rs".to_string()],
        paths: vec!["src/lib.rs".to_string()],
        prior_states: BTreeMap::new(),
        after_digests: BTreeMap::new(),
        observed_bytes: BTreeMap::new(),
        observed_evidence: vec![observed.clone()],
    };
    let required = RequiredAcceptance {
        criteria: Vec::new(),
        evidence_obligations: vec![EvidenceObligation {
            obligation_id: "required-read".to_string(),
            kind: EvidenceObligationKind::ContentRead,
            target: observed.target.clone(),
            observation_requirement: EvidenceObservationRequirement::ProviderModel,
        }],
    };
    let complete = ProviderModelObservationAttestation {
        provider_invocation_id: "tool-call-1".to_string(),
        obligation_ids: vec!["required-read".to_string()],
        raw_ref: access.evidence_ref.clone(),
        model_receipt_sha256: format!("sha256:{}", "a".repeat(64)),
        raw_tokens: 42,
        receipt_tokens: 42,
        omitted_tokens: 0,
        complete: true,
        provider_request_sequence: 2,
        provider_attempt: 1,
        model: "qwen3.8-max".to_string(),
    };

    let promoted = model_observed_evidence(
        &required,
        std::slice::from_ref(&complete),
        std::slice::from_ref(&receipt),
    );
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].model_observation.as_ref(), Some(&complete));
    assert!(crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
        &required.evidence_obligations[0],
        &promoted,
    ));

    let mut omitted = complete.clone();
    omitted.omitted_tokens = 1;
    omitted.complete = false;
    let incomplete = model_observed_evidence(&required, &[omitted], &[receipt.clone()]);
    assert!(!crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
        &required.evidence_obligations[0],
        &incomplete,
    ));

    let mut unrelated = complete;
    unrelated.provider_invocation_id = "tool-call-2".to_string();
    let uncorrelated = model_observed_evidence(&required, &[unrelated], &[receipt]);
    assert_eq!(uncorrelated, vec![observed]);
}

#[test]
fn delegated_child_session_inherits_the_runtime_services_workspace() {
    let workspace = std::path::Path::new("/workspace/project");
    let session = delegated_child_session("parent-session", "model", workspace);

    assert_eq!(session.session_id, "parent-session");
    assert_eq!(session.model.as_deref(), Some("model"));
    assert_eq!(session.workspace_root(), Some(workspace));
}

#[tokio::test]
async fn send_input_enters_the_live_child_turn_inbox() {
    let worker = InProcessAgentWorker::new(Weak::new());
    let stream = crate::SessionInputStream::new("child-session");
    stream.set_active_turn(Some(TurnId::from_string("child-turn")));
    worker.active_runs.lock().unwrap().insert(
        "run-1".into(),
        ActiveInProcessRun {
            cancellation: crate::CancellationToken::new(),
            session_id: "child-session".into(),
            input_stream: stream.clone(),
            completion: Arc::new(tokio::sync::Notify::new()),
            completed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        },
    );
    worker
        .command(
            &AgentRunHandle {
                run_id: "run-1".into(),
                agent_id: "agent-1".into(),
                backend: AgentBackendKind::InProcess,
                revision: 1,
                status: harness_contract::agent::AgentStatus::Running,
            },
            &AgentCommandRequest {
                command_id: "input-1".into(),
                agent_id: "agent-1".into(),
                expected_revision: 1,
                command: AgentCommand::SendInput,
                input: Some(AgentInput::UserSupplement("use the new requirement".into())),
            },
        )
        .await
        .expect("input accepted");
    let inbox = stream.inbox_snapshot(Some(TurnId::from_string("child-turn")));
    assert_eq!(inbox.items.len(), 1);
    assert_eq!(inbox.items[0].content_preview, "use the new requirement");
    assert!(worker.capabilities().supports_input);
    assert!(!worker.capabilities().supports_pause);
}

#[tokio::test]
async fn cancel_waits_for_cleanup_and_completed_tombstone_is_race_safe() {
    let worker = Arc::new(InProcessAgentWorker::new(Weak::new()));
    let cancellation = crate::CancellationToken::new();
    let completion = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    worker.active_runs.lock().unwrap().insert(
        "run-cancel".into(),
        ActiveInProcessRun {
            cancellation: cancellation.clone(),
            session_id: "child-session".into(),
            input_stream: crate::SessionInputStream::new("child-session"),
            completion: Arc::clone(&completion),
            completed: Arc::clone(&completed),
        },
    );
    let cleanup = ActiveRunCleanup {
        worker: worker.as_ref(),
        run_id: "run-cancel".into(),
        completion,
        completed,
    };
    let handle = AgentRunHandle {
        run_id: "run-cancel".into(),
        agent_id: "agent-cancel".into(),
        backend: AgentBackendKind::InProcess,
        revision: 1,
        status: harness_contract::agent::AgentStatus::Running,
    };
    let request = AgentCommandRequest {
        command_id: "cancel-1".into(),
        agent_id: "agent-cancel".into(),
        expected_revision: 1,
        command: AgentCommand::Cancel,
        input: None,
    };
    let cancel = {
        let worker = Arc::clone(&worker);
        let handle = handle.clone();
        let request = request.clone();
        tokio::spawn(async move { worker.command(&handle, &request).await })
    };
    cancellation.cancelled().await;
    drop(cleanup);
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel)
        .await
        .expect("cancel returns after cleanup")
        .expect("cancel task joins")
        .expect("cancel is accepted");
    assert!(worker.active_runs.lock().unwrap().is_empty());
    assert!(worker.pending_cancellations.lock().unwrap().is_empty());
    assert!(worker.run_completed("run-cancel"));

    // A command arriving in the just-completed/no-active window must use
    // the bounded tombstone instead of waiting ten seconds as if the run
    // had not registered yet.
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        worker.command(&handle, &request),
    )
    .await
    .expect("completed tombstone resolves cancellation")
    .expect("completed cancellation is idempotent");
    assert!(worker.pending_cancellations.lock().unwrap().is_empty());

    // Dropping a command future while it is waiting for an active run (or
    // immediately after observing a completion tombstone) must also
    // release its pending entry; no worker cleanup may still be available
    // to do that on its behalf.
    let aborted_token = crate::CancellationToken::new();
    let aborted_completion = Arc::new(tokio::sync::Notify::new());
    let aborted_completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    worker.active_runs.lock().unwrap().insert(
        "run-aborted-command".into(),
        ActiveInProcessRun {
            cancellation: aborted_token.clone(),
            session_id: "child-session".into(),
            input_stream: crate::SessionInputStream::new("child-session"),
            completion: Arc::clone(&aborted_completion),
            completed: Arc::clone(&aborted_completed),
        },
    );
    let aborted_cleanup = ActiveRunCleanup {
        worker: worker.as_ref(),
        run_id: "run-aborted-command".into(),
        completion: aborted_completion,
        completed: aborted_completed,
    };
    let aborted_handle = AgentRunHandle {
        run_id: "run-aborted-command".into(),
        agent_id: "agent-aborted-command".into(),
        ..handle.clone()
    };
    let aborted = {
        let worker = Arc::clone(&worker);
        let request = request.clone();
        tokio::spawn(async move { worker.command(&aborted_handle, &request).await })
    };
    aborted_token.cancelled().await;
    aborted.abort();
    let _ = aborted.await;
    assert!(
        worker.pending_cancellations.lock().unwrap().is_empty(),
        "PendingCancellationOwner must clean a dropped command future"
    );
    drop(aborted_cleanup);

    worker.record_completed_run("run-completed-abort-window");
    worker
        .pending_cancellations
        .lock()
        .unwrap()
        .insert("run-completed-abort-window".into());
    let completed_window_owner = PendingCancellationOwner {
        pending: &worker.pending_cancellations,
        run_id: "run-completed-abort-window".into(),
    };
    drop(completed_window_owner);
    assert!(
        worker.pending_cancellations.lock().unwrap().is_empty(),
        "an abort between pending insertion and tombstone inspection must be leak-free"
    );
}

#[test]
fn blocked_child_turn_is_not_relabelled_as_completed_agent_work() {
    let (status, failure) = agent_terminal_outcome(
        harness_contract::goal::GoalCompletion::Partial,
        "provider path exhausted",
    );
    assert_eq!(status, AgentTerminalStatus::Blocked);
    assert_eq!(failure.as_deref(), Some("provider path exhausted"));
}

#[test]
fn delegated_prompt_rejects_simulated_tool_markup() {
    let mut packet = AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            "agent",
            "run",
            "task",
            "session",
            "mission",
            Some("team"),
            "graph",
            "node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "inspect source".into(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        requires_managed_collaboration_escalation: false,
        acceptance: Vec::new(),
        team_role_identity: None,
        team_role: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: Vec::new(),
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget",
            "agent",
            "agent",
            1,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "key".into(),
    };
    let prompt = system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
    assert!(prompt.contains("Never write simulated tool syntax"));
    assert!(prompt.contains("If no native tool is authorized, answer directly"));
    assert!(!prompt.contains("## Runtime clock"));

    packet.resource_scopes = vec!["read:external-app".to_string()];
    let scoped_prompt = system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
    assert!(scoped_prompt.contains("scope read:project means project/Cargo.toml"));
    assert!(scoped_prompt.contains("never bare Cargo.toml"));

    packet.objective = "update fixtures/target.txt".into();
    packet.output_acceptance = vec![harness_contract::team::TeamAcceptanceRequirement {
        criterion: "implementation".to_string(),
        check: harness_contract::team::TeamAcceptanceCheck::WorkspaceChange {
            field: harness_contract::team::TeamStructuredOutputField::Implementation,
            scopes: vec!["write:fixtures/target.txt".to_string()],
        },
    }];
    let mutation_prompt = system_prompt(
        &packet,
        std::path::Path::new("/workspace"),
        &["read_file".into(), "write_file".into()],
    )
    .join("\n");
    assert!(mutation_prompt.contains("Read each target at most once before mutation"));
    assert!(mutation_prompt.contains("write:fixtures/target.txt"));
    assert!(mutation_prompt.contains("Repeated reads"));
    assert!(mutation_prompt.contains("Native structured output"));
    assert!(!mutation_prompt.contains("Return exactly one JSON object"));
}

#[test]
fn team_markdown_fragment_cache_is_digest_bound_and_counts_metrics() {
    let worker = InProcessAgentWorker::new(std::sync::Weak::new());
    let first = worker.cached_team_markdown_fragment("binding-a", "team-a", "# Team\n\nReview.");
    assert!(first[0].contains("binding digest team-a"));
    assert_eq!(worker.team_prompt_cache_stats().0, 0);
    assert_eq!(worker.team_prompt_cache_stats().1, 1);
    assert!(
        worker.team_prompt_cache_stats().2 > 0,
        "token increment is recorded"
    );

    let second = worker.cached_team_markdown_fragment("binding-a", "team-a", "# Team\n\nReview.");
    assert_eq!(first, second);
    assert_eq!(
        worker.team_prompt_cache_stats().0,
        1,
        "same digest pair is a cache hit"
    );
    assert_eq!(worker.team_prompt_cache_stats().1, 1);

    worker.cached_team_markdown_fragment("binding-a", "team-b", "# Team\n\nReview.");
    worker.cached_team_markdown_fragment("binding-b", "team-a", "# Team\n\nReview.");
    assert_eq!(
        worker.team_prompt_cache_stats().1,
        3,
        "any digest change rebuilds the prefix; no stale prefix is reused"
    );
}

#[test]
fn scoped_tool_effect_key_is_stable_across_worker_recovery() {
    let first = deterministic_scoped_tool_idempotency_key(
        "graph-1",
        "node-1",
        2,
        3,
        "write_file",
        r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
    );
    let recovered = deterministic_scoped_tool_idempotency_key(
        "graph-1",
        "node-1",
        2,
        3,
        "write_file",
        r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
    );
    let next_effect = deterministic_scoped_tool_idempotency_key(
        "graph-1",
        "node-1",
        2,
        4,
        "write_file",
        r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
    );

    assert_eq!(first, recovered);
    assert_ne!(first, next_effect);
    assert!(first.contains("agent-tool:graph-1:node-1:2:3:write_file:"));
}

#[test]
fn recovered_receipt_context_is_bounded_and_explicitly_fences_tools() {
    let receipt = crate::execution_core::graph::DurableAgentToolReceipt {
        sequence: 7,
        effect_kind: harness_contract::tool::ToolEffectKind::Write,
        authorized_scopes: vec!["write:src/lib.rs".to_string()],
        outcome: crate::RuntimeToolExecutionOutcome {
            tool_use_id: "tool-7".to_string(),
            tool_name: "write_file".to_string(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: crate::ToolSafetyCategory::WriteLocal,
            output: Some("committed output".to_string()),
            error: None,
            evidence_ref: "tool://receipt-7".to_string(),
            observed_evidence: Vec::new(),
        },
    };

    let prompt = recovered_agent_tool_receipt_prompt(&[receipt]).expect("recovery prompt");
    assert!(prompt.contains("committed output"));
    assert!(prompt.contains("Do not call tools"));
    assert!(prompt.contains("ToolHost receipts"));
}

#[test]
fn autonomous_proposer_selection_follows_team_topology_not_node_sort_order() {
    use harness_contract::execution_graph::{
        ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
    };

    let mut graph = ExecutionGraph::new("serial autonomous Team");
    for id in ["node", "a-successor", "z-final"] {
        let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
        node.id = id.to_string();
        graph.nodes.push(node);
    }
    graph.edges.push(ExecutionEdge {
        from: "node".to_string(),
        to: "a-successor".to_string(),
        kind: ExecutionEdgeKind::DependsOn,
    });
    graph.edges.push(ExecutionEdge {
        from: "a-successor".to_string(),
        to: "z-final".to_string(),
        kind: ExecutionEdgeKind::DependsOn,
    });

    assert_eq!(
        topological_agent_node_ids(&graph),
        vec!["node", "a-successor", "z-final"]
    );
    assert_eq!(
        designated_autonomous_proposer_nodes(&graph),
        vec!["node", "a-successor"]
    );
    let packet = test_agent_packet(Vec::new());
    assert!(missing_required_proposal_action(&graph, &packet, false).is_none());
    let action = missing_required_proposal_action(&graph, &packet, true)
        .expect("first topological Agent must propose");
    assert_eq!(action["action"], "propose_work");
    assert_eq!(
        action.pointer("/mutation_template/operation"),
        Some(&serde_json::Value::String("propose_work".to_string()))
    );

    let mut non_designated = packet.clone();
    non_designated.assignment.node_id = "z-final".to_string();
    assert!(missing_required_proposal_action(&graph, &non_designated, true).is_none());

    let mut work = harness_contract::execution_graph::ExecutionWorkContract::new(
        harness_contract::execution_graph::ExecutionWorkRole::CrossCheck,
    );
    work.proposed_by = Some(packet.agent_id().to_string());
    graph.autonomous_work.insert("work-1".to_string(), work);
    assert!(missing_required_proposal_action(&graph, &packet, true).is_none());
}

#[test]
fn autonomous_checkpoint_tool_overlay_is_minimal_and_action_specific() {
    let mut packet = test_agent_packet(Vec::new());
    packet.allowed_tools = vec![
        "tool_search".to_string(),
        "collaboration_control".to_string(),
        "team_board".to_string(),
        "read_file".to_string(),
        "write_file".to_string(),
    ];

    assert!(autonomy_checkpoint_tool_plan(&packet, false, false).is_empty());
    assert_eq!(
        autonomy_checkpoint_tool_plan(&packet, true, false),
        vec!["collaboration_control".to_string()]
    );
    let execution = autonomy_checkpoint_tool_plan(&packet, true, true);
    assert_eq!(
        execution,
        vec![
            "collaboration_control".to_string(),
            "team_board".to_string(),
            "read_file".to_string(),
            "write_file".to_string(),
        ]
    );
    assert!(!execution.iter().any(|tool| tool == "tool_search"));
}

#[test]
fn runtime_default_autonomous_proposal_is_identity_stable_and_parallel_safe() {
    use harness_contract::execution_graph::{
        ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionWorkRole,
    };

    let mut graph = ExecutionGraph::new("runtime autonomy default");
    graph.id = "team-graph:stable".to_string();
    graph.revision = 42;
    let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", "{}");
    node.id = "node".to_string();
    graph.nodes.push(node);
    let packet = test_agent_packet(Vec::new());

    let first = runtime_default_autonomous_proposal_request(&graph, &packet);
    graph.revision = 99;
    let after_parallel_commit = runtime_default_autonomous_proposal_request(&graph, &packet);
    assert_eq!(first.expected_revision, None);
    assert_eq!(after_parallel_commit.expected_revision, None);
    assert_eq!(first.expected_work_revision, Some(0));
    assert!(matches!(
        first.operation,
        crate::CollaborationControlOperation::ProposeWork
    ));
    let first_proposal = first.proposal.expect("Runtime default proposal");
    let later_proposal = after_parallel_commit
        .proposal
        .expect("same Runtime default proposal");
    assert_eq!(
        first_proposal.idempotency_key,
        later_proposal.idempotency_key
    );
    assert_eq!(
        first_proposal.idempotency_key,
        "autonomy:team-graph:stable:agent:follow-up-v1"
    );
    assert_eq!(first_proposal.role, ExecutionWorkRole::CrossCheck);
    assert_eq!(
        first_proposal.output_artifact_kinds,
        vec!["autonomous_cross_check".to_string()]
    );
}
