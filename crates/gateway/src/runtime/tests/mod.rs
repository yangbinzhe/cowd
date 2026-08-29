use super::*;

mod policy_transition;

#[test]
fn execution_policy_default_equality_includes_sandbox_posture() {
    let left = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        11,
        runtime::SessionExecutionPolicyOrigin::ConfigDefault,
    );
    let mut right = left.clone();
    assert!(execution_policy_defaults_match(&left, &right));
    right.sandbox_posture = harness_contract::policy::SandboxPosture::HostFullAccess;
    assert!(!execution_policy_defaults_match(&left, &right));
}
use crate::services::session_service::{
    presence::SessionPresenceLedger, repository::SessionRepository,
};
use model_protocol::provider_config::{ProviderConfig, ProvidersConfig};

#[test]
fn session_approval_control_parses_skip() {
    let skip =
        parse_session_approval_control("/skip approval-1 once").expect("skip command must parse");
    assert!(!skip.approved);
    assert!(skip.skip);
    assert_eq!(skip.approval_id.as_deref(), Some("approval-1"));
    assert_eq!(skip.scope, runtime::ApprovalGrantScope::Once);

    let approved = parse_session_approval_control("同意").expect("approve command");
    assert!(approved.approved);
    assert!(!approved.skip);
    assert!(!parse_session_approval_control("maybe").is_some());
}

fn test_bound_provider_registry() -> Arc<runtime::ProviderRegistry> {
    Arc::new(
        runtime::ProviderRegistry::new(ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                ProviderConfig {
                    name: "test".to_string(),
                    base_url: "http://127.0.0.1:9/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["test-model".to_string()],
                    protocol: Some("completions".to_string()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        })
        .expect("valid inert test provider registry"),
    )
}

fn test_runtime_service_with_services(
    active_sessions: Arc<ActiveSessionDirectory>,
    store: Arc<session::UnifiedSessionStore>,
    runtime_services: Arc<runtime::RuntimeServices>,
) -> RuntimeService {
    let projection_hub = crate::event_bus::SessionProjectionHub::new();
    let repository = Arc::new(SessionRepository::new(
        active_sessions.clone(),
        Some(Arc::clone(&store)),
        Arc::clone(&projection_hub),
    ));
    let presence = Arc::new(SessionPresenceLedger::new());
    let session_runtime_port =
        crate::session_runtime_data_port::GatewaySessionRuntimePort::new_for_test(
            repository, presence,
        );
    runtime_services
        .install_session_ports(
            session_runtime_port.clone(),
            session_runtime_port.clone(),
            session_runtime_port.clone(),
            session_runtime_port.clone(),
        )
        .expect("test Session runtime port");
    RuntimeService::new(
        active_sessions.clone(),
        Arc::new(SessionLeaseRegistry::default()),
        session_runtime_port,
        projection_hub,
        Instant::now(),
        None,
        Arc::new(runtime::ProviderRegistry::empty()),
        Arc::new(runtime::UpgradeCoordinator::new()),
        runtime_services,
    )
    .expect("test runtime service")
}

fn test_runtime_service(
    active_sessions: Arc<ActiveSessionDirectory>,
    store: Option<Arc<session::UnifiedSessionStore>>,
) -> RuntimeService {
    let store = store.unwrap_or_else(|| {
        Arc::new(session::UnifiedSessionStore::open_in_memory().expect("test session store"))
    });
    let runtime_services = runtime::RuntimeServices::in_memory().expect("test runtime services");
    test_runtime_service_with_services(active_sessions, store, runtime_services)
}

fn test_bound_runtime_service(
    active_sessions: Arc<ActiveSessionDirectory>,
    store: Arc<session::UnifiedSessionStore>,
    defaults: Option<(runtime::PermissionMode, runtime::ApprovalProfile)>,
) -> (Arc<RuntimeService>, Arc<crate::services::SessionService>) {
    let projection_hub = crate::event_bus::SessionProjectionHub::new();
    let repository = Arc::new(SessionRepository::new(
        Arc::clone(&active_sessions),
        Some(Arc::clone(&store)),
        Arc::clone(&projection_hub),
    ));
    let presence = Arc::new(SessionPresenceLedger::new());
    let session_runtime_port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
    let runtime_services = runtime::RuntimeServices::in_memory().expect("test runtime services");
    runtime_services
        .install_session_ports(
            session_runtime_port.clone(),
            session_runtime_port.clone(),
            session_runtime_port.clone(),
            session_runtime_port.clone(),
        )
        .expect("test Session runtime port");
    let mut service = RuntimeService::new(
        active_sessions,
        Arc::new(SessionLeaseRegistry::default()),
        session_runtime_port.clone(),
        projection_hub,
        Instant::now(),
        Some("test-model".to_string()),
        test_bound_provider_registry(),
        Arc::new(runtime::UpgradeCoordinator::new()),
        runtime_services,
    )
    .expect("test runtime service");
    if let Some((permission_mode, approval_profile)) = defaults {
        service = service
            .with_permission_mode(permission_mode)
            .with_approval_profile(approval_profile);
    }
    let service = Arc::new(service);
    let coordinator = Arc::new(
        crate::services::session_service::activation::SessionActivationCoordinator::new(
            Arc::clone(&service),
            repository,
            presence,
            Arc::new(runtime::session_lifecycle::SessionWorkingSetManager::new(
                runtime::session_lifecycle::SessionLifecycleConfig::default(),
            )),
            None,
            runtime::SessionRecoveryConfig::default(),
        ),
    );
    let session_service = Arc::new(crate::services::SessionService::new_unbound(
        Arc::clone(&service),
        coordinator,
    ));
    session_runtime_port
        .bind(&session_service)
        .expect("bind production-shaped Session service");
    (service, session_service)
}

#[tokio::test]
async fn activation_materializes_default_policy_without_reentrant_session_lock() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (_runtime, session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );

    let outcome = tokio::time::timeout(
        Duration::from_secs(2),
        session_service.ensure_surface_session(crate::services::EnsureSessionRequest::new(
            "activation-policy-materialization",
            Some("test-model".to_string()),
            crate::services::SessionSource::WebUi,
        )),
    )
    .await
    .expect("Session activation must not wait on its own exclusive gate")
    .expect("Session activation succeeds");

    assert!(outcome.created);
    let stored = store
        .get_session("activation-policy-materialization")
        .await
        .expect("load activated Session")
        .expect("activated Session is durable");
    assert!(stored_session_execution_policy(&stored).is_some());
}

#[tokio::test]
async fn session_execution_policy_persists_and_restores_permission_and_autonomy() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let active_sessions = Arc::new(ActiveSessionDirectory::default());
    let projection_hub = crate::event_bus::SessionProjectionHub::new();
    let repository = Arc::new(SessionRepository::new(
        Arc::clone(&active_sessions),
        Some(Arc::clone(&store)),
        Arc::clone(&projection_hub),
    ));
    let presence = Arc::new(SessionPresenceLedger::new());
    let session_runtime_port = crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
    let runtime_services = runtime::RuntimeServices::in_memory().expect("test runtime services");
    runtime_services
        .install_session_ports(
            session_runtime_port.clone(),
            session_runtime_port.clone(),
            session_runtime_port.clone(),
            session_runtime_port.clone(),
        )
        .expect("test Session runtime port");
    let service = Arc::new(
        RuntimeService::new(
            active_sessions,
            Arc::new(SessionLeaseRegistry::default()),
            session_runtime_port.clone(),
            projection_hub,
            Instant::now(),
            None,
            Arc::new(runtime::ProviderRegistry::empty()),
            Arc::new(runtime::UpgradeCoordinator::new()),
            runtime_services,
        )
        .expect("test runtime service"),
    );
    let coordinator = Arc::new(
        crate::services::session_service::activation::SessionActivationCoordinator::new(
            Arc::clone(&service),
            repository,
            presence,
            Arc::new(runtime::session_lifecycle::SessionWorkingSetManager::new(
                runtime::session_lifecycle::SessionLifecycleConfig::default(),
            )),
            None,
            runtime::SessionRecoveryConfig::default(),
        ),
    );
    let session_service = Arc::new(crate::services::SessionService::new_unbound(
        Arc::clone(&service),
        coordinator,
    ));
    session_runtime_port
        .bind(&session_service)
        .expect("bind production-shaped Session service");
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "policy-session".to_string(),
            platform: "test".to_string(),
            chat_id: "policy-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("test session");

    let receipt = service
        .set_session_execution_policy(
            "policy-session",
            runtime::AutonomyProfileId::Yolo,
            1,
            runtime::SessionExecutionPolicyOrigin::SessionExplicit,
        )
        .await
        .expect("persist execution policy");
    assert_eq!(
        receipt.policy.permission_mode,
        runtime::PermissionMode::DangerFullAccess
    );
    assert_eq!(
        receipt.policy.autonomy_profile,
        runtime::AutonomyProfileId::Yolo
    );
    assert_eq!(
        receipt.policy.approval_profile,
        runtime::ApprovalProfile::TrustAll
    );
    assert_eq!(receipt.policy.revision, 2);
    assert_eq!(
        service
            .session_execution_policy_value("policy-session")
            .await
            .unwrap()
            .policy,
        receipt.policy
    );

    let stored = store
        .get_session("policy-session")
        .await
        .expect("load persisted session")
        .expect("persisted session exists");
    let restored = session_execution_policy_from_record(
        &stored,
        &runtime::SessionExecutionPolicy::from_defaults(
            runtime::PermissionMode::WorkspaceWrite,
            runtime::ApprovalProfile::Balanced,
        ),
    );
    assert_eq!(restored.autonomy_profile, runtime::AutonomyProfileId::Yolo);
    assert_eq!(
        restored.permission_mode,
        runtime::PermissionMode::DangerFullAccess
    );
    assert_eq!(
        restored.approval_profile,
        runtime::ApprovalProfile::TrustAll
    );
    let metadata: serde_json::Value =
        serde_json::from_str(stored.metadata_json.as_deref().unwrap()).unwrap();
    assert_eq!(metadata["execution_policy"]["revision"], 2);

    let conflict = service
        .set_session_execution_policy(
            "policy-session",
            runtime::AutonomyProfileId::Cautious,
            1,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .unwrap_err();
    assert!(conflict.contains("session_execution_policy_revision_conflict"));
}

#[tokio::test]
async fn policy_transition_pins_started_attempts_and_fences_both_posture_directions() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let _runtime_services = service.runtime_services();
    let now = chrono::Utc::now().to_rfc3339();
    let host_policy = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Yolo,
        1,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    store
        .create_session(&session::SessionRecord {
            session_id: "policy-posture-transition".to_string(),
            platform: "test".to_string(),
            chat_id: "policy-posture-transition".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(serde_json::json!({ "execution_policy": host_policy }).to_string()),
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("policy Session");
    service.install_test_session_policy("policy-posture-transition", host_policy.clone());
    let live_control = service
        .sessions
        .session("policy-posture-transition")
        .and_then(|session| session.policy_control())
        .expect("test aggregate policy control");

    let (host_cancellation, host_guard) = service
        .install_active_turn_control(
            "turn-host",
            "policy-posture-transition",
            Some("execution-host".to_string()),
        )
        .expect("host attempt admission");
    {
        let registry = service.active_turns.state.lock().unwrap();
        let control = registry.controls.get("turn-host").unwrap();
        assert_eq!(control.policy_revision, 1);
        assert_eq!(
            control.requested_sandbox_posture,
            harness_contract::policy::SandboxPosture::HostFullAccess
        );
        assert_eq!(
            control.effective_sandbox_posture,
            harness_contract::policy::SandboxPosture::HostFullAccess
        );
    }
    let draining = service
        .set_session_execution_policy(
            "policy-posture-transition",
            runtime::AutonomyProfileId::Cautious,
            1,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .expect("host to read-only transition");
    assert_eq!(
        draining.transition.as_ref().unwrap().phase,
        harness_contract::policy::PolicyTransitionPhase::Draining
    );
    assert_eq!(draining.permission_revision, Some(1));
    assert!(!host_cancellation.is_cancelled());
    let fenced_error =
        match service.install_active_turn_control("turn-fenced", "policy-posture-transition", None)
        {
            Ok(_) => panic!("new admission must remain fenced while old revision drains"),
            Err(error) => error,
        };
    assert!(fenced_error.contains("session_policy_transition_in_progress"));
    assert_eq!(live_control.revision(), 1);
    drop(host_guard);

    let stable_read_only = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = service
                .session_execution_policy_value("policy-posture-transition")
                .await
                .unwrap();
            if response.transition.as_ref().is_none_or(|transition| {
                transition.phase == harness_contract::policy::PolicyTransitionPhase::Stable
            }) && response.permission_revision == Some(2)
            {
                break response;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("host to read-only transition settles");
    assert_eq!(
        stable_read_only.policy.sandbox_posture,
        harness_contract::policy::SandboxPosture::ReadOnlySandbox
    );
    assert_eq!(live_control.revision(), 2);

    let (sandbox_cancellation, sandbox_guard) = service
        .install_active_turn_control(
            "turn-sandbox",
            "policy-posture-transition",
            Some("execution-sandbox".to_string()),
        )
        .expect("read-only attempt admission");
    let back_to_host = service
        .set_session_execution_policy(
            "policy-posture-transition",
            runtime::AutonomyProfileId::Yolo,
            2,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .expect("read-only to host transition");
    assert_eq!(
        back_to_host.transition.as_ref().unwrap().phase,
        harness_contract::policy::PolicyTransitionPhase::Draining
    );
    assert!(!sandbox_cancellation.is_cancelled());
    assert_eq!(live_control.revision(), 2);
    drop(sandbox_guard);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if live_control.revision() == 3 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("read-only to host transition settles");
    assert_eq!(
        live_control.snapshot().sandbox_posture,
        harness_contract::policy::SandboxPosture::HostFullAccess
    );
    service.gateway_tasks.shutdown().await;
}

#[tokio::test]
async fn policy_transition_never_force_cancels_an_admitted_background_task() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let runtime_services = service.runtime_services();
    let initial = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        1,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "policy-background-drain".to_string(),
            platform: "test".to_string(),
            chat_id: "policy-background-drain".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(serde_json::json!({ "execution_policy": initial }).to_string()),
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("policy Session");
    runtime_services.publish_session_execution_policy(
        "policy-background-drain".to_string(),
        runtime::permissions::SessionExecutionPolicyControl::from_policy(initial.clone()),
    );
    service.install_test_session_policy("policy-background-drain", initial);
    let spec = runtime_services
        .task_runtime_port()
        .bind_task_spec(
            "policy-background-drain",
            Some(harness_contract::policy::PermissionMode::ReadOnly),
            harness_contract::task::TaskSpec::new("background work awaiting graph submission"),
        )
        .expect("bound Task policy");
    runtime_services
        .task_runtime_port()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "policy-background-task".to_string(),
            mission_id: runtime_services
                .mission_runtime()
                .default_mission_id()
                .to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::Schedule,
            origin_session_id: "mission-schedule:test".to_string(),
            origin_turn_id: "schedule-turn:test".to_string(),
            root_task_id: "policy-background-task".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Automatic,
            mission_assigned_by: "runtime.test".to_string(),
            spec,
            evidence_refs: Vec::new(),
        })
        .expect("admitted background Task");

    let draining = service
        .set_session_execution_policy(
            "policy-background-drain",
            runtime::AutonomyProfileId::Yolo,
            1,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .expect("policy transition");
    let transition = draining.transition.expect("transition receipt");
    assert_eq!(
        transition.phase,
        harness_contract::policy::PolicyTransitionPhase::Draining
    );
    assert_eq!(transition.old_revision_active_attempts, 1);
    assert_eq!(draining.permission_revision, Some(1));

    // The old drain-grace force-cancel is removed: after a grace period
    // that used to terminate the Task, it must still be running and the
    // transition must still be draining.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert_eq!(
        runtime_services
            .task_aggregate_service()
            .get("policy-background-task")
            .expect("task read")
            .expect("Task")
            .status,
        harness_contract::task::TaskStatus::Running
    );
    let pending = service
        .session_execution_policy_value("policy-background-drain")
        .await
        .expect("policy read");
    assert_eq!(pending.permission_revision, Some(1));
    assert_eq!(
        pending.transition.as_ref().unwrap().phase,
        harness_contract::policy::PolicyTransitionPhase::Draining
    );

    // Only an explicit cancellation drains the old revision; then the
    // desired policy becomes Stable.
    runtime_services
        .cancel_attempts_for_session_policy_revision(
            "policy-background-drain",
            1,
            "explicit test cancellation",
        )
        .await
        .expect("explicit old-revision cancellation");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let response = service
                .session_execution_policy_value("policy-background-drain")
                .await
                .expect("policy read");
            if response.permission_revision == Some(2) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background Task drains only after explicit cancellation");
    assert_eq!(
        runtime_services
            .task_aggregate_service()
            .get("policy-background-task")
            .expect("task read")
            .expect("Task")
            .status,
        harness_contract::task::TaskStatus::Cancelled
    );
    service.gateway_tasks.shutdown().await;
}

#[tokio::test]
async fn consecutive_desired_revisions_activate_only_the_latest_snapshot() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let _runtime_services = service.runtime_services();
    let now = chrono::Utc::now().to_rfc3339();
    let initial = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        1,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    store
        .create_session(&session::SessionRecord {
            session_id: "policy-latest-wins".to_string(),
            platform: "test".to_string(),
            chat_id: "policy-latest-wins".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(serde_json::json!({ "execution_policy": initial }).to_string()),
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    service.install_test_session_policy("policy-latest-wins", initial.clone());
    let live_control = service
        .sessions
        .session("policy-latest-wins")
        .and_then(|session| session.policy_control())
        .expect("test aggregate policy control");
    let (_, guard) = service
        .install_active_turn_control(
            "turn-latest-wins",
            "policy-latest-wins",
            Some("execution-latest-wins".to_string()),
        )
        .unwrap();
    let first = service
        .set_session_execution_policy(
            "policy-latest-wins",
            runtime::AutonomyProfileId::Cautious,
            1,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .unwrap();
    assert_eq!(first.policy.revision, 2);
    let latest = service
        .set_session_execution_policy(
            "policy-latest-wins",
            runtime::AutonomyProfileId::Yolo,
            2,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .unwrap();
    assert_eq!(latest.policy.revision, 3);
    assert_eq!(live_control.revision(), 1);
    drop(guard);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if live_control.revision() == 3 {
                break;
            }
            assert_ne!(
                live_control.revision(),
                2,
                "superseded desired revision activated"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("latest desired policy settles");
    assert_eq!(
        live_control.snapshot().autonomy_profile,
        runtime::AutonomyProfileId::Yolo
    );
    let stored = store
        .get_session("policy-latest-wins")
        .await
        .unwrap()
        .unwrap();
    let state = stored_session_execution_policy_state(&stored).expect("policy state");
    assert_eq!(state.effective.revision, 3);
    assert!(state.desired.is_none());
    assert!(state.pending_transition.is_none());
    service.gateway_tasks.shutdown().await;
}

#[tokio::test]
async fn restart_recovers_a_durable_draining_policy_transition() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let effective = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        4,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    let desired = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Cautious,
        5,
        runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
    );
    let transition = harness_contract::policy::PolicyTransitionReceipt {
        transition_id: "restart-transition".to_string(),
        phase: harness_contract::policy::PolicyTransitionPhase::Draining,
        desired_revision: 5,
        effective_revision: 4,
        old_revision_active_attempts: 1,
        requested_at_ms: 1,
        effective_at_ms: None,
        blocker: Some("old process stopped while draining".to_string()),
        failure: None,
    };
    let state = harness_contract::policy::SessionExecutionPolicyState {
        effective: effective.clone(),
        desired: Some(desired.clone()),
        pending_transition: Some(transition),
    };
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "policy-restart".to_string(),
            platform: "test".to_string(),
            chat_id: "policy-restart".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(
                serde_json::json!({
                    "execution_policy": effective,
                    "execution_policy_state": state,
                })
                .to_string(),
            ),
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let (restarted, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let response = restarted
        .session_execution_policy_value("policy-restart")
        .await
        .expect("restart reconciliation");
    assert_eq!(response.policy, desired);
    assert_eq!(response.permission_revision, Some(5));
    assert!(response.transition.as_ref().is_none_or(|transition| {
        transition.phase == harness_contract::policy::PolicyTransitionPhase::Stable
    }));
    let stored = store.get_session("policy-restart").await.unwrap().unwrap();
    let stable = stored_session_execution_policy_state(&stored).unwrap();
    assert_eq!(stable.effective.revision, 5);
    assert!(stable.desired.is_none());
    assert!(stable.pending_transition.is_none());
    restarted.gateway_tasks.shutdown().await;
}

#[tokio::test]
async fn policy_transition_waits_for_the_active_turn_and_never_cancels_it() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let initial = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        1,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    let now = chrono::Utc::now().to_rfc3339();
    for session_id in ["policy-drain", "policy-unrelated"] {
        store
            .create_session(&session::SessionRecord {
                session_id: session_id.to_string(),
                platform: "test".to_string(),
                chat_id: session_id.to_string(),
                user_id: None,
                model: None,
                created_at: now.clone(),
                last_activity: now.clone(),
                message_count: 0,
                reset_policy: "manual".to_string(),
                metadata_json: Some(serde_json::json!({ "execution_policy": initial }).to_string()),
                input_tokens: 0,
                output_tokens: 0,
                status: "active".to_string(),
            })
            .await
            .unwrap();
        service.install_test_session_policy(session_id, initial.clone());
    }
    let (active_cancel, active_guard) = service
        .install_active_turn_control("active-turn", "policy-drain", None)
        .unwrap();
    let (unrelated_cancel, unrelated_guard) = service
        .install_active_turn_control("other-turn", "policy-unrelated", None)
        .unwrap();
    let transition = service
        .set_session_execution_policy(
            "policy-drain",
            runtime::AutonomyProfileId::Yolo,
            1,
            runtime::SessionExecutionPolicyOrigin::SurfaceCommand,
        )
        .await
        .unwrap();
    let receipt = transition.transition.unwrap();
    assert_eq!(
        receipt.phase,
        harness_contract::policy::PolicyTransitionPhase::Draining
    );
    assert_eq!(receipt.old_revision_active_attempts, 1);

    // The old drain-grace deadline is removed: after a grace period that
    // used to force-cancel the turn, the running turn must still be
    // alive and the transition must still be draining.
    tokio::time::sleep(Duration::from_millis(250)).await;
    assert!(!active_cancel.is_cancelled());
    let value = service
        .session_execution_policy_value("policy-drain")
        .await
        .unwrap();
    assert_eq!(value.permission_revision, Some(1));
    assert_eq!(
        value.transition.as_ref().unwrap().phase,
        harness_contract::policy::PolicyTransitionPhase::Draining
    );

    // The turn finishes on its own terms; only then does Stable activate.
    drop(active_guard);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let value = service
                .session_execution_policy_value("policy-drain")
                .await
                .unwrap();
            if value.permission_revision == Some(2)
                && value.transition.as_ref().is_none_or(|transition| {
                    transition.phase == harness_contract::policy::PolicyTransitionPhase::Stable
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("natural turn completion settles before Stable");
    assert!(!unrelated_cancel.is_cancelled());
    assert!(service.is_session_turn_active("policy-unrelated", "other-turn"));
    drop(unrelated_guard);
    service.gateway_tasks.shutdown().await;
}

#[test]
fn policy_update_lock_hot_set_does_not_grow_with_session_history() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    for index in 0..10_000 {
        let lock = service.session_policy_update_lock(&format!("historical-{index}"));
        drop(lock);
    }
    assert_eq!(service.policy_transition_stripes.len(), 64);
}

#[tokio::test]
async fn config_default_reload_updates_only_default_owned_sessions_and_live_controls() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let now = chrono::Utc::now().to_rfc3339();
    let record =
        |session_id: &str, policy: &runtime::SessionExecutionPolicy| session::SessionRecord {
            session_id: session_id.to_string(),
            platform: "test".to_string(),
            chat_id: session_id.to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now.clone(),
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(
                serde_json::json!({
                    "execution_policy": policy,
                    "execution_policy_state": {
                        "effective": policy,
                        "desired": null,
                        "pending_transition": null
                    }
                })
                .to_string(),
            ),
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        };
    let default_owned = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        3,
        runtime::SessionExecutionPolicyOrigin::ConfigDefault,
    );
    let explicit = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Cautious,
        5,
        runtime::SessionExecutionPolicyOrigin::SessionExplicit,
    );
    store
        .create_session(&record("default-owned", &default_owned))
        .await
        .unwrap();
    store
        .create_session(&record("explicit-owned", &explicit))
        .await
        .unwrap();
    service.install_test_session_policy("default-owned", default_owned.clone());
    service.install_test_session_policy("explicit-owned", explicit.clone());
    let control =
        runtime::permissions::SessionExecutionPolicyControl::from_policy(default_owned.clone());
    service
        .runtime_services
        .publish_session_execution_policy("default-owned".to_string(), control.clone());

    let receipt = service
        .update_execution_policy_defaults(
            runtime::PermissionMode::DangerFullAccess,
            runtime::ApprovalProfile::Autonomous,
        )
        .await;

    assert_eq!(receipt["status"], "applied", "{receipt}");
    assert_eq!(receipt["updated_active_sessions"], 1);
    let applied = service
        .session_execution_policy_value("default-owned")
        .await
        .unwrap()
        .policy;
    assert_eq!(applied.revision, 4);
    assert_eq!(
        applied.permission_mode,
        runtime::PermissionMode::DangerFullAccess
    );
    assert_eq!(
        applied.approval_profile,
        runtime::ApprovalProfile::Autonomous
    );
    assert_eq!(control.snapshot(), applied);
    assert_eq!(
        service
            .session_execution_policy_value("explicit-owned")
            .await
            .unwrap()
            .policy,
        explicit
    );
    let stored = store.get_session("default-owned").await.unwrap().unwrap();
    assert_eq!(stored_session_execution_policy(&stored), Some(applied));
}

#[tokio::test]
async fn unchanged_config_reload_retries_a_default_owned_session_after_persistence_recovers() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        None,
    );
    let prior = runtime::SessionExecutionPolicy::from_profile(
        runtime::AutonomyProfileId::Supervised,
        3,
        runtime::SessionExecutionPolicyOrigin::ConfigDefault,
    );
    service.install_test_session_policy("retry-default", prior.clone());

    let failed = service
        .update_execution_policy_defaults(
            runtime::PermissionMode::DangerFullAccess,
            runtime::ApprovalProfile::Autonomous,
        )
        .await;
    assert_eq!(failed["status"], "attention", "{failed}");
    assert_eq!(failed["updated_active_sessions"], 0);
    assert_eq!(
        service.effective_session_execution_policy("retry-default"),
        prior
    );

    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "retry-default".to_string(),
            platform: "test".to_string(),
            chat_id: "retry-default".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: Some(
                serde_json::json!({
                    "execution_policy": prior,
                    "execution_policy_state": {
                        "effective": prior,
                        "desired": null,
                        "pending_transition": null
                    }
                })
                .to_string(),
            ),
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();

    let recovered = service
        .update_execution_policy_defaults(
            runtime::PermissionMode::DangerFullAccess,
            runtime::ApprovalProfile::Autonomous,
        )
        .await;
    assert_eq!(recovered["status"], "applied", "{recovered}");
    assert_eq!(recovered["default_changed"], false);
    assert_eq!(recovered["updated_active_sessions"], 1);
    let stored = store.get_session("retry-default").await.unwrap().unwrap();
    let policy = stored_session_execution_policy(&stored).expect("stored execution policy");
    assert_eq!(policy.revision, 4);
    assert_eq!(
        policy.permission_mode,
        runtime::PermissionMode::DangerFullAccess
    );
}

#[tokio::test]
async fn first_policy_read_materializes_the_current_config_default() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let (service, _session_service) = test_bound_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        Some((
            runtime::PermissionMode::ReadOnly,
            runtime::ApprovalProfile::Supervised,
        )),
    );
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "unmaterialized-policy".to_string(),
            platform: "test".to_string(),
            chat_id: "unmaterialized-policy".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();

    let response = service
        .session_execution_policy_value("unmaterialized-policy")
        .await
        .unwrap();

    assert_eq!(
        response.policy.permission_mode,
        runtime::PermissionMode::ReadOnly
    );
    assert_eq!(
        response.policy.approval_profile,
        runtime::ApprovalProfile::Supervised
    );
    assert_eq!(
        response.policy.origin,
        runtime::SessionExecutionPolicyOrigin::ConfigDefault
    );
    let stored = store
        .get_session("unmaterialized-policy")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        stored_session_execution_policy(&stored),
        Some(response.policy.clone())
    );
    assert_eq!(
        service.effective_session_execution_policy("unmaterialized-policy"),
        response.policy
    );
}

#[tokio::test]
async fn remove_active_runtime_keeps_other_session_restorations_isolated() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    service
        .gateway_tasks
        .open_session("session-a")
        .await
        .unwrap();
    service
        .gateway_tasks
        .open_session("session-b")
        .await
        .unwrap();
    service
        .gateway_tasks
        .spawn(
            crate::runtime_host::task_set::GatewayTaskKind::RuntimeRestoration,
            Some("session-a".to_string()),
            |cancellation| async move {
                cancellation.cancelled().await;
            },
        )
        .unwrap();
    service
        .gateway_tasks
        .spawn(
            crate::runtime_host::task_set::GatewayTaskKind::RuntimeRestoration,
            Some("session-b".to_string()),
            |cancellation| async move {
                cancellation.cancelled().await;
            },
        )
        .unwrap();

    assert!(service.remove_active_runtime("session-a").await.is_none());

    assert_eq!(service.gateway_tasks.tracked_task_count(), 1);
    service
        .gateway_tasks
        .close_session_and_drain("session-b", Duration::from_secs(1))
        .await;
    assert_eq!(service.gateway_tasks.tracked_task_count(), 0);
    service.gateway_tasks.shutdown().await;
}

#[tokio::test]
async fn restart_reuses_terminal_receipt_before_provider_runtime_lookup() {
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "restart-session".to_string(),
            platform: "test".to_string(),
            chat_id: "restart-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    store
        .append_ingress_with_runtime_outbox(
            "restart-session",
            "user",
            Some(r#"[{"type":"text","text":"must not run"}]"#),
            1,
            &session::SessionRuntimeOutboxRequest {
                input_id: "restart-input".to_string(),
                request_id: "restart-request".to_string(),
                turn_id: "restart-turn".to_string(),
                message_id: "restart-message".to_string(),
                session_generation: 1,
                decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                target_turn_id: None,
                classification_json: None,
                task_route_hint: None,
                created_at_ms: 1,
                runtime_options_json: None,
            },
        )
        .await
        .unwrap();
    let claim_at = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let claimed = store
        .claim_session_runtime_outbox("worker-a", claim_at, 30_000, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let claim_token = claimed.claim_token.clone().unwrap();
    let record = store
        .mark_session_runtime_outbox_running(
            "restart-request",
            "worker-a",
            claimed.session_generation,
            &claim_token,
            claimed.revision,
            claim_at,
        )
        .await
        .unwrap();
    let event_store_path = temp.path().join("runtime-events.sqlite");
    let runtime_event_store =
        Arc::new(runtime::RuntimeEventStore::try_open(&event_store_path).unwrap());
    let services = runtime::RuntimeServices::builder(&home, &workspace)
        .runtime_event_store(Arc::clone(&runtime_event_store))
        .build()
        .unwrap();
    let terminal_receipt = runtime_event_store
        .append_transaction_with_terminal(
            runtime::AppendTransactionRequest {
                transaction_id: "restart-terminal-transaction".to_string(),
                expected_streams: vec![runtime::ExpectedStreamRevision {
                    stream_id: "turn:restart-turn".to_string(),
                    expected_revision: 0,
                }],
                events: vec![runtime::RuntimeTransactionEventInput {
                    event: runtime::RuntimeEventInput {
                        stream_id: "turn:restart-turn".to_string(),
                        scope: runtime::RuntimeEventScope::SessionInput,
                        kind: "turn.terminal_committed".to_string(),
                        status: Some("completed".to_string()),
                        actor: Some("restart-test".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"result": "done"}),
                    },
                    idempotency_key: Some("restart-terminal-event".to_string()),
                    schema_version: 1,
                }],
            },
            runtime::SessionTerminalInput {
                terminal_id: "turn-terminal:restart-request".to_string(),
                message_id: "assistant-restart-message".to_string(),
                session_id: "restart-session".to_string(),
                execution_id: Some(runtime::session_ingress_graph_id(
                    "restart-session",
                    "restart-request",
                    "restart-turn",
                )),
                turn_id: Some("restart-turn".to_string()),
                request_id: Some("restart-request".to_string()),
                session_generation: Some(record.session_generation),
                input_sequence: Some(record.sequence as u64),
                input_claim_owner: record.claim_owner.clone(),
                input_claim_token: record.claim_token.clone(),
                input_claim_revision: record.claim_fence_epoch,
                controlled_recovery_claim_fingerprints: Vec::new(),
                payload_ref: "assistant_json:\"done\"".to_string(),
            },
        )
        .unwrap();
    let terminal_port = services.session_terminal_delivery();
    let claimed_terminal = terminal_port
        .claim("delivery-worker", claim_at, 30_000, 1)
        .unwrap()
        .pop()
        .unwrap();
    store
        .commit_terminal_transcript_if_fenced(&session::SessionTerminalTranscriptCommit {
            terminal_message_id: "assistant-restart-message".to_string(),
            ingress_message_id: "restart-message".to_string(),
            session_id: "restart-session".to_string(),
            turn_id: "restart-turn".to_string(),
            messages: vec![session::SessionMessage {
                stable_message_id: "assistant-restart-message".to_string(),
                session_id: "restart-session".to_string(),
                sequence: 0,
                role: "assistant".to_string(),
                content_json: r#"[{"type":"text","text":"done"}]"#.to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: claim_at,
            }],
            runtime_commit_cursor: terminal_receipt.commit_cursor,
            consumed_input_sequence: record.sequence,
            created_at_ms: claim_at,
            fence: session::SessionTerminalExecutionFence {
                request_id: record.request_id.clone(),
                input_sequence: record.sequence,
                session_generation: record.session_generation,
                claim_owner: record.claim_owner.clone().unwrap(),
                claim_token: record.claim_token.clone().unwrap(),
                claim_fence_epoch: record
                    .claim_fence_epoch
                    .expect("running input owns an immutable claim fence"),
            },
        })
        .await
        .unwrap();
    terminal_port
        .acknowledge(
            &claimed_terminal.terminal_id,
            "delivery-worker",
            claimed_terminal.revision,
            claim_at,
        )
        .unwrap();
    let first = test_runtime_service_with_services(
        Arc::new(ActiveSessionDirectory::new()),
        Arc::clone(&store),
        services,
    );
    assert_eq!(
        first
            .execute_ingress_record(&record, "must not run")
            .await
            .unwrap()
            .commit_cursor,
        terminal_receipt.commit_cursor
    );
    drop(first);
    drop(runtime_event_store);

    let restarted_event_store =
        Arc::new(runtime::RuntimeEventStore::try_open(&event_store_path).unwrap());
    let restarted_services = runtime::RuntimeServices::builder(&home, &workspace)
        .runtime_event_store(restarted_event_store)
        .build()
        .unwrap();
    let restarted = test_runtime_service_with_services(
        Arc::new(ActiveSessionDirectory::new()),
        store,
        restarted_services,
    );
    let receipt = restarted
        .execute_ingress_record(&record, "must still not run")
        .await
        .unwrap();
    assert_eq!(receipt.commit_cursor, terminal_receipt.commit_cursor);
    assert_eq!(
        receipt.graph_id,
        runtime::session_ingress_graph_id("restart-session", "restart-request", "restart-turn")
    );
}

#[tokio::test]
async fn recovered_terminal_settles_the_exact_primary_input_projection() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let temp = tempfile::tempdir().unwrap();
    let home = temp.path().join("home");
    let workspace = temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let runtime_event_store = Arc::new(runtime::RuntimeEventStore::try_open_in_memory().unwrap());
    let runtime_services = runtime::RuntimeServices::builder(&home, &workspace)
        .runtime_event_store(Arc::clone(&runtime_event_store))
        .build()
        .unwrap();
    let service = test_runtime_service_with_services(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        runtime_services,
    );
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "projection-session".to_string(),
            platform: "test".to_string(),
            chat_id: "projection-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("test session");
    service.install_test_session_input(
        "projection-session",
        runtime::SessionInputStream::new("projection-session"),
    );
    let admission = service
        .admit_session_input_with_materialized(
            SessionInputEnvelope::text(
                "projection-session",
                harness_contract::turn::InputSourceKind::Webui,
                "already supplied to ingress",
            )
            .with_idempotency_key("projection-primary"),
        )
        .await
        .expect("admission");
    let queued = store
        .get_session_runtime_outbox("projection-primary")
        .await
        .expect("outbox lookup")
        .expect("persisted ingress");
    let now_ms = chrono::Utc::now().timestamp_millis().max(0) as u64;
    let claimed = store
        .claim_session_runtime_outbox("projection-worker", now_ms, 30_000, 1)
        .await
        .expect("claim persisted ingress")
        .into_iter()
        .next()
        .expect("claim result");
    assert_eq!(claimed.request_id, queued.request_id);
    let claim_token = claimed
        .claim_token
        .clone()
        .expect("claim token is part of the execution fence");
    let record = store
        .mark_session_runtime_outbox_running(
            &claimed.request_id,
            "projection-worker",
            claimed.session_generation,
            &claim_token,
            claimed.revision,
            now_ms,
        )
        .await
        .expect("mark claimed ingress running");
    let terminal_commit = runtime_event_store
        .append_transaction_with_terminal(
            runtime::AppendTransactionRequest {
                transaction_id: "projection-terminal-transaction".to_string(),
                expected_streams: vec![runtime::ExpectedStreamRevision {
                    stream_id: "turn:projection-primary".to_string(),
                    expected_revision: 0,
                }],
                events: vec![runtime::RuntimeTransactionEventInput {
                    event: runtime::RuntimeEventInput {
                        stream_id: "turn:projection-primary".to_string(),
                        scope: runtime::RuntimeEventScope::SessionInput,
                        kind: "turn.terminal_committed".to_string(),
                        status: Some("completed".to_string()),
                        actor: Some("projection-test".to_string()),
                        refs: Vec::new(),
                        payload: serde_json::json!({"result": "done"}),
                    },
                    idempotency_key: Some("projection-terminal-event".to_string()),
                    schema_version: 1,
                }],
            },
            runtime::SessionTerminalInput {
                terminal_id: admission.terminal_id.clone(),
                message_id: "assistant-projection-primary".to_string(),
                session_id: record.session_id.clone(),
                execution_id: Some(admission.execution_graph_id.clone()),
                turn_id: Some(record.turn_id.clone()),
                request_id: Some(record.request_id.clone()),
                session_generation: Some(record.session_generation),
                input_sequence: Some(record.sequence as u64),
                input_claim_owner: record.claim_owner.clone(),
                input_claim_token: record.claim_token.clone(),
                input_claim_revision: record.claim_fence_epoch,
                controlled_recovery_claim_fingerprints: Vec::new(),
                payload_ref: "assistant_json:\"done\"".to_string(),
            },
        )
        .expect("terminal and its exact Session fence commit atomically");
    let persisted_terminal = service
        .runtime_services()
        .session_terminal_delivery()
        .get(&admission.terminal_id)
        .expect("terminal lookup")
        .expect("terminal persisted");
    assert_eq!(
        persisted_terminal.request_id.as_deref(),
        Some(record.request_id.as_str())
    );
    assert_eq!(
        persisted_terminal.input_claim_revision,
        record.claim_fence_epoch
    );
    assert_eq!(
        persisted_terminal.commit_cursor,
        terminal_commit.commit_cursor
    );
    assert_eq!(persisted_terminal.input_claim_owner, record.claim_owner);
    assert_eq!(persisted_terminal.input_claim_token, record.claim_token);
    assert_eq!(
        persisted_terminal.session_generation,
        Some(record.session_generation)
    );
    assert_eq!(
        persisted_terminal.turn_id.as_deref(),
        Some(record.turn_id.as_str())
    );
    assert_eq!(
        persisted_terminal.execution_id.as_deref(),
        Some(admission.execution_graph_id.as_str())
    );
    assert_eq!(persisted_terminal.session_id, record.session_id);
    assert_eq!(persisted_terminal.terminal_id, admission.terminal_id);
    assert_eq!(
        persisted_terminal.message_id,
        "assistant-projection-primary"
    );
    assert_eq!(persisted_terminal.payload_ref, "assistant_json:\"done\"");
    assert_eq!(persisted_terminal.status, "pending");
    assert_eq!(persisted_terminal.revision, 0);
    assert_eq!(persisted_terminal.attempts, 0);
    assert_eq!(persisted_terminal.next_attempt_at_ms, None);
    assert_eq!(persisted_terminal.claim_owner, None);
    assert_eq!(persisted_terminal.claim_expires_at_ms, None);
    assert_eq!(persisted_terminal.failure_class, None);
    assert_eq!(persisted_terminal.last_error, None);
    assert_eq!(persisted_terminal.materialized_at_ms, None);
    let terminal_claim = service
        .runtime_services()
        .session_terminal_delivery()
        .claim("projection-delivery", now_ms.saturating_add(1), 30_000, 1)
        .expect("claim terminal delivery")
        .into_iter()
        .find(|terminal| terminal.terminal_id == admission.terminal_id)
        .expect("terminal delivery claim");
    assert_eq!(
        terminal_claim.input_claim_revision,
        record.claim_fence_epoch
    );
    assert!(terminal_claim.revision > persisted_terminal.revision);
    service
        .runtime_services()
        .session_terminal_delivery()
        .acknowledge(
            &terminal_claim.terminal_id,
            "projection-delivery",
            terminal_claim.revision,
            now_ms.saturating_add(2),
        )
        .expect("materialize recovered terminal");

    service
        .execute_ingress_record(&record, "must not call provider")
        .await
        .expect("recovered terminal is delivered");

    let projection = service
        .session_input_projection("projection-session")
        .await
        .expect("input projection");
    assert_eq!(projection.pending_count, 0);
    assert_eq!(projection.consumed_count, 1);
    assert!(projection.inputs.is_empty());
    assert_eq!(
        projection.consumed_cursor,
        Some(harness_contract::turn::SessionInputCursor::new(
            record.session_generation,
            u64::try_from(record.sequence).unwrap_or(u64::MAX),
        ))
    );
    let stream = service
        .test_session_input("projection-session")
        .expect("in-process stream");
    assert!(stream
        .record_snapshot(&admission.receipt.input_id)
        .is_none());
    assert_eq!(
        stream.highest_consumed_cursor(&TurnId::from_string(record.turn_id.clone())),
        projection.consumed_cursor
    );
}

#[test]
fn upgrade_status_mapping_preserves_active_and_terminal_boundaries() {
    assert_eq!(
        upgrade_agent_status(&harness_contract::agent::AgentStatus::Running),
        runtime::UpgradeCarrierStatus::Running
    );
    assert_eq!(
        upgrade_agent_status(&harness_contract::agent::AgentStatus::Completed),
        runtime::UpgradeCarrierStatus::Completed
    );
    assert_eq!(
        upgrade_team_status("review_required"),
        runtime::UpgradeCarrierStatus::Waiting
    );
}

#[test]
fn upgrade_carrier_hash_is_stable_for_same_projection() {
    let state = serde_json::json!({"status": "running", "revision": 3});
    let first = upgrade_carrier_record(
        "agent",
        "agent-1".to_string(),
        runtime::UpgradeCarrierStatus::Running,
        3,
        None,
        None,
        &state,
    );
    let second = upgrade_carrier_record(
        "agent",
        "agent-1".to_string(),
        runtime::UpgradeCarrierStatus::Running,
        3,
        None,
        None,
        &state,
    );
    assert_eq!(first.state_hash, second.state_hash);
}

#[tokio::test]
async fn runtime_service_status_does_not_initialize_model_provider() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);

    let value = service.status_value();
    assert_eq!(value["ok"], true);
    assert_eq!(value["runtime_host"], "gateway-runtime-host");
    let removed_legacy_key = ["dae", "mon"].concat();
    assert!(value.get(&removed_legacy_key).is_none());
    assert_eq!(value["active_sessions"], 0);
}

#[tokio::test]
async fn runtime_service_snapshot_reports_lease_projection() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);

    let lease = service
        .acquire_session_lease_value("session-1", "tui:test", "collaborative")
        .await;
    assert_eq!(lease["ok"], true);

    let snapshot = service.snapshot_value().await;
    assert_eq!(snapshot["kind"], "gateway_runtime_snapshot");
    assert!(snapshot.get("legacy_kind").is_none());
    let removed_legacy_key = ["dae", "mon"].concat();
    assert!(snapshot.get(&removed_legacy_key).is_none());
    assert_eq!(snapshot["leases"]["total"], 1);
    assert_eq!(snapshot["transport"]["control"], "gateway_http");
}

#[tokio::test]
async fn runtime_service_records_durable_turn_journal() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "journal-session".to_string(),
            platform: "test".to_string(),
            chat_id: "journal-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let service = test_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Some(store.clone()),
    );

    let submitted = service
        .submit_turn_value(
            Some("journal-session".to_string()),
            Some("task-a".to_string()),
            "persist this turn".to_string(),
        )
        .await;

    assert_eq!(submitted["ok"], true);
    assert_eq!(submitted["durable_journal"], true);
    let events = store
        .get_events_by_type_limited("journal-session", "TurnJournal", 0, 10)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    let payload: serde_json::Value = serde_json::from_str(&events[0].event_json).unwrap();
    assert_eq!(payload["event_type"], "turn.submitted");
    assert_eq!(payload["phase"], "submitted");
    assert_eq!(payload["payload"]["prompt"], "persist this turn");
    assert_eq!(payload["payload"]["task_id"], "task-a");
}

#[tokio::test]
async fn runtime_service_persists_session_input_runtime_event() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "input-session".to_string(),
            platform: "test".to_string(),
            chat_id: "input-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .unwrap();
    let active_sessions = Arc::new(ActiveSessionDirectory::default());
    let service = test_runtime_service(active_sessions, Some(store.clone()));
    service.install_test_session_input(
        "input-session",
        runtime::SessionInputStream::new("input-session"),
    );

    let receipt = service
        .admit_session_input(harness_contract::turn::SessionInputEnvelope::text(
            "input-session",
            harness_contract::turn::InputSourceKind::Api,
            "remember this during the current work",
        ))
        .await
        .expect("admit input");

    assert_eq!(receipt.session_id, "input-session");
    let page = store
        .session_domain_events_page("input-session", 0, 10)
        .await
        .expect("runtime events page");
    let kinds = page
        .events
        .iter()
        .map(|event| event.kind.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        vec![
            "session.input.accepted.v1",
            "session.input.classified.v1",
            "session.input.queued.v1",
        ],
        "durable admission owns one canonical accepted/classified/queued timeline"
    );
    let event = &page.events[0];
    assert_eq!(event.payload["input_id"], receipt.input_id.to_string());
    assert_eq!(event.payload["message_id"], receipt.input_id.to_string());
    assert_eq!(event.status.as_deref(), Some("accepted"));
    let hot = service
        .runtime_services()
        .hot_session_snapshot("input-session")
        .expect("hot Session input projection");
    assert_eq!(hot.pending_inputs, 1, "{hot:?}");
    assert_eq!(hot.durable_cursor, Some(0), "{hot:?}");
    assert_eq!(
        hot.inbox_refs,
        vec![format!("session-input:{}", receipt.input_id)]
    );
}

#[test]
fn checkpoint_consumed_supplement_is_authoritative_after_turn_completion() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let stream = runtime::SessionInputStream::new("checkpoint-session");
    let turn_id = TurnId::from_string("turn-active");
    stream.set_active_turn(Some(turn_id.clone()));
    let envelope = SessionInputEnvelope::text(
        "checkpoint-session",
        harness_contract::turn::InputSourceKind::Api,
        "late supplement",
    );
    let input_id = envelope.input_id.clone();
    let receipt = SessionInputReceipt {
        input_id: input_id.clone(),
        session_id: "checkpoint-session".to_string(),
        status: SessionInputStatus::AttachedToTurn,
        decision: InputRoutingDecision::SupplementCurrentTurn,
        relation_proposal: None,
        reason: Some(InputRoutingReason::new(
            "test",
            "attached to active turn",
            10_000,
        )),
        active_turn_id: Some(turn_id.clone()),
        evidence_refs: Vec::new(),
        cursor: Some(harness_contract::turn::SessionInputCursor::new(1, 2)),
        created_at: envelope.created_at,
    };
    stream.project_durable(envelope, receipt);
    assert_eq!(
        stream
            .consume_for_checkpoint(
                &turn_id,
                harness_contract::turn::TurnInputCheckpoint::BeforeProviderRequest,
                1,
            )
            .len(),
        1
    );
    stream.set_active_turn(None);
    service.install_test_session_input("checkpoint-session", stream);

    assert!(service.session_input_checkpoint_consumed(
        "checkpoint-session",
        input_id.as_str(),
        Some("turn-active")
    ));
    assert!(!service.session_input_checkpoint_consumed(
        "checkpoint-session",
        input_id.as_str(),
        Some("turn-other")
    ));
    assert_eq!(
        service.acknowledge_durable_session_inputs_through(
            "checkpoint-session",
            "turn-active",
            1,
            2,
        ),
        1
    );
    assert!(!service.session_input_checkpoint_consumed(
        "checkpoint-session",
        input_id.as_str(),
        Some("turn-active")
    ));
}

#[test]
fn runtime_service_rejects_unsupported_protocol_as_legacy_socket_error() {
    let request: RuntimeRequest = serde_json::from_value(serde_json::json!({
        "protocol_version": 999,
        "request_id": "req-old",
        "cmd": "status",
    }))
    .expect("request parses");

    let value = RuntimeService::unsupported_protocol_value(&request);
    assert_eq!(value["ok"], false);
    assert_eq!(value["request_id"], "req-old");
    assert_eq!(value["error_kind"], "unsupported_protocol");
    assert_eq!(value["retryable"], false);
    assert!(value["error"]
        .as_str()
        .unwrap_or_default()
        .contains("unsupported runtime protocol version"));
}

#[test]
fn runtime_service_records_executing_turn_lifecycle() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);

    let running = service.start_running_turn(
        Some("session-turn".to_string()),
        Some("task-turn".to_string()),
        "execute real turn".to_string(),
    );
    assert_eq!(running.status, TurnStatus::Running);
    assert_eq!(running.session_id.as_deref(), Some("session-turn"));
    assert_eq!(running.primary_task_id.as_deref(), Some("task-turn"));

    let completed = service.finish_turn(&running.turn_id, TurnStatus::Completed, None);
    assert_eq!(completed.status, TurnStatus::Completed);
    assert!(completed.completed_at.is_some());
    assert_eq!(completed.events.len(), 2);
    assert_eq!(completed.events[0].status, TurnStatus::Running);
    assert_eq!(completed.events[1].status, TurnStatus::Completed);

    let snapshot = service.turns_value();
    assert_eq!(snapshot["turns"], serde_json::json!([]));
}

#[test]
fn ten_thousand_terminal_turns_and_control_guards_leave_no_hot_entries() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    for index in 0..10_000 {
        let running = service.start_running_turn(
            Some(format!("session-{index}")),
            None,
            "bounded hot turn".to_string(),
        );
        let completed = service.finish_turn(&running.turn_id, TurnStatus::Completed, None);
        assert_eq!(completed.status, TurnStatus::Completed);

        let turn_id = format!("control-{index}");
        service
            .active_turns
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .controls
            .insert(
                turn_id.clone(),
                ActiveTurnControl {
                    session_id: format!("session-{index}"),
                    execution_id: Some(format!("execution-{index}")),
                    policy_revision: 1,
                    requested_sandbox_posture:
                        harness_contract::policy::SandboxPosture::ReadOnlySandbox,
                    effective_sandbox_posture:
                        harness_contract::policy::SandboxPosture::ReadOnlySandbox,
                    cancellation_token: runtime::CancellationToken::new(),
                },
            );
        drop(ActiveTurnControlGuard {
            turn_id,
            registry: Arc::clone(&service.active_turns),
        });
    }
    assert!(service.turns.lock().unwrap().is_empty());
    assert!(service
        .active_turns
        .state
        .lock()
        .unwrap()
        .controls
        .is_empty());
}

#[test]
fn runtime_event_relay_preserves_event_type_without_inventing_lifecycle() {
    let text = SessionProjectionEvent::runtime(runtime::CowdEvent::TextDelta {
        text: "partial".to_string(),
    })
    .to_transport_value();
    assert_eq!(text["type"], "TextDelta");
    assert_eq!(text["text"], "partial");

    let completed = SessionProjectionEvent::runtime(runtime::CowdEvent::TurnComplete {
        assistant_text: "draft".to_string(),
        iterations: 2,
    })
    .to_transport_value();
    assert_eq!(completed["type"], "TurnComplete");
    assert_eq!(completed["assistant_text"], "draft");
    assert!(completed.get("committed").is_none());

    let scoped = SessionProjectionEvent::runtime(runtime::CowdEvent::ExecutionScoped {
        context: runtime::CowdExecutionContext {
            execution_id: "execution-1".to_string(),
            session_id: "session-1".to_string(),
            turn_id: "turn-1".to_string(),
        },
        activity_binding: None,
        event: Box::new(runtime::CowdEvent::ExecutionPhase {
            status: ExecutionLiveStatus::CallingModel,
            detail: Some("requesting model".to_string()),
        }),
    })
    .to_transport_value();
    assert_eq!(scoped["type"], "ExecutionPhase");
    assert_eq!(scoped["execution_id"], "execution-1");
    assert_eq!(scoped["turn_id"], "turn-1");
}

#[tokio::test]
async fn runtime_event_relay_forwards_render_events_to_gateway_session_bus() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let gateway_bus = Arc::clone(&service.projection_hub);
    let mut rx = gateway_bus.subscribe("relay-session", 8).await;
    let old_runtime_bus = runtime::CowdEventBus::new();
    service
        .install_session_event_relay("relay-session", old_runtime_bus.clone())
        .await
        .unwrap();
    old_runtime_bus.emit(runtime::CowdEvent::TextDelta {
        text: "before replacement".to_string(),
    });
    let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("relay should forward within bounded time")
        .expect("gateway subscriber remains open");
    let payload = payload.to_transport_value();
    assert_eq!(payload["type"], "TextDelta");
    assert_eq!(payload["text"], "before replacement");

    let current_runtime_bus = runtime::CowdEventBus::new();
    service
        .install_session_event_relay("relay-session", current_runtime_bus.clone())
        .await
        .unwrap();
    old_runtime_bus.emit(runtime::CowdEvent::TextDelta {
        text: "stale relay".to_string(),
    });
    current_runtime_bus.emit(runtime::CowdEvent::TextDelta {
        text: "current relay".to_string(),
    });
    let payload = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .expect("replacement relay should forward within bounded time")
        .expect("gateway subscriber remains open")
        .to_transport_value();
    assert_eq!(payload["text"], "current relay");
    assert_eq!(service.gateway_tasks.tracked_task_count(), 1);

    service.remove_active_runtime("relay-session").await;
    assert_eq!(service.gateway_tasks.tracked_task_count(), 0);
    current_runtime_bus.emit(runtime::CowdEvent::TextDelta {
        text: "after removal".to_string(),
    });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), rx.recv())
            .await
            .is_err(),
        "removed relay must not forward additional events"
    );
    service.gateway_tasks.shutdown().await;
}

#[tokio::test]
async fn replacement_relay_resumes_text_range_from_runtime_live_state() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let mut rx = service
        .projection_hub
        .subscribe("relay-range-session", 8)
        .await;
    let context = runtime::CowdExecutionContext {
        execution_id: "relay-range-execution".to_string(),
        session_id: "relay-range-session".to_string(),
        turn_id: "relay-range-turn".to_string(),
    };
    let text_event =
        |context: runtime::CowdExecutionContext, item_id: &str, delta_sequence, text: &str| {
            runtime::CowdEvent::ExecutionScoped {
                context,
                activity_binding: None,
                event: Box::new(runtime::CowdEvent::Causal {
                    identity: runtime::CausalItemIdentity {
                        model_step_id: "relay-range-model-step".to_string(),
                        item_id: item_id.to_string(),
                        segment_id: format!("{item_id}:text:0"),
                        causal_sequence: 1,
                        delta_sequence,
                        tool_call_id: None,
                        causal_parent_ids: Vec::new(),
                    },
                    event: Box::new(runtime::CowdEvent::TextDelta {
                        text: text.to_string(),
                    }),
                }),
            }
        };

    let first_bus = runtime::CowdEventBus::new();
    service
        .install_session_event_relay("relay-range-session", first_bus.clone())
        .await
        .unwrap();
    first_bus.emit(text_event(context.clone(), "relay-range-text", 1, "第一段"));
    let first = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap()
        .to_transport_value();
    let first_end = first["end_bytes"].as_u64().unwrap();
    assert_eq!(first["start_bytes"], 0);

    let replacement_bus = runtime::CowdEventBus::new();
    service
        .install_session_event_relay("relay-range-session", replacement_bus.clone())
        .await
        .unwrap();
    replacement_bus.emit(text_event(context.clone(), "relay-range-text", 2, "second"));
    let second = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap()
        .to_transport_value();
    assert_eq!(second["start_bytes"], first_end);
    assert_eq!(second["end_bytes"], first_end + 6);
    replacement_bus.emit(text_event(context, "relay-range-text-2", 1, "new"));
    let third = tokio::time::timeout(std::time::Duration::from_secs(1), rx.recv())
        .await
        .unwrap()
        .unwrap()
        .to_transport_value();
    assert_eq!(third["part_id"], "relay-range-text-2:text:0");
    assert_eq!(third["start_bytes"], 0);
    assert_eq!(third["end_bytes"], 3);
    service.gateway_tasks.shutdown().await;
}

#[test]
fn session_execution_index_exposes_running_only_and_retains_terminal_reference() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    service.record_live_execution(
        "session-index",
        "execution-running".to_string(),
        "turn-running".to_string(),
    );
    service.record_live_execution(
        "session-index",
        "execution-finished".to_string(),
        "turn-finished".to_string(),
    );

    let report = ContextTurnReport::new(
        "turn-finished",
        harness_contract::context::ContextPressureState::new("default", 32_000, 8_000),
    );
    service.complete_live_execution(
        "execution-finished",
        &report,
        &[],
        "terminal-finished".to_string(),
    );

    let index = service.session_execution_index("session-index");
    assert_eq!(index.active_execution_ids, vec!["execution-running"]);
    assert_eq!(
        index.latest_execution_id.as_deref(),
        Some("execution-finished")
    );
    assert_eq!(index.latest_status, Some(ExecutionLiveStatus::Complete));
    assert_eq!(index.terminal_ref.as_deref(), Some("terminal-finished"));
    assert_eq!(index.executions.len(), 2);
    assert_eq!(index.executions[0].turn_id.as_deref(), Some("turn-running"));
    assert_eq!(
        index.executions[1].graph_id.as_deref(),
        None,
        "a graph id is exposed only after Runtime binds the queryable graph"
    );
    assert!(service
        .running_session_execution_indices()
        .iter()
        .any(|entry| entry.session_id == "session-index"));
}

#[test]
fn session_cancel_reaches_the_runtime_turn_control_instead_of_only_emitting_ui_state() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let (cancellation, guard) = service
        .install_active_turn_control(
            "turn-cancel",
            "session-cancel",
            Some("execution-cancel".to_string()),
        )
        .unwrap();
    service.record_live_execution(
        "session-cancel",
        "execution-cancel".to_string(),
        "turn-cancel".to_string(),
    );

    let cancelled = service.cancel_active_session("session-cancel", "evaluator timeout isolation");

    assert_eq!(cancelled, vec!["execution-cancel"]);
    assert!(cancellation.is_cancelled());
    drop(guard);
}

#[tokio::test]
async fn user_cancelled_primary_ingress_does_not_write_ingress_failed() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "cancel-journal-session".to_string(),
            platform: "test".to_string(),
            chat_id: "cancel-journal-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("test session");
    let service = test_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        Some(Arc::clone(&store)),
    );
    let stream = runtime::SessionInputStream::new("cancel-journal-session");
    service.install_test_session_input("cancel-journal-session", stream.clone());
    let admission = service
        .admit_session_input_with_materialized(
            SessionInputEnvelope::text(
                "cancel-journal-session",
                harness_contract::turn::InputSourceKind::Tui,
                "cancel me",
            )
            .with_idempotency_key("cancel-journal-request"),
        )
        .await
        .expect("admit primary input");
    let outbox = store
        .get_session_runtime_outbox("cancel-journal-request")
        .await
        .unwrap()
        .expect("durable ingress");
    service
        .bind_primary_ingress_projection(&outbox, &admission.execution_graph_id)
        .await;
    service
        .cancel_primary_ingress_projection(&outbox, "user requested")
        .await;
    let record = stream
        .record_snapshot(&admission.receipt.input_id)
        .expect("cancelled input projection");
    assert_eq!(record.status, SessionInputStatus::Cancelled);

    let failed = store
        .get_events_by_type_limited("cancel-journal-session", "SessionInputIngressFailed", 0, 32)
        .await
        .unwrap();
    assert!(
        failed.is_empty(),
        "user cancellation must not be journalled as ingress failure: {failed:?}"
    );
}

#[tokio::test]
async fn durable_requested_cancellation_stops_ingress_before_provider_or_tool_work() {
    let store = Arc::new(session::UnifiedSessionStore::open_in_memory().unwrap());
    let now = chrono::Utc::now().to_rfc3339();
    store
        .create_session(&session::SessionRecord {
            session_id: "cancel-before-runtime-session".to_string(),
            platform: "test".to_string(),
            chat_id: "cancel-before-runtime-session".to_string(),
            user_id: None,
            model: None,
            created_at: now.clone(),
            last_activity: now,
            message_count: 0,
            reset_policy: "manual".to_string(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".to_string(),
        })
        .await
        .expect("test session");
    // Deliberately do not install an active Session runtime. Reaching the
    // provider path would therefore fail, so a Cancelled receipt proves
    // the durable intent fenced all model/tool work first.
    let service = test_runtime_service_with_services(
        Arc::new(ActiveSessionDirectory::default()),
        Arc::clone(&store),
        runtime::RuntimeServices::in_memory().expect("runtime services"),
    );
    service.install_test_session_input(
        "cancel-before-runtime-session",
        runtime::SessionInputStream::new("cancel-before-runtime-session"),
    );
    let admission = service
        .admit_session_input_with_materialized(
            SessionInputEnvelope::text(
                "cancel-before-runtime-session",
                harness_contract::turn::InputSourceKind::Tui,
                "must never reach a provider",
            )
            .with_idempotency_key("cancel-before-runtime-request"),
        )
        .await
        .expect("admit primary input");
    let record = store
        .get_session_runtime_outbox("cancel-before-runtime-request")
        .await
        .unwrap()
        .expect("durable ingress");
    service.runtime_services().record_live_execution(
        &record.session_id,
        admission.execution_graph_id.clone(),
        record.turn_id.clone(),
    );
    service
        .runtime_services()
        .commit_cancellation_receipt(harness_contract::turn::CancellationReceipt {
            cancellation_id: "cancel-before-runtime-id".to_string(),
            session_id: record.session_id.clone(),
            turn_id: record.turn_id.clone(),
            execution_id: admission.execution_graph_id.clone(),
            actor_id: "principal:local-human".to_string(),
            cause: harness_contract::turn::CancellationCause::UserRequested,
            reason: Some("user_requested".to_string()),
            requested_at_ms: 100,
            effective_at_ms: None,
            status: harness_contract::turn::CancellationStatus::Requested,
            journal_sequence: 0,
            projection_revision: 0,
        })
        .expect("durable cancellation intent");

    let executed = service
        .execute_ingress_record(&record, "must never run")
        .await
        .expect("durable cancellation is a successful cancelled settlement");
    assert_eq!(
        executed.status,
        runtime::SessionIngressExecutionStatus::Cancelled
    );
    assert!(executed.commit_cursor > 0);
    assert_eq!(
        service
            .runtime_services()
            .cancellation_receipt("cancel-before-runtime-id")
            .unwrap()
            .unwrap()
            .status,
        harness_contract::turn::CancellationStatus::Cancelled
    );
    let failed = store
        .get_events_by_type_limited(
            "cancel-before-runtime-session",
            "SessionInputIngressFailed",
            0,
            32,
        )
        .await
        .unwrap();
    assert!(failed.is_empty());
}

#[tokio::test]
async fn process_shutdown_rejects_new_turns_and_waits_for_active_turn_guard() {
    let service = Arc::new(test_runtime_service(
        Arc::new(ActiveSessionDirectory::default()),
        None,
    ));
    let (cancellation, guard) = service
        .install_active_turn_control(
            "turn-shutdown",
            "session-shutdown",
            Some("execution-shutdown".to_string()),
        )
        .unwrap();

    let cancelled = service.stop_accepting_and_cancel_active_turns("Gateway process shutdown test");
    assert_eq!(cancelled, vec!["execution-shutdown"]);
    assert!(cancellation.is_cancelled());
    assert_eq!(service.active_turn_count(), 1);
    assert!(service
        .install_active_turn_control("turn-late", "session-shutdown", None)
        .is_err());

    let waiter_service = Arc::clone(&service);
    let waiter = tokio::spawn(async move {
        waiter_service
            .wait_for_active_turns(cancelled.len(), Duration::from_secs(1))
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!waiter.is_finished());

    drop(guard);
    let report = waiter.await.unwrap();
    assert_eq!(report.cancelled, 1);
    assert_eq!(report.drained, 1);
    assert!(report.remaining_turn_ids.is_empty());
    assert_eq!(service.active_turn_count(), 0);
    service.gateway_tasks.shutdown().await;
}

#[test]
fn durable_ingress_index_recovers_execution_identity_without_mixing_cursors() {
    let records = vec![
        session::SessionRuntimeOutboxRecord {
            input_id: "input-complete".to_string(),
            request_id: "request-complete".to_string(),
            turn_id: "turn-complete".to_string(),
            message_id: "message-complete".to_string(),
            session_id: "session-recovery".to_string(),
            sequence: 1,
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            status: session::SessionRuntimeInputStatus::Completed,
            runtime_commit_cursor: Some(44),
            attempts: 1,
            next_attempt_at_ms: 0,
            claim_owner: None,
            claim_token: None,
            claim_fence_epoch: None,
            claim_expires_at_ms: None,
            failure_class: None,
            last_error: None,
            revision: 9,
            created_at_ms: 10,
            updated_at_ms: 20,
            terminal_at_ms: Some(20),
            runtime_options_json: None,
            application_receipt: None,
        },
        session::SessionRuntimeOutboxRecord {
            input_id: "input-pending".to_string(),
            request_id: "request-pending".to_string(),
            turn_id: "turn-pending".to_string(),
            message_id: "message-pending".to_string(),
            session_id: "session-recovery".to_string(),
            sequence: 2,
            session_generation: 1,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            status: session::SessionRuntimeInputStatus::Queued,
            runtime_commit_cursor: None,
            attempts: 0,
            next_attempt_at_ms: 0,
            claim_owner: None,
            claim_token: None,
            claim_fence_epoch: None,
            claim_expires_at_ms: None,
            failure_class: None,
            last_error: None,
            revision: 3,
            created_at_ms: 21,
            updated_at_ms: 30,
            terminal_at_ms: None,
            runtime_options_json: None,
            application_receipt: None,
        },
    ];
    let index = session_execution_index_from_outbox("session-recovery", &records);
    assert_eq!(index.active_execution_ids.len(), 1);
    assert_eq!(index.latest_status, Some(ExecutionLiveStatus::Queued));
    assert_eq!(index.latest_live_revision, None);
    assert_eq!(index.last_progress_at_ms, Some(30));
    assert!(index.terminal_ref.is_none());
    assert_eq!(
        index
            .executions
            .iter()
            .map(|entry| entry.turn_id.as_deref().unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["turn-complete", "turn-pending"]
    );
    assert!(
        index
            .executions
            .iter()
            .all(|entry| entry.graph_id.as_deref() == Some(entry.execution_id.as_str())),
        "every durable Session ingress execution must expose a queryable graph id"
    );
    assert_eq!(index.latest_graph_id, index.latest_execution_id.clone());
    assert_eq!(
        index.latest_execution_id,
        Some(runtime::session_ingress_graph_id(
            "session-recovery",
            "request-pending",
            "turn-pending"
        ))
    );
}

#[test]
fn durable_execution_index_ignores_newer_supplement_carriers() {
    let primary = session::SessionRuntimeOutboxRecord {
        input_id: "input-primary".to_string(),
        request_id: "request-primary".to_string(),
        turn_id: "turn-primary".to_string(),
        message_id: "message-primary".to_string(),
        session_id: "session-recovery".to_string(),
        sequence: 1,
        session_generation: 1,
        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
        target_turn_id: None,
        classification_json: None,
        task_route_hint: None,
        status: session::SessionRuntimeInputStatus::Completed,
        runtime_commit_cursor: Some(44),
        attempts: 1,
        next_attempt_at_ms: 0,
        claim_owner: None,
        claim_token: None,
        claim_fence_epoch: None,
        claim_expires_at_ms: None,
        failure_class: None,
        last_error: None,
        revision: 9,
        created_at_ms: 10,
        updated_at_ms: 20,
        terminal_at_ms: Some(20),
        runtime_options_json: None,
        application_receipt: None,
    };
    let supplement = session::SessionRuntimeOutboxRecord {
        input_id: "input-supplement".to_string(),
        request_id: "request-supplement".to_string(),
        turn_id: "turn-supplement".to_string(),
        message_id: "message-supplement".to_string(),
        session_id: "session-recovery".to_string(),
        sequence: 2,
        session_generation: 1,
        decision: harness_contract::turn::InputRoutingDecision::SupplementCurrentTurn,
        target_turn_id: Some("turn-primary".to_string()),
        classification_json: None,
        task_route_hint: None,
        status: session::SessionRuntimeInputStatus::Supplemented,
        runtime_commit_cursor: None,
        attempts: 1,
        next_attempt_at_ms: 0,
        claim_owner: None,
        claim_token: None,
        claim_fence_epoch: None,
        claim_expires_at_ms: None,
        failure_class: None,
        last_error: None,
        revision: 5,
        created_at_ms: 21,
        updated_at_ms: 30,
        terminal_at_ms: Some(30),
        runtime_options_json: None,
        application_receipt: None,
    };

    let index = session_execution_index_from_outbox("session-recovery", &[primary, supplement]);

    assert_eq!(
        index.latest_execution_id,
        Some(runtime::session_ingress_graph_id(
            "session-recovery",
            "request-primary",
            "turn-primary"
        ))
    );
    assert_eq!(index.latest_status, Some(ExecutionLiveStatus::Complete));
    assert_eq!(
        index.terminal_ref.as_deref(),
        Some("turn-terminal:request-primary")
    );
}

#[test]
fn durable_materialization_cannot_reclassify_blocked_live_outcome_as_complete() {
    let execution_id = "session-ingress-graph:blocked".to_string();
    let volatile = SessionExecutionIndexProjection {
        session_id: "session-blocked".to_string(),
        executions: vec![SessionExecutionEntryProjection {
            execution_id: execution_id.clone(),
            graph_id: Some("execution-graph:blocked".to_string()),
            turn_id: Some("turn-blocked".to_string()),
            status: ExecutionLiveStatus::Error,
            live_revision: Some(7),
            started_at_ms: Some(10),
            updated_at_ms: 100,
            terminal_ref: Some("turn-terminal:blocked".to_string()),
        }],
        active_execution_ids: Vec::new(),
        latest_execution_id: Some(execution_id.clone()),
        latest_graph_id: Some("execution-graph:blocked".to_string()),
        latest_status: Some(ExecutionLiveStatus::Error),
        latest_live_revision: Some(7),
        last_progress_at_ms: Some(100),
        terminal_ref: Some("turn-terminal:blocked".to_string()),
    };
    let durable = SessionExecutionIndexProjection {
        session_id: "session-blocked".to_string(),
        executions: vec![SessionExecutionEntryProjection {
            execution_id: execution_id.clone(),
            graph_id: None,
            turn_id: Some("turn-blocked".to_string()),
            status: ExecutionLiveStatus::Complete,
            live_revision: None,
            started_at_ms: Some(10),
            updated_at_ms: 110,
            terminal_ref: Some("turn-terminal:blocked".to_string()),
        }],
        active_execution_ids: Vec::new(),
        latest_execution_id: Some(execution_id),
        latest_graph_id: None,
        latest_status: Some(ExecutionLiveStatus::Complete),
        latest_live_revision: None,
        last_progress_at_ms: Some(110),
        terminal_ref: Some("turn-terminal:blocked".to_string()),
    };

    let reconciled = reconcile_session_execution_indices(volatile, durable);

    assert_eq!(reconciled.latest_status, Some(ExecutionLiveStatus::Error));
    assert_eq!(reconciled.latest_live_revision, Some(7));
    assert_eq!(
        reconciled.latest_graph_id.as_deref(),
        Some("execution-graph:blocked")
    );
    assert_eq!(reconciled.last_progress_at_ms, Some(110));
    assert_eq!(
        reconciled.terminal_ref.as_deref(),
        Some("turn-terminal:blocked")
    );
    assert_eq!(reconciled.executions[0].status, ExecutionLiveStatus::Error);
}

#[test]
fn durable_terminal_entry_cannot_be_reopened_by_a_stale_live_checkpoint() {
    let execution_id = "session-ingress-graph:complete".to_string();
    let volatile = SessionExecutionIndexProjection {
        session_id: "session-complete".to_string(),
        executions: vec![SessionExecutionEntryProjection {
            execution_id: execution_id.clone(),
            graph_id: Some("execution-graph:complete".to_string()),
            turn_id: Some("turn-complete".to_string()),
            status: ExecutionLiveStatus::Finalizing,
            live_revision: Some(6),
            started_at_ms: Some(10),
            updated_at_ms: 100,
            terminal_ref: None,
        }],
        active_execution_ids: vec![execution_id.clone()],
        latest_execution_id: Some(execution_id.clone()),
        latest_graph_id: Some("execution-graph:complete".to_string()),
        latest_status: Some(ExecutionLiveStatus::Finalizing),
        latest_live_revision: Some(6),
        last_progress_at_ms: Some(100),
        terminal_ref: None,
    };
    let durable = SessionExecutionIndexProjection {
        session_id: "session-complete".to_string(),
        executions: vec![SessionExecutionEntryProjection {
            execution_id: execution_id.clone(),
            graph_id: None,
            turn_id: Some("turn-complete".to_string()),
            status: ExecutionLiveStatus::Complete,
            live_revision: None,
            started_at_ms: Some(10),
            updated_at_ms: 110,
            terminal_ref: Some("turn-terminal:complete".to_string()),
        }],
        active_execution_ids: Vec::new(),
        latest_execution_id: Some(execution_id),
        latest_graph_id: None,
        latest_status: Some(ExecutionLiveStatus::Complete),
        latest_live_revision: None,
        last_progress_at_ms: Some(110),
        terminal_ref: Some("turn-terminal:complete".to_string()),
    };

    let reconciled = reconcile_session_execution_indices(volatile, durable);

    assert_eq!(
        reconciled.latest_status,
        Some(ExecutionLiveStatus::Complete)
    );
    assert_eq!(
        reconciled.executions[0].status,
        ExecutionLiveStatus::Complete
    );
    assert!(reconciled.active_execution_ids.is_empty());
    assert_eq!(
        reconciled.latest_graph_id.as_deref(),
        Some("execution-graph:complete")
    );
}

#[test]
fn durable_turn_root_excludes_newer_child_agent_records_from_session_discovery() {
    let root_id = "session-ingress-graph:root".to_string();
    let child_id = "team:run:researcher:1".to_string();
    let volatile = SessionExecutionIndexProjection {
        session_id: "session-team".to_string(),
        executions: vec![
            SessionExecutionEntryProjection {
                execution_id: root_id.clone(),
                graph_id: Some("execution-graph:root".to_string()),
                turn_id: Some("turn-team".to_string()),
                status: ExecutionLiveStatus::Complete,
                live_revision: Some(9),
                started_at_ms: Some(10),
                updated_at_ms: 30,
                terminal_ref: Some("turn-terminal:root".to_string()),
            },
            SessionExecutionEntryProjection {
                execution_id: child_id.clone(),
                graph_id: Some("execution-graph:child".to_string()),
                turn_id: Some("turn-team".to_string()),
                status: ExecutionLiveStatus::Finalizing,
                live_revision: Some(99),
                started_at_ms: Some(11),
                updated_at_ms: 100,
                terminal_ref: None,
            },
        ],
        active_execution_ids: vec![child_id],
        latest_execution_id: Some("team:run:researcher:1".to_string()),
        latest_graph_id: Some("execution-graph:child".to_string()),
        latest_status: Some(ExecutionLiveStatus::Finalizing),
        latest_live_revision: Some(99),
        last_progress_at_ms: Some(100),
        terminal_ref: None,
    };
    let durable = SessionExecutionIndexProjection {
        session_id: "session-team".to_string(),
        executions: vec![SessionExecutionEntryProjection {
            execution_id: root_id.clone(),
            graph_id: None,
            turn_id: Some("turn-team".to_string()),
            status: ExecutionLiveStatus::Complete,
            live_revision: None,
            started_at_ms: Some(10),
            updated_at_ms: 40,
            terminal_ref: Some("turn-terminal:root".to_string()),
        }],
        active_execution_ids: Vec::new(),
        latest_execution_id: Some(root_id.clone()),
        latest_graph_id: None,
        latest_status: Some(ExecutionLiveStatus::Complete),
        latest_live_revision: None,
        last_progress_at_ms: Some(40),
        terminal_ref: Some("turn-terminal:root".to_string()),
    };

    let reconciled = reconcile_session_execution_indices(volatile, durable);

    assert_eq!(reconciled.latest_execution_id, Some(root_id.clone()));
    assert_eq!(
        reconciled.latest_graph_id.as_deref(),
        Some("execution-graph:root")
    );
    assert_eq!(
        reconciled.latest_status,
        Some(ExecutionLiveStatus::Complete)
    );
    assert!(reconciled.active_execution_ids.is_empty());
    assert_eq!(
        reconciled
            .executions
            .iter()
            .map(|entry| entry.execution_id.as_str())
            .collect::<Vec<_>>(),
        vec![root_id.as_str()]
    );
}

#[test]
fn supplemental_input_reuses_active_execution_without_registering_a_phantom_graph() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let (_cancellation, _guard) = service
        .install_active_turn_control(
            "turn-active",
            "session-supplement",
            Some("execution-active".to_string()),
        )
        .expect("active turn");
    let record = session::SessionRuntimeOutboxRecord {
        input_id: "input-supplement".to_string(),
        request_id: "request-supplement".to_string(),
        turn_id: "turn-supplement".to_string(),
        message_id: "message-supplement".to_string(),
        session_id: "session-supplement".to_string(),
        sequence: 2,
        session_generation: 1,
        decision: harness_contract::turn::InputRoutingDecision::SupplementCurrentTurn,
        target_turn_id: Some("turn-active".to_string()),
        classification_json: None,
        task_route_hint: None,
        status: session::SessionRuntimeInputStatus::Queued,
        runtime_commit_cursor: None,
        attempts: 0,
        next_attempt_at_ms: 0,
        claim_owner: None,
        claim_token: None,
        claim_fence_epoch: None,
        claim_expires_at_ms: None,
        failure_class: None,
        last_error: None,
        revision: 1,
        created_at_ms: 10,
        updated_at_ms: 10,
        terminal_at_ms: None,
        runtime_options_json: None,
        application_receipt: None,
    };

    assert_eq!(
        service.session_input_projection_identity(&record),
        (
            "execution-active".to_string(),
            "turn-active".to_string(),
            true
        )
    );
    assert!(service
        .runtime_services
        .session_execution_index("session-supplement")
        .active_execution_ids
        .is_empty());
}

#[tokio::test]
async fn durable_supplement_preserves_relation_proposal_for_runtime_policy() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let turn_id = TurnId::from_string("turn-active");
    let stream = runtime::SessionInputStream::new("session-supplement");
    stream.set_active_turn(Some(turn_id.clone()));
    service.install_test_session_input("session-supplement", stream);
    let record = session::SessionRuntimeOutboxRecord {
        input_id: "input-supplement".to_string(),
        request_id: "request-supplement".to_string(),
        turn_id: "turn-carrier".to_string(),
        message_id: "message-supplement".to_string(),
        session_id: "session-supplement".to_string(),
        sequence: 2,
        session_generation: 1,
        decision: InputRoutingDecision::SupplementCurrentTurn,
        target_turn_id: Some(turn_id.to_string()),
        classification_json: Some(
            serde_json::json!({
                "relation_proposal": {
                    "candidate": "new_task",
                    "confidence_basis_points": 9000,
                    "reasons": ["explicit_test"]
                }
            })
            .to_string(),
        ),
        task_route_hint: None,
        status: session::SessionRuntimeInputStatus::Running,
        runtime_commit_cursor: None,
        attempts: 1,
        next_attempt_at_ms: 0,
        claim_owner: Some("worker".to_string()),
        claim_token: Some("claim".to_string()),
        claim_fence_epoch: Some(1),
        claim_expires_at_ms: Some(10_000),
        failure_class: None,
        last_error: None,
        revision: 2,
        created_at_ms: 10,
        updated_at_ms: 10,
        terminal_at_ms: None,
        runtime_options_json: None,
        application_receipt: None,
    };

    service
        .deliver_durable_session_input_view(
            &record,
            "append this work".to_string(),
            SessionInputStatus::AttachedToTurn,
        )
        .await
        .expect("durable supplement projected");

    let record = service
        .test_session_input("session-supplement")
        .and_then(|stream| stream.record_snapshot(&SessionInputId::from_string("input-supplement")))
        .expect("projected record");
    assert_eq!(
        record
            .relation_proposal
            .expect("relation proposal")
            .candidate,
        InputRelationKind::NewTask
    );
    assert_eq!(
        record.cursor,
        Some(harness_contract::turn::SessionInputCursor::new(1, 2))
    );
}

#[test]
fn progress_input_materializes_bounded_mission_projection_without_provider_wait() {
    let service = test_runtime_service(Arc::new(ActiveSessionDirectory::default()), None);
    let projection = service
        .responsive_input_projection(
            "session-progress",
            Some(&InputRelationProposal {
                candidate: InputRelationKind::Progress,
                confidence_basis_points: 9_000,
                reasons: vec!["progress_query".to_string()],
                target_ref: None,
            }),
        )
        .expect("progress projection");

    assert_eq!(projection["kind"], "session_input.progress");
    assert_eq!(projection["session_id"], "session-progress");
    assert!(projection["mission"].is_object());
    assert!(projection["execution"]["executions"].is_array());
}
