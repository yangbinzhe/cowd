// Legacy API behavior shard; included into one shared test scope.
use super::*;
    use axum::{
        body::to_bytes,
        body::Body,
        http::{Request, StatusCode},
        Extension,
    };
    use memory::config::{BudgetConfig, StoreConfig};
    use model_protocol::provider_config::{ProviderConfig, ProvidersConfig};
    use runtime::{ContextProfile, ExecutionGraphHost, ResumeContextSource};
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Instant;
    use tower::ServiceExt;

    struct ApprovalResumeTestExecutor;

    struct CrossPlaneApprovalTestBackend {
        calls: Arc<std::sync::atomic::AtomicUsize>,
    }
    async fn attach_test_writer(state: &AppState, session_id: &str, observer_id: &str) {
        let principal = AuthenticatedPrincipal(test_human_principal());
        let attached = state
            .services
            .session
            .attach_session_value(
                session_id,
                &surface_actor_id(&principal, observer_id),
                "test",
                Some("writer"),
            )
            .await;
        assert_eq!(attached["ok"], true);
    }

    fn assert_control_plane_readiness_accounting(json: &serde_json::Value) {
        let checks = json["readiness"]["checks"]
            .as_array()
            .expect("readiness checks");
        let required = checks
            .iter()
            .filter(|check| check["required"].as_bool() == Some(true))
            .collect::<Vec<_>>();
        let ready = required
            .iter()
            .filter(|check| check["status"] == "ready")
            .count() as u64;
        let blocked = required.len() as u64 - ready;
        let total = required.len() as u64;
        let score = if total == 0 { 100 } else { ready * 100 / total };

        assert_eq!(json["diagnostics"]["required_check_count"], total);
        assert_eq!(json["diagnostics"]["ready_required_count"], ready);
        assert_eq!(json["diagnostics"]["blocked_required_count"], blocked);
        assert_eq!(json["diagnostics"]["readiness_score"], score);
        assert_eq!(json["diagnostics"]["production_ready"], blocked == 0);
        assert_eq!(json["readiness"]["required_total"], total);
        assert_eq!(json["readiness"]["required_ready"], ready);
        assert_eq!(json["readiness"]["required_blocked"], blocked);
        assert_eq!(json["readiness"]["score"], score);
        assert_eq!(json["readiness"]["production_ready"], blocked == 0);
        assert_eq!(
            json["readiness"]["blocked"]
                .as_array()
                .expect("blocked readiness checks")
                .len() as u64,
            blocked
        );
    }

    #[test]
    fn surface_capability_requests_preserve_intent_for_broker_catalog_validation() {
        assert_eq!(
            validate_surface_capability_request(
                "webui",
                vec!["app.read".to_string(), "app.read".to_string()],
            )
            .expect("well-formed request"),
            vec!["app.read".to_string()]
        );
        assert!(validate_surface_capability_request("", vec!["app.read".to_string()]).is_err());
        assert!(validate_surface_capability_request("webui", vec![" ".to_string()]).is_err());
    }

    #[async_trait::async_trait]
    impl runtime::execution_core::ScopedNodeBackend for CrossPlaneApprovalTestBackend {
        async fn execute(
            &self,
            ticket: &runtime::execution_core::NodeExecutionTicket,
        ) -> Result<
            runtime::execution_core::NodeExecutionOutcome,
            runtime::execution_core::NodeExecutorError,
        > {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(runtime::execution_core::NodeExecutionOutcome::new(
                harness_contract::execution_graph::ExecutionNodeResult {
                    status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                    result_ref: Some(format!("cross-plane-sent:{}", ticket.node_id)),
                    summary: Some("Cross-plane fixture completed".to_string()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    #[async_trait::async_trait]
    impl runtime::execution_core::NodeExecutor for ApprovalResumeTestExecutor {
        fn kind(&self) -> &str {
            "approval_resume_test_tool"
        }

        fn validate(
            &self,
            _node: &harness_contract::execution_graph::ExecutionNodeSpec,
        ) -> Result<(), runtime::execution_core::NodeExecutorError> {
            Ok(())
        }

        async fn start(
            &self,
            context: runtime::execution_core::NodeExecutionContext,
        ) -> Result<
            runtime::execution_core::NodeExecutionTicket,
            runtime::execution_core::NodeExecutorError,
        > {
            Ok(runtime::execution_core::NodeExecutionTicket {
                graph_id: context.graph.id.clone(),
                node_id: context.node.id,
                executor_kind: self.kind().to_string(),
                service_class: context.graph.service_class,
                attempt: context.attempt,
                idempotency_key: context.node.idempotency_key,
                payload_ref: context.node.payload_ref,
            })
        }

        async fn poll_or_await(
            &self,
            ticket: &runtime::execution_core::NodeExecutionTicket,
        ) -> Result<
            runtime::execution_core::NodeExecutionOutcome,
            runtime::execution_core::NodeExecutorError,
        > {
            Ok(runtime::execution_core::NodeExecutionOutcome::new(
                harness_contract::execution_graph::ExecutionNodeResult {
                    status: harness_contract::execution_graph::ExecutionNodeStatus::Completed,
                    result_ref: Some(format!("tool-result:{}", ticket.node_id)),
                    summary: Some("Tool fixture completed".to_string()),
                    evidence_refs: Vec::new(),
                    failure: None,
                    usage: Default::default(),
                    finished_at_ms: 1,
                },
            ))
        }
    }

    #[derive(Clone, Default)]
    struct CapturedTraceEvents {
        events: Arc<std::sync::Mutex<Vec<String>>>,
    }

    static TRACE_CAPTURE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();
    static MISSION_ROUTE_LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> =
        std::sync::OnceLock::new();

    fn trace_capture_lock() -> &'static tokio::sync::Mutex<()> {
        TRACE_CAPTURE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    fn mission_route_lock() -> &'static tokio::sync::Mutex<()> {
        MISSION_ROUTE_LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
    }

    impl CapturedTraceEvents {
        fn lines(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    struct TraceFieldVisitor {
        fields: Vec<String>,
    }

    impl tracing::field::Visit for TraceFieldVisitor {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            self.fields.push(format!("{}={value:?}", field.name()));
        }
    }

    impl<S> tracing_subscriber::Layer<S> for CapturedTraceEvents
    where
        S: tracing::Subscriber,
    {
        fn register_callsite(
            &self,
            _metadata: &'static tracing::Metadata<'static>,
        ) -> tracing::subscriber::Interest {
            tracing::subscriber::Interest::always()
        }

        fn enabled(
            &self,
            _metadata: &tracing::Metadata<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) -> bool {
            true
        }

        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = TraceFieldVisitor { fields: Vec::new() };
            event.record(&mut visitor);
            self.events.lock().unwrap().push(format!(
                "{} {} {}",
                event.metadata().level(),
                event.metadata().target(),
                visitor.fields.join(" ")
            ));
        }
    }

    fn test_profile_manager() -> Arc<ProfileManager> {
        let dir = std::env::temp_dir().join(format!("cowd-api-profiles-{}", uuid::Uuid::new_v4()));
        let manager = Arc::new(ProfileManager::new_with_profiles_dir(dir));
        manager.initialize().unwrap();
        manager
    }

    fn test_session_repository(
        sessions: Arc<HotSessionPool>,
        store: Option<Arc<UnifiedSessionStore>>,
        event_bus: Arc<SessionProjectionHub>,
    ) -> Arc<SessionRepository> {
        Arc::new(SessionRepository::new(sessions, store, event_bus))
    }

    fn test_provider_registry() -> Arc<runtime::ProviderRegistry> {
        Arc::new(
            runtime::ProviderRegistry::new(ProvidersConfig {
                providers: HashMap::from([(
                    "test".to_string(),
                    ProviderConfig {
                        name: "test".to_string(),
                        // Tests never submit this provider.  A closed loopback
                        // endpoint keeps accidental future calls deterministic.
                        base_url: "http://127.0.0.1:9/v1".to_string(),
                        api_key: "test".to_string(),
                        models: vec![
                            crate::DEFAULT_MODEL_ALIAS.to_string(),
                            "test-model".to_string(),
                            "patched-model".to_string(),
                        ],
                        protocol: Some("completions".to_string()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            })
            .expect("valid inert test provider registry"),
        )
    }

    pub(crate) fn publish_test_session_policy(
        services: &crate::services::GatewayServices,
        session_id: &str,
    ) {
        let policy = harness_contract::policy::SessionExecutionPolicy::from_profile(
            harness_contract::policy::AutonomyProfileId::Supervised,
            1,
            harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
        );
        services
            .runtime
            .as_ref()
            .expect("test Runtime service")
            .runtime_services()
            .publish_session_execution_policy(
                session_id.to_string(),
                runtime::permissions::SessionExecutionPolicyControl::from_policy(policy),
            );
    }

    fn seed_test_task(
        services: &crate::services::GatewayServices,
        task_id: &str,
        objective: &str,
    ) -> runtime::TaskAggregate {
        let session_id = "test-session";
        publish_test_session_policy(services, session_id);
        services
            .task
            .create(
                task_id.to_string(),
                services
                    .task
                    .workspace_default_mission_id()
                    .expect("Runtime-backed TaskService"),
                session_id.to_string(),
                format!("test-turn-{task_id}"),
                objective.to_string(),
                vec![harness_contract::reality::EvidenceRef::observed(
                    "test_fixture",
                    format!("test://tasks/{task_id}"),
                )],
            )
            .expect("seed canonical Runtime task")
            .aggregate
    }

    fn test_services(
        session_repository: Arc<SessionRepository>,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
    ) -> Arc<crate::services::GatewayServices> {
        test_services_for_workspace(
            session_repository,
            surface_host,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        )
    }

    fn test_services_for_workspace(
        session_repository: Arc<SessionRepository>,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
        tool_workspace_root: PathBuf,
    ) -> Arc<crate::services::GatewayServices> {
        test_services_for_workspace_with_config_home(
            session_repository,
            surface_host,
            tool_workspace_root,
            isolated_test_config_home(),
        )
    }

    fn test_services_for_workspace_with_config_home(
        session_repository: Arc<SessionRepository>,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
        tool_workspace_root: PathBuf,
        config_home: PathBuf,
    ) -> Arc<crate::services::GatewayServices> {
        let sessions = Arc::new(HotSessionPool::new());
        let runtime_services =
            runtime::RuntimeServices::in_memory().expect("test runtime services");
        let runtime_store = session_repository.test_unified_store().unwrap_or_else(|| {
            Arc::new(UnifiedSessionStore::open_in_memory().expect("test session store"))
        });
        let presence_ledger = Arc::new(
            crate::services::session_service::presence::SessionPresenceLedger::with_store(
                Arc::clone(&runtime_store),
            ),
        );
        let session_runtime_port =
            crate::session_runtime_data_port::GatewaySessionRuntimePort::new();
        runtime_services
            .install_session_ports(
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
                session_runtime_port.clone(),
            )
            .expect("test session router");
        let runtime = Arc::new(
            crate::runtime_service::RuntimeService::new(
                Arc::clone(&sessions),
                Arc::new(session::SessionLeaseRegistry::default()),
                session_runtime_port.clone(),
                session_repository.test_event_bus(),
                Instant::now(),
                Some("test-model".to_string()),
                test_provider_registry(),
                Arc::new(runtime::UpgradeCoordinator::new()),
                runtime_services,
            )
            .expect("test runtime service")
            .with_tool_host(Arc::new(
                tools::ToolHost::builtin("gateway-test-runtime", tool_workspace_root)
                    .with_authorization_lease_verifier(Arc::new(
                        runtime::AuthorizationNegotiator::verify_lease_signature,
                    )),
            )),
        );
        let session_activation = Arc::new(
            crate::services::session_service::activation::SessionActivationCoordinator::new(
                Arc::clone(&runtime),
                session_repository,
                presence_ledger,
                Arc::new(runtime::session_lifecycle::SessionWorkingSetManager::new(
                    runtime::session_lifecycle::SessionLifecycleConfig::default(),
                )),
                None,
                runtime::SessionRecoveryConfig::default(),
            ),
        );
        let services = Arc::new(crate::services::GatewayServices::new_with_config_home(
            runtime,
            session_activation,
            crate::session_runtime_bridge::SessionWorkerSupervisor::for_tests(),
            surface_host.unwrap_or_else(|| {
                Arc::new(
                    crate::surface_host::SurfaceHost::baseline()
                        .expect("test Surface message ledger"),
                )
            }),
            None,
            config_home,
        ));
        session_runtime_port
            .bind(&services.session)
            .expect("test Runtime port binds the routed SessionService");
        services
    }

    pub(crate) fn test_state() -> Arc<AppState> {
        let sessions = Arc::new(HotSessionPool::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new(); // returns Arc<Self>
        let session_store = Arc::new(
            UnifiedSessionStore::open_in_memory().expect("test session store should open"),
        );
        let session_repository =
            test_session_repository(sessions.clone(), Some(session_store), event_bus.clone());
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    pub(crate) fn test_state_with_app_platform(
        app_platform: Arc<crate::app_platform::GatewayAppPlatform>,
    ) -> Arc<AppState> {
        let mut state = Arc::try_unwrap(test_state())
            .unwrap_or_else(|_| panic!("fresh test state must be uniquely owned"));
        state.services = Arc::new(
            state
                .services
                .as_ref()
                .clone()
                .with_app_platform(app_platform),
        );
        Arc::new(state)
    }

    fn test_state_with_config(config: serde_json::Value) -> Arc<AppState> {
        test_state_with_config_and_runtime(config, None)
    }

    fn test_state_with_config_and_runtime(
        config: serde_json::Value,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
    ) -> Arc<AppState> {
        test_state_with_config_runtime_and_workspace(
            config,
            surface_host,
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
        )
    }

    fn test_state_with_config_runtime_and_workspace(
        config: serde_json::Value,
        surface_host: Option<Arc<crate::surface_host::SurfaceHost>>,
        workspace_root: PathBuf,
    ) -> Arc<AppState> {
        let sessions = Arc::new(HotSessionPool::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository = test_session_repository(sessions.clone(), None, event_bus.clone());
        let config_home = isolated_test_config_home_with_config(&config);
        Arc::new(AppState {
            tool_registry: tools,
            config: Some(config),
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: workspace_root.clone(),
            config_home,
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services_for_workspace(session_repository, surface_host, workspace_root),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    fn unique_test_workspace(label: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("cowd-{label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn isolated_test_config_home() -> PathBuf {
        unique_test_workspace("config-home")
    }

    fn isolated_test_config_home_with_config(config: &serde_json::Value) -> PathBuf {
        let path = isolated_test_config_home();
        let rendered = serde_yaml::to_string(config).expect("test config renders as yaml");
        std::fs::write(path.join("config.yaml"), rendered).expect("test config writes");
        path
    }

    fn test_state_with_store(store: Arc<UnifiedSessionStore>) -> Arc<AppState> {
        let sessions = Arc::new(HotSessionPool::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository =
            test_session_repository(sessions.clone(), Some(store.clone()), event_bus.clone());
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services(session_repository, None),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    fn test_state_with_store_and_workspace(
        store: Arc<UnifiedSessionStore>,
        workspace_root: PathBuf,
        config_home: PathBuf,
    ) -> Arc<AppState> {
        let sessions = Arc::new(HotSessionPool::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let session_repository =
            test_session_repository(sessions.clone(), Some(store.clone()), event_bus.clone());
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: workspace_root.clone(),
            config_home: config_home.clone(),
            profile_id: "enterprise".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services_for_workspace_with_config_home(
                session_repository,
                None,
                workspace_root,
                config_home.clone(),
            ),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    fn test_memory_config(sqlite_path: &std::path::Path) -> memory::MemoryConfig {
        memory::MemoryConfig {
            store: StoreConfig {
                sqlite_path: sqlite_path.to_path_buf(),
                blob_dir: sqlite_path.parent().unwrap().join("blobs"),
                ..Default::default()
            },
            budget: BudgetConfig {
                context_window: 8_000,
                reserved_system: 2_000,
                reserved_response: 1_000,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn test_state_with_memory(memory_manager: Arc<CognitiveContextManager>) -> Arc<AppState> {
        let tools = Arc::new(ToolCatalog::builtin());
        let task_runtime = runtime::RuntimeServices::in_memory().expect("test task runtime");
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: Arc::new(
                crate::services::GatewayServices::with_memory_for_tests(memory_manager)
                    .with_task_runtime_for_tests(task_runtime),
            ),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    fn test_state_with_memory_and_workspace(
        memory_manager: Arc<CognitiveContextManager>,
        workspace_root: PathBuf,
    ) -> Arc<AppState> {
        let tools = Arc::new(ToolCatalog::builtin());
        let task_runtime = runtime::RuntimeServices::in_memory().expect("test task runtime");
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: workspace_root.clone(),
            config_home: isolated_test_config_home(),
            profile_id: "default".to_string(),
            profile_manager: test_profile_manager(),
            services: Arc::new(
                crate::services::GatewayServices::with_memory_for_tests(memory_manager)
                    .with_task_runtime_for_tests(task_runtime),
            ),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    fn test_state_with_workspace(workspace_root: PathBuf, config_home: PathBuf) -> Arc<AppState> {
        let sessions = Arc::new(HotSessionPool::new());
        let tools = Arc::new(ToolCatalog::builtin());
        let event_bus = SessionProjectionHub::new();
        let store = Arc::new(
            UnifiedSessionStore::open_in_memory()
                .expect("workspace-backed API tests require the production Session contract"),
        );
        let session_repository =
            test_session_repository(sessions.clone(), Some(store), event_bus.clone());
        Arc::new(AppState {
            tool_registry: tools,
            config: None,
            static_webui: crate::gateway_static::StaticWebUiSource::missing_config(),
            auth_token: None,
            workspace_root: workspace_root.clone(),
            config_home: config_home.clone(),
            profile_id: "enterprise".to_string(),
            profile_manager: test_profile_manager(),
            services: test_services_for_workspace_with_config_home(
                session_repository,
                None,
                workspace_root,
                config_home,
            ),
            session_lease_registry: Some(Arc::new(session::SessionLeaseRegistry::default())),
            live_registry: Arc::new(live_routes::LiveRegistry::new()),
        })
    }

    fn activate_test_provider_config(state: &Arc<AppState>) {
        let config = state
            .services
            .system
            .runtime_config(&state.workspace_root, &state.config_home)
            .expect("test provider config");
        state
            .services
            .runtime
            .as_ref()
            .expect("test runtime service")
            .provider_registry()
            .replace(config.providers().clone())
            .expect("activate test provider config");
    }

    #[tokio::test]
    async fn session_service_exposes_session_queries_without_repository_handles() {
        let state = test_state_with_store(Arc::new(UnifiedSessionStore::open_in_memory().unwrap()));

        let _projection_hub = state.services.session.event_bus();
        assert!(state.services.session.has_unified_store());
        assert_eq!(
            state
                .services
                .session
                .list_stored_sessions()
                .await
                .expect("session query")
                .expect("durable store")
                .len(),
            0
        );
    }

    #[tokio::test]
    async fn session_history_index_is_bounded_typed_and_body_free() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "surface-history-index";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        for sequence in 0..3 {
            store
                .insert_message(&session::SessionMessage {
                    stable_message_id: format!("message-{sequence}"),
                    session_id: session_id.to_string(),
                    sequence,
                    role: if sequence % 2 == 0 {
                        "user".to_string()
                    } else {
                        "assistant".to_string()
                    },
                    content_json: format!(
                        "[{{\"type\":\"text\",\"text\":\"secret body {sequence}\"}}]"
                    ),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: sequence as u64 + 1,
                })
                .await
                .unwrap();
        }
        let app = api_router(test_state_with_store(store));
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/history-index?metadata_limit=2&card_limit=8"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let projection: harness_contract::projection::SessionHistoryIndexProjection =
            serde_json::from_slice(&body).unwrap();
        assert_eq!(projection.session_id, session_id);
        assert_eq!(projection.total_messages, 3);
        assert_eq!(projection.recent_metadata.len(), 2);
        assert!(projection.projection_generation > 0);
        assert!(
            !String::from_utf8_lossy(&body).contains("secret body"),
            "history index must not materialize transcript bodies"
        );
    }

    #[tokio::test]
    async fn agent_catalog_route_consumes_runtime_definition_projection() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/agents/catalog")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("catalog json");
        assert_eq!(value["source"], "runtime.definition_catalog");
        assert!(value["agents"].is_array());
        assert!(value.get("working_directory").is_none());
        assert!(value["summary"].get("shadowed").is_none());
    }

    #[tokio::test]
    async fn team_template_route_consumes_runtime_definition_projection() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/team-templates")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let value: serde_json::Value = serde_json::from_slice(&body).expect("templates json");
        assert_eq!(value["source"], "runtime.definition_catalog");
        let templates = value["templates"].as_array().expect("template list");
        assert!(templates.len() >= 8);
        assert!(templates.iter().any(|template| {
            template["revision_ref"]["template_id"] == "builtin/cowd/parallel-research-synthesis"
        }));
    }

    #[tokio::test]
    async fn session_execution_and_evidence_routes_use_durable_turn_binding() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "durable-execution-route-session";
        let request_id = "durable-execution-route-request";
        let turn_id = "durable-execution-route-turn";
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".into()),
            ))
            .await
            .unwrap();
        let session_generation = store
            .get_session_input_admission(session_id)
            .await
            .unwrap()
            .expect("created Session has durable input admission")
            .generation;
        store
            .append_ingress_with_runtime_outbox(
                session_id,
                "user",
                Some("[{\"type\":\"text\",\"text\":\"durable route check\"}]"),
                42,
                &session::SessionRuntimeOutboxRequest {
                    input_id: "durable-execution-route-input".to_string(),
                    request_id: request_id.to_string(),
                    turn_id: turn_id.to_string(),
                    message_id: "durable-execution-route-message".to_string(),
                    session_generation,
                    decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                    target_turn_id: None,
                    classification_json: None,
                    task_route_hint: None,
                    created_at_ms: 42,
                    runtime_options_json: Some("{\"profile\":\"main_turn\"}".to_string()),
                },
            )
            .await
            .unwrap();
        let execution_id = runtime::session_ingress_graph_id(session_id, request_id, turn_id);
        let state = test_state_with_store(store);
        state
            .services
            .runtime
            .as_ref()
            .expect("runtime service")
            .runtime_services()
            .record_live_execution(session_id, execution_id.clone(), turn_id.to_string());
        let app = api_router(state);

        let index = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/execution"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(index.status(), StatusCode::OK);
        let index_body = to_bytes(index.into_body(), usize::MAX).await.unwrap();
        let index_json: serde_json::Value = serde_json::from_slice(&index_body).unwrap();
        assert_eq!(index_json["latest_execution_id"], execution_id);
        assert_eq!(index_json["latest_status"], "queued");
        assert_eq!(
            index_json["active_execution_ids"],
            serde_json::json!([execution_id])
        );

        let live = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/execution/live"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
        let live_body = to_bytes(live.into_body(), usize::MAX).await.unwrap();
        let live_json: serde_json::Value = serde_json::from_slice(&live_body).unwrap();
        assert_eq!(live_json["execution_id"], execution_id);
        assert_eq!(live_json["live"]["status"], "queued");

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/evidence"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);
        let evidence_body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&evidence_body).unwrap();
        assert_eq!(evidence_json["freshness"], "live");
        assert_eq!(evidence_json["turns"][0]["turn_id"], turn_id);
        assert_eq!(evidence_json["turns"][0]["execution_id"], execution_id);
        assert_eq!(
            evidence_json["turns"][0]["evidence_refs"],
            serde_json::json!([])
        );

        let turn = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/sessions/{session_id}/turns/{turn_id}/evidence"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(turn.status(), StatusCode::OK);
        let turn_body = to_bytes(turn.into_body(), usize::MAX).await.unwrap();
        let turn_json: serde_json::Value = serde_json::from_slice(&turn_body).unwrap();
        assert_eq!(turn_json["execution_id"], execution_id);
    }

    #[tokio::test]
    async fn session_evidence_projection_preserves_durable_order_for_large_history() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let session_id = "ordered-evidence-history";
        store
            .create_session(&new_api_session_record(session_id, None))
            .await
            .unwrap();
        let session_generation = store
            .get_session_input_admission(session_id)
            .await
            .unwrap()
            .expect("session admission")
            .generation;
        for index in 0..100_u64 {
            store
                .append_ingress_with_runtime_outbox(
                    session_id,
                    "user",
                    Some(&format!(
                        "[{{\"type\":\"text\",\"text\":\"ordered input {index}\"}}]"
                    )),
                    index,
                    &session::SessionRuntimeOutboxRequest {
                        input_id: format!("ordered-input-{index:03}"),
                        request_id: format!("ordered-request-{index:03}"),
                        turn_id: format!("ordered-turn-{index:03}"),
                        message_id: format!("ordered-message-{index:03}"),
                        session_generation,
                        decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
                        target_turn_id: None,
                        classification_json: None,
                        task_route_hint: None,
                        created_at_ms: index,
                        runtime_options_json: None,
                    },
                )
                .await
                .unwrap();
        }

        let response = api_router(test_state_with_store(store))
            .oneshot(
                Request::builder()
                    .uri(format!("/api/sessions/{session_id}/evidence"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let turns = value["turns"].as_array().expect("turn projections");
        assert_eq!(turns.len(), 100);
        for (index, turn) in turns.iter().enumerate() {
            assert_eq!(
                turn["turn_id"],
                format!("ordered-turn-{index:03}"),
                "bounded parallel projection must retain durable ingress order"
            );
        }
    }

    #[tokio::test]
    async fn evidence_batch_resolver_preserves_order_and_isolates_unavailable_items() {
        let refs = (0..100)
            .map(|index| format!("unsupported://ordered-{index:03}"))
            .collect::<Vec<_>>();
        let response = api_router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/evidence/resolve/batch")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "explicit-session",
                            "refs": refs,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let items = value["items"].as_array().expect("evidence items");
        assert_eq!(items.len(), 100);
        for (index, item) in items.iter().enumerate() {
            assert_eq!(item["ref"], format!("unsupported://ordered-{index:03}"));
            assert_eq!(item["status"], "unavailable");
        }
    }

    #[tokio::test]
    async fn runtime_outbox_management_reports_poison_and_retries_both_directions() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record("outbox-session", None))
            .await
            .unwrap();
        let session_generation = store
            .get_session_input_admission("outbox-session")
            .await
            .unwrap()
            .expect("created Session has durable input admission")
            .generation;
        let state = test_state_with_store(Arc::clone(&store));
        let request = session::SessionRuntimeOutboxRequest {
            input_id: "ingress-poison-input".to_string(),
            request_id: "ingress-poison".to_string(),
            turn_id: "turn-1".to_string(),
            message_id: "user-1".to_string(),
            session_generation,
            decision: harness_contract::turn::InputRoutingDecision::StartNewTurn,
            target_turn_id: None,
            classification_json: None,
            task_route_hint: None,
            created_at_ms: 1,
            runtime_options_json: None,
        };
        store
            .append_ingress_with_runtime_outbox(
                "outbox-session",
                "user",
                Some("[{\"type\":\"text\",\"text\":\"hello\"}]"),
                1,
                &request,
            )
            .await
            .unwrap();
        let ingress_claim = store
            .claim_session_runtime_outbox("test-worker", 1, 10, 1)
            .await
            .unwrap()
            .pop()
            .unwrap();
        store
            .fail_session_runtime_outbox(
                "ingress-poison",
                "test-worker",
                ingress_claim.session_generation,
                ingress_claim.claim_token.as_deref().expect("claim token"),
                ingress_claim.revision,
                session::OutboxFailureClass::CorruptPayload,
                "bad payload",
                2,
                1,
                2,
            )
            .await
            .unwrap();
        let delivery = state
            .services
            .runtime
            .as_ref()
            .unwrap()
            .runtime_services()
            .session_terminal_delivery();
        delivery
            .enqueue(
                "terminal-poison",
                "assistant-1",
                "outbox-session",
                9,
                "bad payload",
            )
            .unwrap();
        let terminal_claim = delivery
            .claim("test-worker", 1, 10, 1)
            .unwrap()
            .pop()
            .unwrap();
        delivery
            .fail(
                "terminal-poison",
                "test-worker",
                terminal_claim.revision,
                runtime::RuntimeSessionOutboxFailureClass::CorruptPayload,
                "bad payload",
                2,
                1,
                2,
            )
            .unwrap();

        let app = api_router(state);
        let status = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/outbox")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(status.status(), StatusCode::OK);
        let body = to_bytes(status.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["healthy"], false);
        assert_eq!(json["ingress"]["poison"][0]["request_id"], "ingress-poison");
        assert_eq!(
            json["terminal"]["poison"][0]["terminal_id"],
            "terminal-poison"
        );

        for (direction, id) in [
            ("ingress", "ingress-poison"),
            ("terminal", "terminal-poison"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(format!("/api/runtime/outbox/{direction}/{id}/retry"))
                        .header("content-type", "application/json")
                        .body(Body::from(
                            serde_json::json!({"reason":"repaired"}).to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
    }

    fn test_temp_dir(label: &str) -> PathBuf {
        // Several API fixtures host Unix sockets, whose path is capped at
        // SUN_LEN. Keep the diagnostic prefix while bounding the full path
        // independently of the validation lane's TMPDIR.
        let short_label = label.chars().take(8).collect::<String>();
        let path =
            std::env::temp_dir().join(format!("cowd-api-{short_label}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn independent_browser_sessions_from_one_credential_remain_valid() {
        let config_home = test_temp_dir("browser-auth");
        let credential = "shared-browser-test-credential";

        let (first_session, first_entitlement) =
            issue_web_session(&config_home, credential, "webui", Vec::new())
                .expect("first browser session");
        let (second_session, second_entitlement) =
            issue_web_session(&config_home, credential, "webui", Vec::new())
                .expect("second browser session");

        let principal_for = |session: &str| {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(
                axum::http::header::COOKIE,
                format!("{WEB_SESSION_COOKIE}={session}")
                    .parse()
                    .expect("browser cookie header"),
            );
            web_session_principal(&config_home, &headers, Some(credential))
                .expect("browser session remains valid")
        };
        let first_principal = principal_for(&first_session);
        let second_principal = principal_for(&second_session);

        assert_eq!(first_principal.claims().principal_id, "local-human");
        assert_eq!(second_principal.claims().principal_id, "local-human");
        assert_eq!(
            first_principal.claims().credential_epoch,
            second_principal.claims().credential_epoch
        );
        assert_eq!(
            first_entitlement.credential_epoch,
            second_entitlement.credential_epoch
        );
        assert_eq!(WEB_SESSION_TTL_SECONDS, 86_400);

        let _ = std::fs::remove_dir_all(config_home);
    }

    fn gateway_test_actor() -> String {
        "principal:local-human".to_string()
    }

    fn cross_plane_intent_from_action(action: &serde_json::Value) -> serde_json::Value {
        let mut intent = action.clone();
        intent
            .as_object_mut()
            .expect("cross-plane action projection must be an object")
            .remove("actor_principal");
        intent
    }

    async fn wait_for_harness_eval_route_status(
        app: axum::Router,
        run_id: &str,
        expected: &str,
    ) -> serde_json::Value {
        // The quick harness runs in a real background worker. Under the full
        // Gateway suite hundreds of concurrent tests can legitimately delay
        // it beyond five seconds; keep the wait bounded while never treating
        // an honest `running` state as completion.
        for _ in 0..1_200 {
            let detail = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(format!("/api/harness-eval/runs/{run_id}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            let body = to_bytes(detail.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            if json["run"]["status"] == expected {
                return json;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        let detail = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/harness-eval/runs/{run_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn health_returns_ok() {
        let state = test_state();
        let app = api_router(state);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn gateway_capability_contract_endpoints_are_available() {
        let app = api_router(test_state());
        for uri in [
            "/api/gateway/capability-contract",
            "/api/gateway/openapi.json",
            "/api/gateway/openai-tools",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            match uri {
                "/api/gateway/capability-contract" => {
                    assert_eq!(json["kind"], "gateway.capability_contract");
                    assert!(json["route_count"].as_u64().unwrap_or_default() > 50);
                    assert!(json["capabilities"].as_array().is_some_and(|items| {
                        items.iter().any(|capability| {
                            capability["http"]["path"] == "/api/gateway/openapi.json"
                        })
                    }));
                }
                "/api/gateway/openapi.json" => {
                    assert_eq!(json["openapi"], "3.1.0");
                    assert!(json["paths"]["/api/gateway/capability-contract"]["get"].is_object());
                }
                "/api/gateway/openai-tools" => {
                    assert_eq!(json["kind"], "gateway.openai_tools");
                    assert!(json["tools"].as_array().is_some_and(|items| {
                        items.iter().all(|tool| {
                            tool["type"] == "function"
                                && tool["function"]["name"].as_str().is_some()
                                && tool["function"]["parameters"]["type"] == "object"
                        })
                    }));
                }
                _ => unreachable!(),
            }
        }
    }

    #[tokio::test]
    async fn forged_skill_maintenance_counts_have_no_authoritative_route() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/skills/maintenance/evaluate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "route-req-1",
                            "skill_id": "plan-review",
                            "selected_count": 5,
                            "success_count": 3,
                            "failure_count": 1,
                            "correction_count": 2,
                            "activation_gap_count": 0,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(
            matches!(
                response.status(),
                StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
            ),
            "client-supplied counters must not reach a maintenance authority"
        );
    }

    #[tokio::test]
    async fn skill_lifecycle_routes_create_and_list_real_runs() {
        let workspace = test_temp_dir("skill-lifecycle-workspace");
        let config_home = test_temp_dir("skill-lifecycle-config");
        let skill_root = workspace.join(".cowd").join("skills").join("route-demo");
        std::fs::create_dir_all(&skill_root).unwrap();
        std::fs::write(
            skill_root.join("SKILL.md"),
            "---\nname: route-demo\ndescription: Route demo\n---\n# Route Demo\n",
        )
        .unwrap();

        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));
        let validate = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/skills/local:route-demo/actions/validate")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"session_id": "route-test"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(validate.status(), StatusCode::OK);
        let validate_body = to_bytes(validate.into_body(), usize::MAX).await.unwrap();
        let validate_json: serde_json::Value = serde_json::from_slice(&validate_body).unwrap();
        assert_eq!(validate_json["kind"], "skills.action.receipt");
        assert_eq!(validate_json["receipt"]["status"], "succeeded");
        let run_id = validate_json["run"]["run_id"].as_str().unwrap();

        let runs = app
            .oneshot(
                Request::builder()
                    .uri("/api/skills/runs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(runs.status(), StatusCode::OK);
        let runs_body = to_bytes(runs.into_body(), usize::MAX).await.unwrap();
        let runs_json: serde_json::Value = serde_json::from_slice(&runs_body).unwrap();
        assert_eq!(runs_json["kind"], "skills.runs");
        assert!(runs_json["items"]
            .as_array()
            .unwrap()
            .iter()
            .any(|run| run["run_id"] == run_id));

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn skill_translate_route_rejects_empty_content_before_model_call() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/skills/local:missing/translate")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::json!({"content": ""}).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["error"], "content is required");
    }

    #[tokio::test]
    async fn input_disposition_session_targets_use_the_gateway_owner_boundary() {
        use crate::services::session_service::{EnsureSessionRequest, SessionSource};
        use harness_contract::input_disposition::InputDispositionSessionTargetMode;

        let state = test_state();
        let service = &state.services.session;

        for (session_id, owner) in [
            ("disposition-source", "principal-a"),
            ("disposition-target", "principal-a"),
            ("disposition-foreign", "principal-b"),
        ] {
            let mut request = EnsureSessionRequest::new(
                session_id,
                Some("test-model".into()),
                SessionSource::WebUi,
            );
            request.owner_principal_id = Some(owner.to_string());
            service
                .ensure_surface_session(request)
                .await
                .expect("test Session is created through the Gateway owner");
        }

        let existing = service
            .resolve_input_disposition_session_target(&runtime::RuntimeSessionTargetRequest {
                source_session_id: "disposition-source".to_string(),
                disposition_id: "disposition-existing".to_string(),
                mode: InputDispositionSessionTargetMode::ExistingAuthorized,
                target_ref: Some("@session:disposition-target".to_string()),
                objective: "continue authorized work".to_string(),
            })
            .await
            .expect("same-principal and same-workspace target is authorized");
        assert_eq!(existing.target_session_id, "disposition-target");
        assert!(!existing.created);

        let foreign = service
            .resolve_input_disposition_session_target(&runtime::RuntimeSessionTargetRequest {
                source_session_id: "disposition-source".to_string(),
                disposition_id: "disposition-foreign".to_string(),
                mode: InputDispositionSessionTargetMode::ExistingAuthorized,
                target_ref: Some("disposition-foreign".to_string()),
                objective: "must not cross authority".to_string(),
            })
            .await;
        assert!(foreign.is_err_and(|error| error.contains("principal/workspace authority")));

        let isolated_request = runtime::RuntimeSessionTargetRequest {
            source_session_id: "disposition-source".to_string(),
            disposition_id: "disposition-create-once".to_string(),
            mode: InputDispositionSessionTargetMode::CreateIsolated,
            target_ref: None,
            objective: "run isolated work".to_string(),
        };
        let created = service
            .resolve_input_disposition_session_target(&isolated_request)
            .await
            .expect("isolated Session is created");
        let replayed = service
            .resolve_input_disposition_session_target(&isolated_request)
            .await
            .expect("isolated Session creation is idempotent");
        assert_eq!(created.target_session_id, "session-isolated-create-once");
        assert!(created.created);
        assert_eq!(replayed.target_session_id, created.target_session_id);
        assert!(!replayed.created);
    }

    #[tokio::test]
    async fn branch_session_copies_stored_messages_into_new_session() {
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let source_id = "branch-source";
        let mut source = new_api_session_record(source_id, Some("test-model".into()));
        source.metadata_json = Some(serde_json::json!({"title": "Source Topic"}).to_string());
        source.message_count = 2;
        store.create_session(&source).await.unwrap();
        store
            .insert_messages_batch(&[
                session::SessionMessage {
                    stable_message_id: format!("branch:{source_id}:0"),
                    session_id: source_id.to_string(),
                    sequence: 0,
                    role: "user".to_string(),
                    content_json: serde_json::json!([{"type":"text","text":"hello"}]).to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 10,
                },
                session::SessionMessage {
                    stable_message_id: format!("branch:{source_id}:1"),
                    session_id: source_id.to_string(),
                    sequence: 1,
                    role: "assistant".to_string(),
                    content_json: serde_json::json!([{"type":"text","text":"world"}]).to_string(),
                    blocks_count: 1,
                    tool_use_id: None,
                    tool_name: None,
                    token_usage_json: None,
                    created_at_ms: 11,
                },
            ])
            .await
            .unwrap();

        let app = api_router(test_state_with_store(store.clone()));
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{source_id}/branch"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"idempotency_key":"branch-copy-once"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let status = response.status();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "branch response: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["replayed"], false);
        assert_eq!(json["source_message_count"], 2);
        assert_eq!(json["copied_message_count"], 2);
        let branch_id = json["id"]
            .as_str()
            .expect("branch id should be returned")
            .to_string();
        let copied = store.get_messages(&branch_id, 0, 10).await.unwrap();
        assert_eq!(copied.len(), 2);
        assert_eq!(copied[0].session_id, branch_id);
        assert_ne!(copied[0].stable_message_id, format!("branch:{source_id}:0"));
        assert!(copied[0]
            .stable_message_id
            .starts_with(&format!("branch:{branch_id}:")));
        assert_eq!(copied[0].sequence, 0);
        assert!(copied[0].content_json.contains("hello"));
        let branch_record = store
            .get_session(&branch_id)
            .await
            .unwrap()
            .expect("branch record should exist");
        assert_eq!(branch_record.message_count, 2);
        assert!(branch_record
            .metadata_json
            .as_deref()
            .unwrap_or_default()
            .contains("branch-source"));
        let source_events = store.get_events(source_id, 0).await.unwrap();
        assert!(source_events.iter().any(|event| {
            event.event_type == "SessionBranched"
                && event.event_json.contains(&branch_id)
                && event.event_json.contains("\"copied_message_count\":2")
        }));
        let branch_events = store.get_events(&branch_id, 0).await.unwrap();
        assert!(branch_events.iter().any(|event| {
            event.event_type == "BranchCreated"
                && event.event_json.contains(source_id)
                && event.event_json.contains("\"copied_message_count\":2")
        }));

        store
            .insert_message(&session::SessionMessage {
                stable_message_id: format!("branch:{source_id}:2"),
                session_id: source_id.to_string(),
                sequence: 2,
                role: "user".to_string(),
                content_json: serde_json::json!([{"type":"text","text":"after first branch"}])
                    .to_string(),
                blocks_count: 1,
                tool_use_id: None,
                tool_name: None,
                token_usage_json: None,
                created_at_ms: 12,
            })
            .await
            .unwrap();
        let replay = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/sessions/{source_id}/branch"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"idempotency_key":"branch-copy-once"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(replay.status(), StatusCode::OK);
        let replay_body = to_bytes(replay.into_body(), usize::MAX).await.unwrap();
        let replay_json: serde_json::Value = serde_json::from_slice(&replay_body).unwrap();
        assert_eq!(replay_json["id"], branch_id);
        assert_eq!(replay_json["replayed"], true);
        assert_eq!(replay_json["source_message_count"], 2);
        assert_eq!(replay_json["copied_message_count"], 2);
        assert_eq!(
            store.get_messages(&branch_id, 0, 10).await.unwrap().len(),
            2,
            "a response retry must retain the original durable cutoff"
        );
        let branch_sessions = store
            .list_sessions()
            .await
            .unwrap()
            .into_iter()
            .filter(|record| {
                record
                    .metadata_json
                    .as_deref()
                    .unwrap_or_default()
                    .contains("\"branched_from\":\"branch-source\"")
            })
            .count();
        assert_eq!(branch_sessions, 1);
    }

    #[tokio::test]
    async fn harness_eval_routes_create_smoke_run_and_report_latest() {
        let workspace = test_temp_dir("harness-eval-route-workspace");
        let report_dir = test_temp_dir("harness-eval-route-reports");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({ "eval_report_dir": report_dir }),
            None,
            workspace.clone(),
        ));

        let run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/harness-eval/runs")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "level": "quick",
                            "budget": "low",
                            "allow_real_model": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(run.status(), StatusCode::OK);
        let run_body = to_bytes(run.into_body(), usize::MAX).await.unwrap();
        let run_json: serde_json::Value = serde_json::from_slice(&run_body).unwrap();
        assert_eq!(run_json["kind"], "harness_eval.run");
        assert_eq!(run_json["run"]["status"], "running");
        let run_id = run_json["run"]["run_id"].as_str().unwrap();
        let completed_run =
            wait_for_harness_eval_route_status(app.clone(), run_id, "completed").await;
        assert_eq!(completed_run["run"]["status"], "completed");

        let latest = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/harness-eval/reports/latest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(latest.status(), StatusCode::OK);
        let latest_body = to_bytes(latest.into_body(), usize::MAX).await.unwrap();
        let latest_json: serde_json::Value = serde_json::from_slice(&latest_body).unwrap();
        assert_eq!(latest_json["kind"], "harness_eval.latest_report");
        assert_eq!(latest_json["report"]["status"], "passed");
        let report_id = latest_json["report"]["id"].as_str().unwrap();

        let artifacts = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/harness-eval/reports/{report_id}/artifacts"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(artifacts.status(), StatusCode::OK);
        let artifacts_body = to_bytes(artifacts.into_body(), usize::MAX).await.unwrap();
        let artifacts_json: serde_json::Value = serde_json::from_slice(&artifacts_body).unwrap();
        assert_eq!(artifacts_json["kind"], "harness_eval.artifacts");
        assert_eq!(artifacts_json["report_id"], report_id);
        assert!(artifacts_json["count"].as_u64().unwrap_or_default() > 0);

        let gate = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/harness-eval/reports/{report_id}/gate"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(gate.status(), StatusCode::OK);
        let gate_body = to_bytes(gate.into_body(), usize::MAX).await.unwrap();
        let gate_json: serde_json::Value = serde_json::from_slice(&gate_body).unwrap();
        assert_eq!(gate_json["kind"], "harness_eval.report_gate");
        assert_eq!(gate_json["report_gate"]["status"], "passed");

        let scenarios = app
            .oneshot(
                Request::builder()
                    .uri("/api/harness-eval/scenarios")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(scenarios.status(), StatusCode::OK);
        let scenarios_body = to_bytes(scenarios.into_body(), usize::MAX).await.unwrap();
        let scenarios_json: serde_json::Value = serde_json::from_slice(&scenarios_body).unwrap();
        assert!(scenarios_json["next_gen_harness_closure"]
            .as_array()
            .is_some_and(|items| items.len() >= 7));

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(report_dir);
    }

    #[tokio::test]
    async fn evolution_discovery_routes_have_no_gateway_owned_candidate_or_release_path() {
        let workspace = test_temp_dir("evolution-route-workspace");
        let app = api_router(test_state_with_config_runtime_and_workspace(
            serde_json::json!({}),
            None,
            workspace.clone(),
        ));

        let signal = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/evolution/signals")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "signal_type": "memory_noise",
                            "source": {
                                "owner": "runtime",
                                "session_id": "session-1",
                                "agent_id": null,
                                "team_id": null,
                                "run_id": null
                            },
                            "evidence_refs": [{
                                "ref_type": "memory_packet",
                                "id": "memory:packet:noise",
                                "boundary": "observed"
                            }],
                            "severity": "warning",
                            "summary": "memory packet contained unrelated context",
                            "suggested_action": "tighten scope and salience gates",
                            "immediate_task_can_continue": true
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(signal.status(), StatusCode::OK);
        let signal_body = to_bytes(signal.into_body(), usize::MAX).await.unwrap();
        let signal_json: serde_json::Value = serde_json::from_slice(&signal_body).unwrap();
        assert_eq!(signal_json["kind"], "evolution.signal");
        let signal_id = signal_json["signal"]["signal_id"]
            .as_str()
            .expect("signal id")
            .to_string();

        let proposal = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/evolution/proposals")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({"signal_ids": [signal_id]}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proposal.status(), StatusCode::OK);
        let proposal_body = to_bytes(proposal.into_body(), usize::MAX).await.unwrap();
        let proposal_json: serde_json::Value = serde_json::from_slice(&proposal_body).unwrap();
        let proposal_id = proposal_json["proposal"]["proposal_id"].as_str().unwrap();
        assert_eq!(
            proposal_json["diagnosis"]["root_cause_kind"],
            "memory_governance_gap"
        );
        assert_eq!(proposal_json["plan_draft"]["blocked_mainline_write"], true);

        let diagnoses = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/evolution/diagnoses")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(diagnoses.status(), StatusCode::OK);

        let draft = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/evolution/proposals/{proposal_id}/skill-draft"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(draft.status(), StatusCode::OK);
        let draft_body = to_bytes(draft.into_body(), usize::MAX).await.unwrap();
        let draft_json: serde_json::Value = serde_json::from_slice(&draft_body).unwrap();
        assert_eq!(draft_json["kind"], "skills.evolution_draft");
        assert!(draft_json["draft"]["markdown"]
            .as_str()
            .unwrap()
            .contains("Acceptance Gates"));

        let candidates = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/evolution/candidates")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(candidates.status(), StatusCode::OK);
        let candidates_body = to_bytes(candidates.into_body(), usize::MAX).await.unwrap();
        let candidates_json: serde_json::Value = serde_json::from_slice(&candidates_body).unwrap();
        assert_eq!(candidates_json["owner"], "runtime");
        assert!(candidates_json["candidates"].is_array());

        let patterns = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/evolution/collaboration-patterns")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(patterns.status(), StatusCode::OK);
        let patterns_body = to_bytes(patterns.into_body(), usize::MAX).await.unwrap();
        let patterns_json: serde_json::Value = serde_json::from_slice(&patterns_body).unwrap();
        assert_eq!(patterns_json["kind"], "evolution.collaboration_patterns");
        assert_eq!(patterns_json["advisory_only"], true);
        assert!(patterns_json["patterns"].is_array());

        let chain = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/evolution/chain/{proposal_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(chain.status(), StatusCode::OK);

        let decision = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/evolution/proposals/{proposal_id}/decision"))
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"decision":"approved"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(decision.status(), StatusCode::OK);
        let decision_body = to_bytes(decision.into_body(), usize::MAX).await.unwrap();
        let decision_json: serde_json::Value = serde_json::from_slice(&decision_body).unwrap();
        assert_eq!(decision_json["proposal"]["status"], "approved");
        assert_eq!(decision_json["mainline_modified"], false);

        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn gateway_health_reports_pid_addr_static_source() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["gateway"], "gateway-runtime-host");
        assert_eq!(json["api_router"], "gateway-api-router");
        assert!(json["process"]["pid_file"]
            .as_str()
            .unwrap()
            .contains("cowd"));
        assert!(json["process"]["addr_file"]
            .as_str()
            .unwrap()
            .contains("addr"));
        assert_eq!(json["static_webui"]["config_key"], "gateway.webui_dir");
        assert_eq!(json["static_webui"]["required"], false);
        assert_eq!(json["static_webui"]["status"], "missing_config");
        assert_eq!(json["runtime"]["session_repository"], true);
        assert_eq!(
            json["runtime"]["session_projection"]["active_subscribers"],
            0
        );
        assert!(
            json["storage"]["registry"]["endpoint_count"]
                .as_u64()
                .unwrap_or_default()
                >= 11
        );
        assert!(json["storage"]["registry"]["root"]
            .as_str()
            .unwrap()
            .contains("storage"));
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.matrix.endpoint"));
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.growth.endpoint"
                && item["domain"] == "growth"
                && item["status"].as_str().is_some()));
        assert!(json["storage"]["locks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["domain"] == "tasks"));
    }

    #[tokio::test]
    async fn gateway_storage_health_reports_registry_and_locks() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let endpoints = json["storage"]["registry"]["endpoints"].as_array().unwrap();
        assert!(endpoints.iter().any(|item| item["id"] == "session"));
        assert!(endpoints.iter().any(|item| item["id"] == "memory"));
        assert!(endpoints.iter().any(|item| item["id"] == "matrix"));
        assert!(endpoints
            .iter()
            .any(|item| item["domain"]["kind"] == "connector_directory"));
        assert!(endpoints.iter().any(|item| item["id"] == "tasks"));
        assert!(endpoints.iter().all(|item| item["domain"]["kind"] != "app"));
        assert!(
            json["storage"]["locks"].as_array().unwrap().len() >= 7,
            "storage lock list should include all core sqlite domains"
        );
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.tasks.endpoint"));
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.fact.endpoint"));
    }

    #[tokio::test]
    async fn gateway_storage_health_reports_canonical_fact_growth_endpoint() {
        let tmp = std::env::temp_dir().join(format!(
            "cowd-gateway-growth-health-test-{}",
            uuid::Uuid::new_v4()
        ));
        let config_home = tmp.join("config");
        let app = api_router(test_state_with_workspace(
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            config_home,
        ));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(json["storage"]["migrations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["id"] == "storage.fact.endpoint" && item["domain"] == "fact"));
        let _ = std::fs::remove_dir_all(tmp);
    }

    #[tokio::test]
    async fn gateway_status_includes_storage_registry_summary() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["storage"]["registry"]["status"], "registered");
        assert!(
            json["storage"]["registry"]["endpoint_count"]
                .as_u64()
                .unwrap_or_default()
                >= 11
        );
        assert!(
            json["storage"]["registry"]["missing_count"]
                .as_u64()
                .unwrap_or_default()
                > 0
        );
    }

    #[tokio::test]
    async fn gateway_ready_reports_required_runtime_services() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let required = json["required"].as_array().unwrap();
        assert!(required.iter().any(|item| item == "gateway-runtime-host"));
        assert!(required.iter().any(|item| item == "gateway-api-router"));
        assert!(required.iter().any(|item| item == "session-service"));
        assert!(required.iter().any(|item| item == "session-projection"));
        assert!(required
            .iter()
            .any(|item| item == "session-worker-supervisor"));
        assert!(required.iter().any(|item| item == "storage-registry"));
        let old_required_webui = ["static", "webui", "index"].join("-");
        assert!(!required.iter().any(|item| item == &old_required_webui));
        assert!(json["optional"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "static-webui"));
    }

    #[tokio::test]
    async fn webui_manifest_explains_gateway_runtime_host_router_relationship() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/webui/manifest")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["kind"], "cowd.webui.manifest");
        assert!(json.get("daemon").is_none());
        assert!(json.get("socket_transition").is_none());
        assert_eq!(json["runtime_host"], "gateway internal runtime host");
        assert_eq!(json["api_router"], "gateway service route table");
        assert_eq!(
            json["control_channel"],
            "runtime host local control channel"
        );
        assert!(json["enabled_app_ids"]
            .as_array()
            .is_some_and(Vec::is_empty));
    }
