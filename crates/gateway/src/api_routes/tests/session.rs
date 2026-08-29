// Legacy API behavior shard; included into one shared test scope.
    #[tokio::test]
    async fn runtime_turn_routes_submit_project_and_cancel_receipts() {
        let app = api_router(test_state());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/turns")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": "session-turn-api",
                            "task_id": "task-turn-api",
                            "prompt": "verify runtime turn route",
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let submitted: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(submitted["ok"], true);
        assert_eq!(submitted["dispatch"], "runtime_service");
        assert_eq!(submitted["turn"]["status"], "pending");
        let turn_id = submitted["turn"]["turn_id"]
            .as_str()
            .expect("turn id should be present")
            .to_string();

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/turns/{turn_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let detail: serde_json::Value =
            serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(detail["turn"]["primary_task_id"], "task-turn-api");

        let cancelled = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/runtime/turns/{turn_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cancelled: serde_json::Value =
            serde_json::from_slice(&to_bytes(cancelled.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(cancelled["ok"], true);
        assert_eq!(cancelled["turn"]["status"], "cancelled");

        let snapshot = app
            .oneshot(
                Request::builder()
                    .uri("/api/runtime/snapshot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let snapshot: serde_json::Value =
            serde_json::from_slice(&to_bytes(snapshot.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert!(
            snapshot["turns"].as_array().is_some_and(Vec::is_empty),
            "terminal turns must leave the active Runtime snapshot: {snapshot}"
        );
    }

    #[tokio::test]
    async fn mission_routes_expose_runtime_projection_and_session_control() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let session_id = format!("mission-route-test-{}", uuid::Uuid::new_v4());
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "verify mission route",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);
        let created: serde_json::Value =
            serde_json::from_slice(&to_bytes(created.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(created["ok"], true);
        assert_eq!(
            created["snapshot"]["projection"]["mission"]["kind"],
            "mission.runtime"
        );
        assert!(
            created["snapshot"]["projection"]["workspace"]["session_count"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert!(!created["snapshot"]["projection"]["sessions"]
            .as_array()
            .expect("mission sessions")
            .iter()
            .any(|session| session["session_id"].as_str() == Some(session_id.as_str())));

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/mission/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);
        let detail: serde_json::Value =
            serde_json::from_slice(&to_bytes(detail.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(detail["kind"], "mission.session");
        assert_eq!(
            detail["session"]["session_id"].as_str(),
            Some(session_id.as_str())
        );

        let backgrounded = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/mission/sessions/{session_id}/background"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(backgrounded.status(), StatusCode::OK);
        let backgrounded: serde_json::Value = serde_json::from_slice(
            &to_bytes(backgrounded.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(backgrounded["receipt"]["status"], "accepted");
        assert_eq!(backgrounded["receipt"]["result"]["unloaded"], true);
        assert!(!backgrounded["snapshot"]["projection"]["sessions"]
            .as_array()
            .expect("mission sessions")
            .iter()
            .any(|session| session["session_id"].as_str() == Some(session_id.as_str())));

        let background_detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/mission/sessions/{session_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let background_detail: serde_json::Value = serde_json::from_slice(
            &to_bytes(background_detail.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            background_detail["session"]["session_id"].as_str(),
            Some(session_id.as_str())
        );

        let projection = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/mission/control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(projection.status(), StatusCode::OK);
        let projection: serde_json::Value =
            serde_json::from_slice(&to_bytes(projection.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(projection["envelope"]["service"], "mission");
        assert!(
            projection["snapshot"]["projection"]["workspace"]["session_count"]
                .as_u64()
                .is_some_and(|count| count >= 1)
        );
        assert!(!projection["snapshot"]["projection"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|session| session["session_id"].as_str() == Some(session_id.as_str())));

        let interpreted = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control/interpret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "current_session_id": session_id,
                            "command_text": "dispatch pending mission work",
                            "execute": false
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(interpreted.status(), StatusCode::OK);
        let interpreted: serde_json::Value =
            serde_json::from_slice(&to_bytes(interpreted.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            interpreted["kind"],
            "mission_control.command_interpretation"
        );
        assert_eq!(interpreted["ok"], true);
        assert_eq!(interpreted["interpretation"]["status"], "interpreted");
        assert_eq!(
            interpreted["interpretation"]["target_kind"].as_str(),
            Some("dispatch")
        );
    }

    #[tokio::test]
    async fn execution_projection_routes_use_runtime_snapshot_delta_and_command_contracts() {
        use harness_contract::execution_graph::{
            ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
        };

        let session_id = "projection-route-session";
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        store
            .create_session(&new_api_session_record(
                session_id,
                Some("test-model".to_string()),
            ))
            .await
            .unwrap();
        let state = test_state_with_store(store);
        let runtime = state
            .services
            .runtime
            .as_ref()
            .expect("runtime service")
            .runtime_services();
        let mut graph = ExecutionGraph::new("projection route test").with_lineage(
            harness_contract::execution_graph::ExecutionGraphLineage {
                session_id: session_id.to_string(),
                turn_id: "projection-route-turn".to_string(),
                root_task_id: "projection-route-task".to_string(),
                task_id: "projection-route-task".to_string(),
                generation: 1,
            },
        );
        let node = ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            serde_json::json!({
                "session_id": session_id,
                "kind": "projection_route_test",
            })
            .to_string(),
        );
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
        let execution_id = graph.id.clone();
        runtime
            .execution_supervisor()
            .submit_graph(
                graph,
                harness_contract::execution_graph::ExecutionGraphCommand::Start {
                    expected_revision: 0,
                },
            )
            .await
            .expect("graph registers");
        runtime
            .execution_supervisor()
            .wait_for_quiescence(&execution_id)
            .await
            .expect("graph reaches a stable projection before command testing");
        let observer_id = "test.execution-projection";
        attach_test_writer(&state, session_id, observer_id).await;
        let app = api_router(Arc::clone(&state));

        let snapshot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/executions/{execution_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshot.status(), StatusCode::OK);
        let snapshot: serde_json::Value =
            serde_json::from_slice(&to_bytes(snapshot.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(snapshot["execution_id"], execution_id);
        let revision = snapshot["revision"].as_u64().expect("revision");

        let subscription = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/runtime/live-subscriptions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", "test.execution-projection")
                    .body(Body::from(
                        serde_json::json!({
                            "surface_instance": "test.execution-projection",
                            "idempotency_key": "test-execution-projection-live",
                            "selector": {
                                "sources": [{
                                    "kind": "execution",
                                    "id": execution_id,
                                    "cursor": 0,
                                    "detail_scope": "summary"
                                }]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(subscription.status(), StatusCode::CREATED);
        let subscription: serde_json::Value = serde_json::from_slice(
            &to_bytes(subscription.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        let subscription_id = subscription["id"].as_str().expect("subscription id");
        let live = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/live/{subscription_id}"))
                    .header("x-cowd-observer-id", "test.execution-projection")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);
        assert!(live
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.starts_with("text/event-stream")));

        let missing_observer = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/runtime/executions/{execution_id}/commands"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "command_id": "api-projection-missing-observer",
                            "expected_revision": revision,
                            "command": "pause",
                            "payload": { "reason": "must not run" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_observer.status(), StatusCode::FORBIDDEN);

        let command = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/runtime/executions/{execution_id}/commands"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .header("x-cowd-observer-id", observer_id)
                    .body(Body::from(
                        serde_json::json!({
                            "command_id": "api-projection-pause",
                            "expected_revision": revision,
                            "command": "pause",
                            "payload": { "reason": "test" }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(command.status(), StatusCode::OK);
        let command: serde_json::Value =
            serde_json::from_slice(&to_bytes(command.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(command["status"], "accepted");
        assert!(command["accepted_revision"].as_u64().unwrap_or_default() > revision);
    }

    #[tokio::test]
    async fn mission_control_route_exposes_runtime_projection_and_command_router() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let suffix = uuid::Uuid::new_v4();
        let session_a = format!("mission-control-route-a-{suffix}");
        let session_b = format!("mission-control-route-b-{suffix}");

        let created_a = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "mission control command a",
                            "session_id": session_a,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created_a.status(), StatusCode::CREATED);
        let created_a: serde_json::Value =
            serde_json::from_slice(&to_bytes(created_a.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(created_a["ok"], true);
        assert_eq!(created_a["receipt"]["status"], "accepted");
        assert_eq!(created_a["saga"]["phase"], "finalized");

        let created_b = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "mission control command b",
                            "session_id": session_b,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created_b.status(), StatusCode::CREATED);

        let background = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "command_id": format!("mission-control-background-{suffix}"),
                            "action": "background",
                            "target": {
                                "kind": "session",
                                "session_id": session_b,
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let background: serde_json::Value =
            serde_json::from_slice(&to_bytes(background.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(background["kind"], "mission_control.command_result");
        assert_eq!(background["ok"], true);
        assert_eq!(background["receipt"]["action"], "background");
        assert_eq!(background["saga"]["phase"], "finalized");

        let control = app
            .oneshot(
                Request::builder()
                    .uri("/api/mission/control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(control.status(), StatusCode::OK);
        let control: serde_json::Value =
            serde_json::from_slice(&to_bytes(control.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            control["snapshot"]["projection"]["kind"],
            "mission_control.projection"
        );
        assert!(
            control["snapshot"]["projection"]["workspace"]["session_count"]
                .as_u64()
                .is_some_and(|count| count >= 2)
        );
        assert!(control["snapshot"]["projection"]["sessions"]
            .as_array()
            .is_some_and(Vec::is_empty));
        assert!(
            control["snapshot"]["projection"]["event_digest"]["total_recent_events"]
                .as_u64()
                .unwrap_or_default()
                > 0
        );
        assert_eq!(
            control["snapshot"]["projection"]["relations"]["kind"],
            "runtime.session_relations"
        );
        assert!(control["snapshot"]["projection"].get("stewards").is_none());
    }

    #[tokio::test]
    async fn mission_routes_write_approvals_reject_arbitrary_relations_and_project_proxies() {
        let _guard = mission_route_lock().lock().await;
        let _env_guard = crate::test_process_env_lock();
        let app = api_router(test_state());
        let session_a = format!("mission-route-a-{}", uuid::Uuid::new_v4());
        let session_b = format!("mission-route-b-{}", uuid::Uuid::new_v4());
        for session_id in [&session_a, &session_b] {
            let created = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri("/api/mission/sessions")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(
                            serde_json::json!({
                                "title": format!("route session {session_id}"),
                                "session_id": session_id,
                            })
                            .to_string(),
                        ))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(created.status(), StatusCode::CREATED);
        }
        let control = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/mission/control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(control.status(), StatusCode::OK);
        let control: serde_json::Value =
            serde_json::from_slice(&to_bytes(control.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let mission_id = control["snapshot"]["projection"]["mission"]["mission_id"]
            .as_str()
            .expect("default Mission id")
            .to_string();
        let task_id = format!("mission-route-task-{session_a}");
        let turn_id = format!("mission-route-turn-{session_a}");
        let task = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "task_id": task_id,
                            "mission_id": mission_id,
                            "origin_session_id": session_a,
                            "origin_turn_id": turn_id,
                            "objective": "research architecture and review implementation"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(task.status(), StatusCode::CREATED);

        let team = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "command_id": "mission-route-team-create",
                            "action": "create",
                            "target": {
                                "kind": "team",
                                "team_id": "mission-route-team"
                            },
                            "payload": {
                                "request_id": "overridden-by-command-id",
                                "team_id": "mission-route-team",
                                "mission_id": mission_id,
                                "lineage": {
                                    "session_id": session_a,
                                    "turn_id": turn_id,
                                    "root_task_id": task_id,
                                    "task_id": task_id,
                                    "generation": 1
                                },
                                "selection_mode": "explicit",
                                "template_selector": {
                                    "kind": "latest_stable",
                                    "template_id": "builtin/cowd/execute-review"
                                },
                                "objective": "research architecture and review implementation",
                                "acceptance": ["summary", "evidence"],
                                "permission_ceiling": "workspace-write",
                                "model_lease": "default",
                                "execution_budget": {
                                    "budget_id": "mission-route-team-budget",
                                    "predicted_tokens": 32768,
                                    "max_tokens": 65536,
                                    "deadline_at_ms": u64::MAX,
                                    "max_parallel": 4,
                                    "revision": 1
                                },
                                "deadline_at_ms": u64::MAX,
                                "resource_scopes": ["write:crates/runtime"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let team_status = team.status();
        let team_body = to_bytes(team.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            team_status,
            StatusCode::OK,
            "team runtime response: {}",
            String::from_utf8_lossy(&team_body)
        );
        let team_json: serde_json::Value = serde_json::from_slice(&team_body).unwrap();
        assert_eq!(team_json["ok"], true, "{team_json}");
        assert_eq!(team_json["saga"]["phase"], "finalized");
        assert!(team_json["receipt"]["result"]["graph_id"]
            .as_str()
            .is_some());

        let approval = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/approvals")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source": {
                                "kind": "session",
                                "session_id": session_a.clone(),
                                "agent_id": null,
                                "team_id": null,
                                "mission_id": "mission-a"
                            },
                            "action": "apply_patch",
                            "summary": "modify runtime",
                            "risk": "medium",
                            "evidence_refs": ["trace:1"],
                            "timeout_policy": "pending"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(approval.status(), StatusCode::CREATED);
        let approval_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(approval.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(approval_json["ok"], true);
        let approval_id = approval_json["approval"]["approval_id"]
            .as_str()
            .expect("approval id")
            .to_string();
        assert_eq!(approval_json["approval"]["status"], "pending");
        assert!(
            approval_json["approvals"]["pending_count"]
                .as_u64()
                .expect("pending count")
                >= 1
        );

        let approval_pending = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/approval/pending")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(approval_pending.status(), StatusCode::OK);
        let approval_pending_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(approval_pending.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            approval_pending_json["kind"],
            "gateway.unified_approval_pending"
        );
        assert!(approval_pending_json["pending"]
            .as_array()
            .expect("pending")
            .iter()
            .any(|approval| approval["approval_id"].as_str() == Some(approval_id.as_str())));

        let approval_decision = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/approval/respond")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "id": approval_id,
                            "approved": true,
                            "scope": "once",
                            "reason": "verified"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(approval_decision.status(), StatusCode::OK);
        let approval_decision_json: serde_json::Value = serde_json::from_slice(
            &to_bytes(approval_decision.into_body(), usize::MAX)
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            approval_decision_json["receipt"]["approval_id"].as_str(),
            Some(approval_id.as_str())
        );
        assert_eq!(approval_decision_json["receipt"]["status"], "approved");

        let relation = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/control")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "command_id": "mission-relation-link-test",
                            "action": "link",
                            "target": {
                                "kind": "relation",
                                "relation_id": "session-relation-test"
                            },
                            "actor": "gateway-test",
                            "correlation_id": "mission-relation-link-test",
                            "payload": {
                                "from_session_id": session_a.clone(),
                                "to_session_id": session_b.clone(),
                                "kind": "reviews",
                                "summary": "A reviews B",
                                "evidence_refs": ["trace:2"]
                            },
                            "evidence_refs": []
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(relation.status(), StatusCode::UNPROCESSABLE_ENTITY);

        let proxy = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/proxies")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "session_id": session_b.clone(),
                            "summary": "B summary",
                            "evidence_refs": ["trace:3"],
                            "decisions": ["ship"],
                            "open_questions": ["risk?"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(proxy.status(), StatusCode::OK);
        let proxy_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(proxy.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(proxy_json["proxy"]["session_id"], session_b);
    }

    #[tokio::test]
    async fn mission_control_keeps_unbound_session_out_of_default_mission() {
        let _guard = mission_route_lock().lock().await;
        let app = api_router(test_state());
        let session_id = format!("runtime-events-session-{}", uuid::Uuid::new_v4());
        let created = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/mission/sessions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "title": "runtime event session",
                            "session_id": session_id,
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let events = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/mission/control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        let events_json: serde_json::Value =
            serde_json::from_slice(&to_bytes(events.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        let projection = &events_json["snapshot"]["projection"];
        assert_eq!(projection["mission"]["kind"], "mission.runtime");
        assert!(projection["mission"]["aggregate"]["mission_id"]
            .as_str()
            .is_some_and(|mission_id| mission_id.starts_with("mission-default-")));
        assert!(projection["workspace"]["session_count"]
            .as_u64()
            .is_some_and(|count| count >= 1));
        assert!(!projection["sessions"]
            .as_array()
            .expect("mission control sessions")
            .iter()
            .any(|session| {
                session["session_id"].as_str() == Some(session_id.as_str())
                    && session["lifecycle"].as_str() == Some("active")
            }));

        let repeated = app
            .oneshot(
                Request::builder()
                    .uri("/api/mission/control")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let repeated: serde_json::Value =
            serde_json::from_slice(&to_bytes(repeated.into_body(), usize::MAX).await.unwrap())
                .unwrap();
        assert_eq!(
            repeated["snapshot"]["revision"], events_json["snapshot"]["revision"],
            "unchanged canonical sessions and event cursor must reuse the Mission projection cache"
        );
    }

    #[tokio::test]
    async fn cowd_projection_route_separates_cli_from_webui_surface() {
        let app = api_router(test_state());
        let webui = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/projection?surface=webui")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let cli = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/projection?surface=cli")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(webui.status(), StatusCode::OK);
        assert_eq!(cli.status(), StatusCode::OK);
        let webui_body = to_bytes(webui.into_body(), usize::MAX).await.unwrap();
        let cli_body = to_bytes(cli.into_body(), usize::MAX).await.unwrap();
        let webui_json: serde_json::Value = serde_json::from_slice(&webui_body).unwrap();
        let cli_json: serde_json::Value = serde_json::from_slice(&cli_body).unwrap();

        assert_eq!(webui_json["surface"], "webui");
        assert_eq!(cli_json["surface"], "cli");
        assert_eq!(webui_json["capability_count"], cli_json["capability_count"]);
        assert!(webui_json["capabilities"][0]["management_fields"]
            .as_array()
            .unwrap()
            .iter()
            .any(|field| field == "bulk_actions"));
        assert_eq!(
            cli_json["capabilities"][0]["management_fields"],
            serde_json::json!(["json_output", "core_controls"])
        );
    }

    #[tokio::test]
    async fn cowd_structured_sources_and_structured_ingest_plan_routes_expose_contract_adapter() {
        let workspace = test_temp_dir("cowd-structured-index");
        let config_home = test_temp_dir("cowd-structured-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let source_upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "cowd-structured-source",
                            "session_id": "session-cowd-structured",
                            "source_pack": {
                                "source_pack_id": "pack-1",
                                "source_name": "erp",
                                "owner": "operations",
                                "access_mode": "connector",
                                "refresh_mode": "incremental",
                                "entity_mappings": [{
                                    "source_entity": "plant",
                                    "matrix_entity_type": "factory",
                                    "source_key_field": "plant_id"
                                }],
                                "fact_mappings": [{
                                    "source_table": "inventory",
                                    "fact_type": "inventory_balance",
                                    "metric_key": "stock_on_hand",
                                    "entity_ref_fields": ["plant_id"],
                                    "measure_fields": ["qty"],
                                    "dedup_key": "plant_id:sku:week",
                                    "delta_signature": "qty"
                                }],
                                "reconciliation_rules": ["dedup_key_unique"],
                                "quality_rules": ["qty_non_negative"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(source_upsert.status(), StatusCode::OK);

        let fact_ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "cowd-structured-fact",
                            "session_id": "session-cowd-structured",
                            "facts": [{
                                "fact_id": "fact-stock-1",
                                "snapshot_id": "snapshot-week-30",
                                "fact_type": "inventory_balance",
                                "entity_refs": ["factory:sz"],
                                "metric_key": "stock_on_hand",
                                "dimensions": {"week": "2026-W30"},
                                "measures": {"qty": 42},
                                "source_ref": "pack-1",
                                "confidence": 0.97
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fact_ingest.status(), StatusCode::OK);
        let body = to_bytes(fact_ingest.into_body(), usize::MAX).await.unwrap();
        let fact_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let attention_id = fact_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap()
            .to_string();

        let evidence_build = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "Inventory balance requires structured evidence"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence_build.status(), StatusCode::OK);

        let sources = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let facts = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/facts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/evidence")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/cowd/structured/ingest-plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source_ref": "pack-1",
                            "fact_type": "inventory_balance",
                            "partition_ref": "2026-W30",
                            "high_watermark": "2026-06-14T00:00:00Z",
                            "estimated_rows": 42,
                            "raw_checksum": "sha256:test",
                            "metric_ids": ["stock_on_hand"]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let watermarks = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/structured/watermarks")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(sources.status(), StatusCode::OK);
        assert_eq!(facts.status(), StatusCode::OK);
        assert_eq!(evidence.status(), StatusCode::OK);
        assert_eq!(ingest.status(), StatusCode::OK);
        assert_eq!(watermarks.status(), StatusCode::OK);
        let sources_body = to_bytes(sources.into_body(), usize::MAX).await.unwrap();
        let facts_body = to_bytes(facts.into_body(), usize::MAX).await.unwrap();
        let evidence_body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let ingest_body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let watermarks_body = to_bytes(watermarks.into_body(), usize::MAX).await.unwrap();
        let sources_json: serde_json::Value = serde_json::from_slice(&sources_body).unwrap();
        let facts_json: serde_json::Value = serde_json::from_slice(&facts_body).unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&evidence_body).unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&ingest_body).unwrap();
        let watermarks_json: serde_json::Value = serde_json::from_slice(&watermarks_body).unwrap();

        assert_eq!(sources_json["contract"], "cowd.structured_data.v1");
        assert_eq!(sources_json["list_status"], "ready");
        assert_eq!(sources_json["count"], 1);
        assert_eq!(sources_json["items"][0]["source_id"], "pack-1");
        assert_eq!(facts_json["list_status"], "ready");
        assert_eq!(facts_json["items"][0]["fact_id"], "fact-stock-1");
        assert_eq!(evidence_json["list_status"], "ready");
        assert_eq!(
            evidence_json["items"][0]["problem_statement"],
            "Inventory balance requires structured evidence"
        );
        assert_eq!(ingest_json["source_ref"], "pack-1");
        assert_eq!(ingest_json["fact_type"], "inventory_balance");
        assert_eq!(
            ingest_json["affected_metric_ids"],
            serde_json::json!(["stock_on_hand"])
        );
        assert_eq!(
            ingest_json["watermark"]["high_watermark"],
            "2026-06-14T00:00:00Z"
        );
        assert_eq!(watermarks_json["list_status"], "ready");
        assert_eq!(watermarks_json["count"], 0);
        assert!(watermarks_json["items"]
            .as_array()
            .is_some_and(Vec::is_empty));
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn matrix_source_snapshot_run_maps_rows_through_gateway_api() {
        let workspace = test_temp_dir("matrix-source-snapshot-workspace");
        let config_home = test_temp_dir("matrix-source-snapshot-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let adapters = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/connectors/sources")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(adapters.status(), StatusCode::OK);
        let adapters_body = to_bytes(adapters.into_body(), usize::MAX).await.unwrap();
        let adapters_json: serde_json::Value = serde_json::from_slice(&adapters_body).unwrap();
        assert!(adapters_json["adapters"]
            .as_array()
            .unwrap()
            .iter()
            .any(|adapter| adapter["adapter_id"] == "feishu_bitable"));

        let source_upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/upsert")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "source_pack": {
                                "source_pack_id": "pack-snapshot-orders",
                                "source_name": "supply_gateway_fixture",
                                "owner": "operations",
                                "access_mode": "file",
                                "refresh_mode": "snapshot",
                                "entity_mappings": [
                                    {
                                        "source_entity": "supplier",
                                        "matrix_entity_type": "supplier",
                                        "source_key_field": "supplier_id"
                                    },
                                    {
                                        "source_entity": "part",
                                        "matrix_entity_type": "part",
                                        "source_key_field": "part_id"
                                    }
                                ],
                                "fact_mappings": [{
                                    "source_table": "orders",
                                    "fact_type": "supply.order",
                                    "metric_key": "supply_qty",
                                    "entity_ref_fields": ["supplier_id", "part_id"],
                                    "measure_fields": ["qty"],
                                    "event_time_field": "event_time",
                                    "dedup_key": "order_id",
                                    "delta_signature": "order_id"
                                }],
                                "relation_mappings": [{
                                    "source_table": "orders",
                                    "relation_type": "supplies",
                                    "from_source_key_field": "supplier_id",
                                    "to_source_key_field": "part_id",
                                    "attribute_fields": ["qty"],
                                    "dedup_key": "order_id"
                                }],
                                "reconciliation_rules": ["source_snapshot_is_idempotent"],
                                "quality_rules": ["dedup_key_required"]
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(source_upsert.status(), StatusCode::OK);

        let plan = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/pack-snapshot-orders/snapshots/plan")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "resource_ref": "file://orders.csv",
                            "estimated_rows": 2
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(plan.status(), StatusCode::OK);

        let run = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/source-packs/pack-snapshot-orders/snapshots/run")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "snapshot": {
                                "snapshot_id": "snapshot-gateway-orders-1",
                                "source_system": "supply_gateway_fixture",
                                "source_kind": "file",
                                "resource_ref": "file://orders.csv",
                                "schema_version": "source:csv:orders",
                                "row_count": 2,
                                "checksum": "sha256:fixture",
                                "confidence": 0.95
                            },
                            "rows": [
                                {
                                    "order_id": "O1",
                                    "supplier_id": "S1",
                                    "part_id": "P1",
                                    "qty": 12,
                                    "event_time": "2026-07-02T00:00:00Z"
                                },
                                {
                                    "order_id": "O2",
                                    "supplier_id": "S2",
                                    "part_id": "P2",
                                    "qty": 4,
                                    "event_time": "2026-07-02T01:00:00Z"
                                }
                            ]
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
        assert_eq!(run_json["kind"], "matrix.source_snapshot.run");
        assert_eq!(run_json["apply_report"]["fact_count"], 2);
        assert_eq!(run_json["apply_report"]["relation_count"], 2);

        let snapshots = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/source-packs/pack-snapshot-orders/snapshots")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(snapshots.status(), StatusCode::OK);

        let health = app
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let health_body = to_bytes(health.into_body(), usize::MAX).await.unwrap();
        let health_json: serde_json::Value = serde_json::from_slice(&health_body).unwrap();
        assert_eq!(health_json["source_snapshot_count"], 1);
        assert_eq!(health_json["fact_count"], 2);
        assert_eq!(health_json["relation_count"], 2);
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn cowd_surfaces_route_declares_webui_tui_parity_and_cli_minimality() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/surfaces")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["webui_tui_full_parity"], true);
        assert_eq!(json["cli_is_minimal_control"], true);
        assert_eq!(json["webui"]["role"], "enhanced_management");
        assert_eq!(json["tui"]["role"], "console_full_capability");
        assert_eq!(json["cli"]["role"], "minimal_core_control");
    }

    #[tokio::test]
    async fn cowd_release_gate_route_reports_missing_timeline_evidence() {
        let app = api_router(test_state());
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/cowd/release-gate")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(json["gate_id"], "cowd.release_gate.v1");
        assert_eq!(json["status"], "fail");
        assert!(json["checks"]
            .as_array()
            .unwrap()
            .iter()
            .any(|check| check["check_id"] == "surface.cli.minimal" && check["status"] == "pass"));
        assert!(json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "execution_outcome.timeline.available" && check["status"] == "fail"
        }));
        assert!(json["checks"].as_array().unwrap().iter().any(|check| {
            check["check_id"] == "structured_data.memory_context.bridge"
                && check["status"] == "fail"
        }));
    }

    #[tokio::test]
    async fn matrix_foundation_ingests_fact_and_builds_evidence_packet() {
        let workspace = test_temp_dir("matrix-foundation");
        let config_home = test_temp_dir("matrix-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(health.status(), StatusCode::OK);

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-test-1",
                            "session_id": "session-matrix",
                            "facts": [{
                                "fact_id": "fact-gpu-shortage",
                                "snapshot_id": "snapshot-week-24",
                                "fact_type": "supply.material_shortage",
                                "entity_refs": ["component:gpu-a"],
                                "metric_key": "material_shortage_risk",
                                "dimensions": {"week": "2026-W24"},
                                "measures": {"short_qty": 42},
                                "source_ref": "connector:local.docs:gpu-shortage",
                                "confidence": 0.91
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        let body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ingest_json["ingested"], 1);
        let attention_id = ingest_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap()
            .to_string();

        let hot = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/attention/hot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hot.status(), StatusCode::OK);
        let body = to_bytes(hot.into_body(), usize::MAX).await.unwrap();
        let hot_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(hot_json["items"].as_array().unwrap().len(), 1);

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "GPU shortage may affect server shipments"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);
        let body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let packet_id = evidence_json["packet"]["packet_id"].as_str().unwrap();
        assert!(evidence_json["packet"]["missing_evidence"]
            .as_array()
            .is_some_and(|items| !items.is_empty()));

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/matrix/evidence/{packet_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        assert!(config_home.join("storage").join("matrix.sqlite").exists());
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn reality_core_routes_expose_stable_read_only_projection() {
        let workspace = test_temp_dir("reality-core");
        let config_home = test_temp_dir("reality-core-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        for (uri, kind) in [
            ("/api/reality/status", "reality.status"),
            ("/api/reality/capabilities", "reality.capabilities"),
            ("/api/reality/static", "reality.static"),
            ("/api/reality/flow", "reality.fact_flow"),
            (
                "/api/reality/recall/report?q=reality",
                "reality.recall_report",
            ),
            (
                "/api/reality/context/envelope?q=reality",
                "reality.context_envelope",
            ),
            ("/api/reality/evidence/missing-evidence", "reality.evidence"),
            ("/api/reality/promotions", "reality.promotions"),
            ("/api/reality/governance", "reality.governance"),
            ("/api/reality/boundaries", "reality.boundaries"),
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(json["kind"], kind, "{uri}");
            assert!(json.get("envelope").is_some(), "{uri}");
        }

        let flow = app
            .oneshot(
                Request::builder()
                    .uri("/api/reality/flow?session_id=session-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(flow.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["source"], "growth.promotions");
        assert!(json["stages"].as_array().is_some());

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn reality_recall_report_and_context_include_fact_and_matrix_sources() {
        let workspace = test_temp_dir("reality-recall");
        let config_home = test_temp_dir("reality-recall-config");
        let state = test_state_with_workspace(workspace.clone(), config_home.clone());
        let app = api_router(state.clone());

        let record = harness_contract::growth::LearningRecord::from_input(
            harness_contract::growth::GrowthInput {
                selected_pattern: harness_contract::core::ExecutionPattern::Execute,
                complexity: harness_contract::core::TaskComplexity::Complex,
                risk: harness_contract::core::TaskRisk::Medium,
                context_omitted: 0,
                tool_requires_checkpoint: false,
                tool_requires_human_confirm: false,
                verification_can_finalize: true,
                bench_passed: true,
            },
        );
        let mut event = harness_contract::growth::GrowthEvent::from_input(
            harness_contract::growth::GrowthEventInput {
                session_id: "session-reality-recall".to_string(),
                source_event_kind: "runtime.context.reality_test".to_string(),
                strategy_pattern: harness_contract::core::ExecutionPattern::Execute,
                learning_record: record,
                evidence_refs: vec![harness_contract::growth::GrowthEvidenceRef::new(
                    "test_evidence",
                    "trace:gpu-shortage",
                    "GPU shortage trace",
                )],
            },
        );
        event.memory_candidates = vec![harness_contract::growth::GrowthMemoryCandidate {
            id: "candidate-gpu-shortage".to_string(),
            kind: harness_contract::growth::GrowthMemoryCandidateKind::AuthorityPromotion,
            summary: "GPU shortage requires expedited supplier allocation".to_string(),
            reason: "observed shortage was confirmed by runtime evidence".to_string(),
            confidence_bp: 9_100,
        }];
        event.matrix_signals = vec![harness_contract::growth::GrowthMatrixSignal {
            fact_type: "supply.material_shortage".to_string(),
            dimensions: serde_json::json!({"component": "gpu", "week": "2026-W24"}),
            measures: serde_json::json!({"short_qty": 42, "risk": "high"}),
            confidence_bp: 9_200,
        }];
        let receipt = state
            .services
            .growth
            .ingest_growth_event(
                &state.config_home,
                &state.services.memory,
                &state.services.matrix,
                event,
            )
            .await;
        assert!(receipt.errors.is_empty(), "{receipt:#?}");
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "fact.memory" && item.status == "promote"));
        assert!(receipt
            .promotions
            .iter()
            .any(|item| item.target == "matrix.fact" && item.status == "promoted"));

        let recall = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/reality/recall/report?q=GPU%20shortage&max_items=20")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recall.status(), StatusCode::OK);
        let body = to_bytes(recall.into_body(), usize::MAX).await.unwrap();
        let recall_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let source_names = recall_json["recall_report"]["sources"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|source| source["source"].as_str())
            .collect::<Vec<_>>();
        assert!(source_names.contains(&"fact"), "{recall_json:#}");
        assert!(source_names.contains(&"matrix"), "{recall_json:#}");
        assert!(recall_json["recall_report"]["selected"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["source"] == "fact"));
        assert!(recall_json["recall_report"]["selected"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["source"] == "matrix"));

        let context = app
            .oneshot(
                Request::builder()
                    .uri("/api/context/current?q=GPU%20shortage")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(context.status(), StatusCode::OK);
        let body = to_bytes(context.into_body(), usize::MAX).await.unwrap();
        let context_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let selected = context_json["envelope"]["selected"].as_array().unwrap();
        assert!(
            selected.iter().any(|item| item["source"] == "Fact"),
            "{context_json:#}"
        );
        assert!(
            selected.iter().any(|item| item["source"] == "Matrix"),
            "{context_json:#}"
        );

        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn matrix_routes_expose_structured_fact_engine() {
        let workspace = test_temp_dir("matrix-foundation");
        let config_home = test_temp_dir("matrix-config");
        let app = api_router(test_state_with_workspace(
            workspace.clone(),
            config_home.clone(),
        ));

        let matrix_health = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let matrix_health_again = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(matrix_health.status(), StatusCode::OK);
        assert_eq!(matrix_health_again.status(), StatusCode::OK);
        let matrix_health_body = to_bytes(matrix_health.into_body(), usize::MAX)
            .await
            .unwrap();
        let matrix_health_again_body = to_bytes(matrix_health_again.into_body(), usize::MAX)
            .await
            .unwrap();
        let matrix_health_json: serde_json::Value =
            serde_json::from_slice(&matrix_health_body).unwrap();
        let matrix_health_again_json: serde_json::Value =
            serde_json::from_slice(&matrix_health_again_body).unwrap();
        assert_eq!(matrix_health_json, matrix_health_again_json);

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-test-1",
                            "session_id": "session-matrix",
                            "facts": [{
                                "fact_id": "fact-matrix-gpu-shortage",
                                "snapshot_id": "snapshot-week-24",
                                "fact_type": "supply.material_shortage",
                                "entity_refs": ["component:gpu-a"],
                                "metric_key": "material_shortage_risk",
                                "dimensions": {"week": "2026-W24"},
                                "measures": {"short_qty": 42},
                                "source_ref": "connector:local.docs:gpu-shortage",
                                "confidence": 0.91
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        let body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(ingest_json["ingested"], 1);
        let attention_id = ingest_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap()
            .to_string();

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "attention_id": attention_id,
                            "problem_statement": "Matrix evidence should share Matrix storage"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);
        let body = to_bytes(evidence.into_body(), usize::MAX).await.unwrap();
        let evidence_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let packet_id = evidence_json["packet"]["packet_id"].as_str().unwrap();

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/matrix/evidence/{packet_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::OK);
        assert!(config_home.join("storage").join("matrix.sqlite").exists());
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn matrix_fact_and_evidence_append_execution_summaries_to_runtime_timeline() {
        let workspace = test_temp_dir("matrix-outcome-timeline");
        let config_home = test_temp_dir("matrix-outcome-config");
        let store = Arc::new(UnifiedSessionStore::open_in_memory().unwrap());
        let app = api_router(test_state_with_store_and_workspace(
            store,
            workspace.clone(),
            config_home.clone(),
        ));
        let session_id = "matrix-outcome-session";

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-outcome-fact",
                            "session_id": session_id,
                            "facts": [{
                                "fact_id": "fact-outcome-stock",
                                "snapshot_id": "snapshot-outcome",
                                "fact_type": "inventory_balance",
                                "entity_refs": ["factory:sz"],
                                "metric_key": "stock_on_hand",
                                "dimensions": {"week": "2026-W30"},
                                "measures": {"qty": 64},
                                "source_ref": "pack-outcome",
                                "confidence": 0.93
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);
        let body = to_bytes(ingest.into_body(), usize::MAX).await.unwrap();
        let ingest_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let attention_id = ingest_json["attention"][0]["attention_id"]
            .as_str()
            .unwrap();

        let evidence = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/evidence/build")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "request_id": "matrix-outcome-evidence",
                            "session_id": session_id,
                            "attention_id": attention_id,
                            "problem_statement": "Inventory balance outcome should reach timeline"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(evidence.status(), StatusCode::OK);

        let timeline = app
            .oneshot(
                Request::builder()
                    .uri(format!(
                        "/api/runtime/timeline?session_id={session_id}&limit=10"
                    ))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(timeline.status(), StatusCode::OK);
        let body = to_bytes(timeline.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let outcome_events = json["events"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|event| event["kind"] == "application.execution_summary")
            .collect::<Vec<_>>();
        assert_eq!(outcome_events.len(), 2);
        assert!(outcome_events.iter().any(|event| {
            event["refs"].as_array().is_some_and(|refs| {
                refs.iter().any(|reference| {
                    reference["type"] == "structured_fact"
                        && reference["id"] == "fact-outcome-stock"
                })
            })
        }));
        assert!(outcome_events.iter().any(|event| {
            event["refs"].as_array().is_some_and(|refs| {
                refs.iter()
                    .any(|reference| reference["type"] == "structured_evidence")
            })
        }));
        let _ = std::fs::remove_dir_all(workspace);
        let _ = std::fs::remove_dir_all(config_home);
    }

    #[tokio::test]
    async fn matrix_metric_recompute_projects_changes_and_attention() {
        let workspace = test_temp_dir("matrix-metric");
        let config_home = test_temp_dir("matrix-metric-config");
        let app = api_router(test_state_with_workspace(workspace.clone(), config_home));

        let ingest = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/facts/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "facts": [
                                {
                                    "fact_id": "fact-plan-api-1",
                                    "snapshot_id": "snapshot-plan-api-1",
                                    "fact_type": "plan.weekly_demand",
                                    "entity_refs": ["product:server-a"],
                                    "metric_key": "plan_bom_delta",
                                    "dimensions": {"week": "2026-W24"},
                                    "measures": {"demand_qty": 100},
                                    "confidence": 0.8
                                },
                                {
                                    "fact_id": "fact-plan-api-2",
                                    "snapshot_id": "snapshot-plan-api-2",
                                    "fact_type": "plan.weekly_demand",
                                    "entity_refs": ["product:server-a"],
                                    "metric_key": "plan_bom_delta",
                                    "dimensions": {"week": "2026-W24"},
                                    "measures": {"demand_qty": 140},
                                    "confidence": 0.9
                                }
                            ]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(ingest.status(), StatusCode::OK);

        let recompute = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/matrix/metrics/recompute")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recompute.status(), StatusCode::OK);
        let body = to_bytes(recompute.into_body(), usize::MAX).await.unwrap();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["result"]["metric_state_count"], 1);
        assert_eq!(json["result"]["change_count"], 1);
        assert_eq!(json["result"]["metric_states"][0]["value"], 240.0);

        let metric = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/metrics/plan_bom_delta")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metric.status(), StatusCode::OK);

        let changes = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/changes")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(changes.status(), StatusCode::OK);
        let body = to_bytes(changes.into_body(), usize::MAX).await.unwrap();
        let changes_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(changes_json["changes"].as_array().unwrap().len(), 1);

        let hot = app
            .oneshot(
                Request::builder()
                    .uri("/api/matrix/attention/hot")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hot.status(), StatusCode::OK);
        let body = to_bytes(hot.into_body(), usize::MAX).await.unwrap();
        let hot_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(hot_json["items"].as_array().unwrap().iter().any(|item| {
            item["reason_codes"]
                .as_array()
                .unwrap()
                .iter()
                .any(|reason| reason == "metric_delta_detected")
        }));
        let _ = std::fs::remove_dir_all(workspace);
    }

    #[tokio::test]
    async fn task_execution_graph_is_runtime_owned_and_surface_read_only() {
        let state = test_state();
        let source_session_id = "session-task-execution-graph";
        publish_test_session_policy(&state.services, source_session_id);
        let mission_id = state
            .services
            .task
            .workspace_default_mission_id()
            .expect("Runtime-backed TaskService");
        let app = api_router(state);
        let started = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/tasks/start")
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "task_id": "task-execution-graph",
                            "mission_id": mission_id,
                            "origin_session_id": source_session_id,
                            "origin_turn_id": "turn-task-execution-graph",
                            "objective": "coordinate multi agent"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(started.status(), StatusCode::CREATED);
        let body = to_bytes(started.into_body(), usize::MAX).await.unwrap();
        let task: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let task_id = task["task_id"].as_str().unwrap();

        let runs = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/agents/execution-graphs")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(runs.status(), StatusCode::OK);
        let body = to_bytes(runs.into_body(), usize::MAX).await.unwrap();
        let runs_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(runs_json["kind"], "execution_graphs");
        assert_eq!(runs_json["graphs"].as_array().unwrap().len(), 0);

        let upsert = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/tasks/{task_id}/execution-graph"))
                    .header("content-type", "application/json")
                    .body(Body::from(
                        serde_json::json!({
                            "objective": "coordinate multi agent",
                            "nodes": [
                                {
                                    "id": "planner",
                                    "kind": "agent_task",
                                    "payload_ref": "task:planner",
                                    "executor_kind": "agent_task",
                                    "idempotency_key": "task:planner:1",
                                    "lease_ref": null,
                                    "acceptance": {"criteria": [], "required_evidence": [], "minimum_score_basis_points": null},
                                    "retry_policy": {"max_attempts": 1, "retryable_failure_kinds": [], "base_backoff_ms": 500, "maximum_backoff_ms": 30000},
                                    "resource_scopes": []
                                },
                                {
                                    "id": "review",
                                    "kind": "verify",
                                    "payload_ref": "task:review",
                                    "executor_kind": "verify",
                                    "idempotency_key": "task:review:1",
                                    "lease_ref": null,
                                    "acceptance": {"criteria": [], "required_evidence": [], "minimum_score_basis_points": null},
                                    "retry_policy": {"max_attempts": 1, "retryable_failure_kinds": [], "base_backoff_ms": 500, "maximum_backoff_ms": 30000},
                                    "resource_scopes": []
                                }
                            ],
                            "edges": [{"from": "planner", "to": "review", "kind": "depends_on"}]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(upsert.status(), StatusCode::METHOD_NOT_ALLOWED);

        let fetched = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/tasks/{task_id}/execution-graph"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(fetched.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn runtime_agent_routes_reject_commands_without_recoverable_backend_handle() {
        let state = test_state();
        let agent_id = format!("agent-route-{}", uuid::Uuid::new_v4());
        let services = state
            .services
            .runtime
            .as_ref()
            .expect("runtime service")
            .runtime_services();
        let graph_identity = harness_contract::execution::ExecutionIdentity::for_task_graph(
            "principal-route",
            services.workspace_key(),
            "mission-route",
            "task-route",
            "session-route",
            "turn-route",
            "graph-route",
        )
        .expect("graph identity");
        services
            .agent_runtime()
            .restore_verified_run(runtime::AgentRunSnapshot {
                execution_identity: harness_contract::execution::ExecutionIdentity::for_agent_node(
                    &graph_identity,
                    format!("run-{agent_id}"),
                    "node-route",
                )
                .expect("agent identity"),
                run_id: format!("run-{agent_id}"),
                agent_id: agent_id.clone(),
                task_id: "task-route".to_string(),
                root_task_id: "task-route".to_string(),
                session_id: "session-route".to_string(),
                graph_id: "graph-route".to_string(),
                node_id: "node-route".to_string(),
                attempt: 1,
                expected_graph_revision: 1,
                backend: runtime::AgentBackendKind::InProcess,
                status: harness_contract::agent::AgentStatus::Running,
                revision: 0,
                model: None,
                provider: None,
                binding: None,
                started_at_ms: 1,
                updated_at_ms: 1,
                failure: None,
            })
            .expect("restore agent");
        let app = api_router(state);

        let detail = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/agents/{agent_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(detail.status(), StatusCode::OK);

        let cancel = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(format!("/api/runtime/agents/{agent_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(cancel.status(), StatusCode::OK);
        let body = to_bytes(cancel.into_body(), usize::MAX).await.unwrap();
        let cancel_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(cancel_json["receipt"]["accepted"], false);
        assert_eq!(
            cancel_json["receipt"]["reject_reason"],
            "unsupported_by_backend"
        );

        let events = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/runtime/agents/{agent_id}/events"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(events.status(), StatusCode::OK);
        let body = to_bytes(events.into_body(), usize::MAX).await.unwrap();
        let events_json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert!(events_json["count"].as_u64().unwrap_or_default() >= 2);
    }
