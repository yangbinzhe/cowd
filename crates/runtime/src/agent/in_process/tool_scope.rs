use super::*;

#[cfg(test)]
mod resource_frame_tests {
    use super::*;

    struct CatalogHost(tools::ToolHostLease);

    #[async_trait::async_trait]
    impl RuntimeExecutionHost for CatalogHost {
        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            Some(self.0.describe_effect(name, input))
        }

        async fn execute_runtime_tool(
            &self,
            _: &RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            panic!("rejected composite must not reach a prefix effect")
        }
    }

    #[tokio::test]
    async fn r1_composite_effect_parent_requires_all_domains_and_file_operations() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("held.txt"), "preimage").unwrap();
        std::fs::write(root.path().join("source.txt"), "source").unwrap();
        let host = Arc::new(CatalogHost(
            tools::ToolHost::builtin("parent-resource-frame", root.path()).pin_snapshot(),
        ));
        let mut executor = ScopedRuntimeToolExecutor {
            tool_batch: None,
            host: host.clone(),
            allowed_tools: BTreeSet::from([
                "edit".into(),
                "read_file".into(),
                "web_fetch".into(),
                "tool_orchestrate".into(),
            ]),
            session_id: "session".into(),
            sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("session", "agent"),
            reality_context: None,
            model_lease: "model".into(),
            execution_id: "graph".into(),
            node_id: "node".into(),
            attempt: 1,
            workspace_root: root.path().into(),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap(),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: None,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
            provider_model_obligations: Vec::new(),
        };
        let mixed = serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"held.txt","content":"changed"}},
            {"id":"read","op":"call","tool":"read_file","input":{"path":"source.txt"}},
            {"id":"fetch","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"summarize"}}
        ]});
        let descriptor = host.0.describe_effect("tool_orchestrate", &mixed);
        assert!(descriptor.uses_network && !descriptor.spawns_process);
        for scopes in [
            vec!["network:*"],
            vec!["write:held.txt", "read:source.txt"],
            vec!["network:*", "write:other.txt", "read:source.txt"],
            vec!["network:*", "read:held.txt", "read:source.txt"],
            vec!["network:*", "write:held.txt"],
        ] {
            executor.resource_scopes = Some(scopes.into_iter().map(str::to_string).collect());
            assert!(executor
                .enforce_resource_ceiling("tool_orchestrate", &mixed.to_string())
                .is_err());
            assert!(executor
                .execute_output("tool_orchestrate", &mixed.to_string())
                .await
                .is_err());
            assert_eq!(
                std::fs::read_to_string(root.path().join("held.txt")).unwrap(),
                "preimage"
            );
            assert!(executor.receipts.lock().unwrap().is_empty());
        }
        let complete = vec![
            "network:*".to_string(),
            "write:held.txt".into(),
            "read:source.txt".into(),
            "write:.cowd/checkpoints".into(),
        ];
        for missing in 0..complete.len() {
            let mut narrow = complete.clone();
            narrow.remove(missing);
            executor.resource_scopes = Some(narrow);
            assert!(executor
                .enforce_resource_ceiling("tool_orchestrate", &mixed.to_string())
                .is_err());
            assert!(executor
                .execute_output("tool_orchestrate", &mixed.to_string())
                .await
                .is_err());
            assert!(executor.receipts.lock().unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.path().join("held.txt")).unwrap(),
                "preimage"
            );
        }
        executor.resource_scopes = Some(complete);
        executor
            .enforce_resource_ceiling("tool_orchestrate", &mixed.to_string())
            .unwrap();
        assert_eq!(
            crate::tool_host::effect_resource_evidence(&descriptor),
            vec![
                "network:*",
                "read:held.txt",
                "read:source.txt",
                "write:.cowd/checkpoints",
                "write:held.txt"
            ]
        );
        executor.allowed_tools.remove("web_fetch");
        assert!(executor
            .execute_output("tool_orchestrate", &mixed.to_string())
            .await
            .is_err());

        let compute = serde_json::json!({"steps":[
            {"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"held.txt","content":"changed"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"}
        ]});
        let descriptor = host.0.describe_effect("tool_orchestrate", &compute);
        assert!(descriptor.spawns_process && !descriptor.uses_network);
        executor.resource_scopes = Some(vec!["read:.".into()]);
        assert!(executor
            .enforce_resource_ceiling("tool_orchestrate", &compute.to_string())
            .is_err());
        executor.resource_scopes = Some(vec!["read:.".into(), "write:held.txt".into()]);
        assert!(executor
            .enforce_resource_ceiling("tool_orchestrate", &compute.to_string())
            .is_err());
        executor.resource_scopes = Some(vec![
            "read:.".into(),
            "write:held.txt".into(),
            "write:.cowd/checkpoints".into(),
        ]);
        executor
            .enforce_resource_ceiling("tool_orchestrate", &compute.to_string())
            .unwrap();
        assert_eq!(
            crate::tool_host::effect_resource_evidence(&descriptor),
            vec![
                "read:.",
                "read:held.txt",
                "write:.cowd/checkpoints",
                "write:held.txt"
            ]
        );
        let pure = serde_json::json!({"steps":[
            {"id":"compute","op":"compute","language":"python","code":"print(1)"}
        ]});
        executor.resource_scopes = Some(vec!["read:.".into()]);
        executor
            .enforce_resource_ceiling("tool_orchestrate", &pure.to_string())
            .unwrap();
        executor.resource_scopes = Some(vec!["read:held.txt".into()]);
        assert!(executor
            .enforce_resource_ceiling("tool_orchestrate", &pure.to_string())
            .is_err());
        executor.resource_scopes = Some(vec!["network:*".into()]);
        executor
            .enforce_resource_ceiling("web_fetch", &mixed["steps"][2]["input"].to_string())
            .unwrap();
        assert_eq!(
            crate::tool_host::effect_resource_evidence(
                &host
                    .0
                    .describe_effect("web_fetch", &mixed["steps"][2]["input"])
            ),
            vec!["network:*"]
        );

        for repository in ["first", "second"] {
            std::fs::create_dir_all(root.path().join(repository).join(".git")).unwrap();
        }
        executor.path_identity_resolver = Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap(),
        );
        assert!(executor
            .path_identity_resolver
            .resolve_existing(".")
            .is_err());
        std::fs::write(root.path().join("first/held.txt"), "first preimage").unwrap();
        let multi = serde_json::json!({"steps":[
            {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"first/held.txt","content":"x"}},
            {"id":"compute","op":"compute","language":"python","code":"print(1)"}
        ]});
        executor.resource_scopes = Some(vec![
            "read:.".into(),
            "write:first/held.txt".into(),
            "write:.cowd/checkpoints".into(),
        ]);
        executor
            .enforce_resource_ceiling("tool_orchestrate", &multi.to_string())
            .unwrap();
        for denied in [
            vec!["read:.", "write:second/held.txt", "write:.cowd/checkpoints"],
            vec!["read:.", "write:first/held.txt"],
            vec![
                "read:.",
                "write:first/held.txt",
                "write:first/.cowd/checkpoints",
            ],
            vec![
                "read:.",
                "write:first/held.txt",
                "write:second/.cowd/checkpoints",
            ],
        ] {
            executor.resource_scopes = Some(denied.into_iter().map(str::to_string).collect());
            assert!(executor
                .enforce_resource_ceiling("tool_orchestrate", &multi.to_string())
                .is_err());
            assert!(executor
                .execute_output("tool_orchestrate", &multi.to_string())
                .await
                .is_err());
            assert!(executor.receipts.lock().unwrap().is_empty());
            assert_eq!(
                std::fs::read_to_string(root.path().join("first/held.txt")).unwrap(),
                "first preimage"
            );
        }
        let broad = serde_json::json!({"steps":[
            {"id":"read","op":"call","tool":"read_file","input":{"path":"first/held.txt"}},
            {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"$read.file.content","content":"x"}}
        ]});
        executor.resource_scopes = Some(vec!["read:.".into(), "write:first/held.txt".into()]);
        assert!(executor
            .enforce_resource_ceiling("tool_orchestrate", &broad.to_string())
            .is_err());
        executor.resource_scopes = Some(vec!["write:.".into()]);
        executor
            .enforce_resource_ceiling("tool_orchestrate", &broad.to_string())
            .unwrap();
    }
    fn checkpoint_executor(
        root: &std::path::Path,
        host: Arc<dyn RuntimeExecutionHost>,
        scopes: Option<Vec<String>>,
    ) -> ScopedRuntimeToolExecutor {
        ScopedRuntimeToolExecutor {
            tool_batch: None,
            host,
            allowed_tools: BTreeSet::from(["edit".into()]),
            session_id: "r372-session".into(),
            sandbox_posture: harness_contract::policy::SandboxPosture::WorkspaceWriteSandbox,
            policy_revision: 1,
            memory_context: memory::MemoryTurnContext::new("r372-session", "producer"),
            reality_context: None,
            model_lease: "never-dispatched".into(),
            execution_id: "r372-graph".into(),
            node_id: "r372-node".into(),
            attempt: 1,
            workspace_root: root.into(),
            path_identity_resolver: Arc::new(
                crate::path_identity::WorkspacePathIdentityResolver::discover(root).unwrap(),
            ),
            scope_locks: Arc::new(ScopeLockManager::new()),
            commit_service: None,
            resource_scopes: scopes,
            managed_invocation: None,
            next_receipt_sequence: AtomicU64::new(0),
            receipts: Mutex::new(Vec::new()),
            provider_model_obligations: Vec::new(),
        }
    }

    fn checkpoint_assessment(
        effect: harness_contract::tool::ToolEffectDescriptor,
        input: &str,
        parent: PermissionMode,
        idempotency_key: &str,
    ) -> crate::EffectiveToolAuthorizationAssessment {
        let policy = permission_policy(None, parent, &BTreeSet::new());
        crate::AuthorizationNegotiator::new().assess_effective(
            &policy,
            &crate::AuthorizationRequest {
                principal_id: "agent:r372-producer".into(),
                capability: "checkpoint".into(),
                input: input.into(),
                idempotency_key: idempotency_key.into(),
                effect,
                parent_ceiling: policy.active_mode(),
                parent_lease_id: Some("binding:r372-producer".into()),
                policy_revision: 1,
                recovery_scope: "turn:r372".into(),
                context: crate::PermissionContext::default(),
                safe_alternatives: Vec::new(),
            },
        )
    }

    #[test]
    fn r372_checkpoint_explicit_range_survives_team_ceiling_and_stays_fail_closed() {
        use harness_contract::policy::{CapabilityGapKind, DataClassification};
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("project")).unwrap();
        std::fs::write(root.path().join("project/a.txt"), b"preimage").unwrap();
        let host = Arc::new(CatalogHost(
            tools::ToolHost::builtin("r372-pure-descriptor", root.path()).pin_snapshot(),
        ));
        let input = serde_json::json!({
            "action":"create", "label":"original prepared range",
            "paths":["new.txt","project/a.txt","new.txt"]
        });
        let builtin = host.0.describe_effect("checkpoint", &input);
        for scopes in [
            vec!["workspace:."],
            vec!["write:project", "write:new.txt", "write:unrelated.txt"],
            vec!["write:project/a.txt", "write:new.txt"],
        ] {
            let executor = checkpoint_executor(
                root.path(),
                host.clone(),
                Some(scopes.into_iter().map(str::to_string).collect()),
            );
            let adapted = executor.internal_checkpoint_input(input.clone()).unwrap();
            assert_eq!(
                adapted, input,
                "a ceiling must not expand, truncate or reorder the prepared range"
            );
            assert_eq!(
                executor.internal_checkpoint_input(adapted.clone()).unwrap(),
                adapted
            );
            let descriptor = executor
                .registered_tool_effect("checkpoint", &input)
                .unwrap();
            assert_eq!(descriptor, builtin);
            let assessed = checkpoint_assessment(
                descriptor.clone(),
                &input.to_string(),
                PermissionMode::WorkspaceWrite,
                "r372-pure",
            );
            assert_eq!(
                assessed.effective.descriptor.required_permission,
                PermissionMode::WorkspaceWrite
            );
            assert_eq!(
                assessed.effective.descriptor.assessment.data_sensitivity,
                DataClassification::Internal
            );
            assert_ne!(
                assessed.assessment.gap.as_ref().map(|gap| gap.kind),
                Some(CapabilityGapKind::PermissionCeiling)
            );
            assert_eq!(
                checkpoint_assessment(
                    descriptor,
                    &input.to_string(),
                    PermissionMode::ReadOnly,
                    "r372-low"
                )
                .assessment
                .gap
                .unwrap()
                .kind,
                CapabilityGapKind::PermissionCeiling
            );
            assert!(executor.receipts.lock().unwrap().is_empty());
        }
        // Preserve the independently observed bad transform as a test-only
        // counterexample. Production must never apply it to an explicit range.
        let mut legacy_widened = input.clone();
        legacy_widened.as_object_mut().unwrap().remove("paths");
        let legacy = checkpoint_assessment(
            host.0.describe_effect("checkpoint", &legacy_widened),
            &input.to_string(),
            PermissionMode::WorkspaceWrite,
            "r371-old-widening",
        );
        assert_eq!(
            legacy.effective.descriptor.required_permission,
            PermissionMode::DangerFullAccess
        );
        assert_eq!(
            legacy.assessment.gap.unwrap().reason,
            "requested effect exceeds the parent Mission permission ceiling"
        );

        let mut executor = checkpoint_executor(root.path(), host, Some(vec!["workspace:.".into()]));
        for paths in [
            serde_json::json!([]),
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!([1]),
            serde_json::json!(["../outside"]),
            serde_json::json!(["/outside"]),
            serde_json::json!(["."]),
            serde_json::json!([".cowd/checkpoints"]),
            serde_json::json!(["project//a.txt"]),
            serde_json::json!(["new.txt/"]),
            serde_json::json!(["./new.txt"]),
        ] {
            let mut invalid = input.clone();
            invalid["paths"] = paths;
            assert!(
                executor.internal_checkpoint_input(invalid.clone()).is_err(),
                "{invalid}"
            );
            assert!(
                executor
                    .registered_tool_effect("checkpoint", &invalid)
                    .is_none()
            );
        }
        let mut invalid_action = input.clone();
        invalid_action["action"] = serde_json::json!("restore");
        assert!(executor.internal_checkpoint_input(invalid_action).is_err());
        executor.resource_scopes = Some(vec!["write:project/a.txt".into()]);
        assert!(
            executor.internal_checkpoint_input(input.clone()).is_err(),
            "one foreign member must reject the whole request, never silently truncate"
        );
        assert!(
            executor
                .internal_checkpoint_input(
                    serde_json::json!({"action":"create","paths":["project"]})
                )
                .is_err(),
            "a file lease does not cover its parent directory"
        );
        for scopes in [
            vec![],
            vec!["read:.".into()],
            vec!["write:foreign.txt".into()],
        ] {
            executor.resource_scopes = Some(scopes);
            assert!(executor.internal_checkpoint_input(input.clone()).is_err());
        }
        executor.resource_scopes = Some(vec!["workspace:.".into()]);
        let secret = serde_json::json!({"action":"create","paths":[".env"]});
        let secret_descriptor = executor
            .registered_tool_effect("checkpoint", &secret)
            .unwrap();
        assert_eq!(
            checkpoint_assessment(
                secret_descriptor,
                &secret.to_string(),
                PermissionMode::WorkspaceWrite,
                "r372-secret"
            )
            .assessment
            .gap
            .unwrap()
            .kind,
            CapabilityGapKind::PermissionCeiling
        );
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("project/a.txt", root.path().join("alias")).unwrap();
            std::os::unix::fs::symlink("absent", root.path().join("dangling")).unwrap();
            for path in ["alias", "dangling", "dangling/child.txt"] {
                assert!(
                    executor
                        .internal_checkpoint_input(
                            serde_json::json!({"action":"create","paths":[path]})
                        )
                        .is_err(),
                    "{path}"
                );
            }
        }
        assert_eq!(executor.next_receipt_sequence.load(Ordering::SeqCst), 0);
        assert!(!root.path().join(".cowd").exists());
        assert_eq!(
            std::fs::read(root.path().join("project/a.txt")).unwrap(),
            b"preimage"
        );
    }

    #[tokio::test]
    async fn r372_scoped_checkpoint_signed_input_matches_original_execution_range() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a.txt"), b"preimage").unwrap();
        std::fs::write(
            root.path().join("unrelated.txt"),
            b"not authorized for this checkpoint",
        )
        .unwrap();
        let host = Arc::new(super::super::tests::DiscoveryTestHost::new(root.path()));
        let executor =
            checkpoint_executor(root.path(), host.clone(), Some(vec!["workspace:.".into()]));
        let lease = host.tools.pin_snapshot();
        let definitions = lease
            .snapshot()
            .catalog
            .definitions(Some(&BTreeSet::from(["checkpoint".into()])))
            .into_iter()
            .map(|d| crate::ProviderToolDefinition {
                name: d.name,
                description: d.description,
                input_schema: d.input_schema,
            })
            .collect::<Vec<_>>();
        let input = serde_json::json!({"action":"create","label":"r372 exact signed range","paths":["a.txt"]});
        let wire = input.to_string();
        let step = Arc::new(
            crate::step_snapshot::BoundStep::bind(
                harness_contract::step::StepIdentity {
                    step_id: "r372-checkpoint-step".into(),
                    session_id: "r372-session".into(),
                    turn_id: "r372-turn".into(),
                    execution_id: Some("r372-graph".into()),
                    actor_ref: Some("runtime:checkpoint-guard".into()),
                    run_ref: None,
                    attempt: None,
                    epoch: None,
                    goal_ref: None,
                    program_ref: None,
                    task_ref: None,
                    build_ref: "unit".into(),
                },
                lease,
                &definitions,
                &serde_json::json!({"source":"runtime_system"}),
                &input,
                BTreeMap::new(),
                i64::MAX,
            )
            .unwrap()
            .with_parent("r372-original-step"),
        );
        let binding = crate::step_snapshot::StepCallBinding::system(
            step.clone(),
            "r372-original-step",
            "r372-system-checkpoint",
        );
        let idempotency = binding.effect_idempotency_key();
        let descriptor = executor
            .registered_tool_effect("checkpoint", &input)
            .unwrap();
        let assessed = checkpoint_assessment(
            descriptor,
            &wire,
            PermissionMode::WorkspaceWrite,
            &idempotency,
        );
        let authorization = crate::ToolPolicy
            .authorize(
                &assessed.effective,
                &assessed.assessment,
                idempotency,
                assessed
                    .assessment
                    .lease
                    .clone()
                    .expect("original pure negotiator must issue an allowed lease"),
                30,
            )
            .unwrap()
            .authorization;
        let forged_provider =
            crate::step_snapshot::StepCallBinding::provider(step, "r372-provider-checkpoint");
        assert!(
            executor
                .execute_step_authorized_invocation_output(
                    Some(&forged_provider),
                    &forged_provider.origin.call_id,
                    &authorization,
                    "checkpoint",
                    &wire
                )
                .await
                .unwrap_err()
                .to_string()
                .contains("SystemDerived")
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 0);
        let output = executor
            .execute_step_authorized_invocation_output(
                Some(&binding),
                &binding.origin.call_id,
                &authorization,
                "checkpoint",
                &wire,
            )
            .await
            .unwrap();
        let output: serde_json::Value = serde_json::from_str(output.model_text()).unwrap();
        let saved = std::path::PathBuf::from(output["path"].as_str().unwrap());
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(saved.join(".manifest.json")).unwrap()).unwrap();
        assert_eq!(manifest["scopes"], serde_json::json!(["a.txt"]));
        assert_eq!(std::fs::read(saved.join("a.txt")).unwrap(), b"preimage");
        assert!(!saved.join("unrelated.txt").exists());
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        let mut changed = input.clone();
        changed["paths"] = serde_json::json!(["unrelated.txt"]);
        assert!(
            executor
                .execute_step_authorized_invocation_output(
                    Some(&binding),
                    &binding.origin.call_id,
                    &authorization,
                    "checkpoint",
                    &changed.to_string()
                )
                .await
                .is_err(),
            "a different allowed path cannot borrow this input's signature"
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            executor.internal_checkpoint_input(input.clone()).unwrap(),
            input
        );
        assert!(executor.receipts.lock().unwrap().is_empty());
        assert_eq!(
            std::fs::read(root.path().join("a.txt")).unwrap(),
            b"preimage"
        );
    }
}

impl ScopedRuntimeToolExecutor {
    pub(super) fn provider_model_obligation_ids(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Vec<String> {
        if self.provider_model_obligations.is_empty()
            || !matches!(tool_name, "read_file" | "tool_orchestrate")
        {
            return Vec::new();
        }
        let Ok(normalized) = normalize_delegated_resource_paths(
            tool_name,
            input,
            &self.workspace_root,
            &self.path_identity_resolver,
            self.resource_scopes.as_deref(),
        ) else {
            return Vec::new();
        };
        let Some(descriptor) = serde_json::from_str::<serde_json::Value>(&normalized)
            .ok()
            .and_then(|input| {
                self.host
                    .delegated_tool_effect_descriptor(tool_name, &input)
            })
        else {
            return Vec::new();
        };
        let requested = crate::governed_tool_plan::resource_scope_from_effect(&descriptor);
        let requested_identities = requested
            .paths
            .iter()
            .filter_map(|path| self.path_identity_resolver.resolve_planned_file(path).ok())
            .collect::<Vec<_>>();
        let mut ids = self
            .provider_model_obligations
            .iter()
            .filter_map(|obligation| {
                let harness_contract::context::EvidenceTargetIdentity::Workspace { scope } =
                    &obligation.target
                else {
                    return None;
                };
                requested_identities
                    .iter()
                    .any(|requested| {
                        let same_workspace = requested.workspace_id == scope.path.workspace_id
                            && requested.repository_id == scope.path.repository_id;
                        let exact_path = requested.workspace_relative_path
                            == scope.path.workspace_relative_path;
                        let descendant_verification = obligation.kind
                            == harness_contract::context::EvidenceObligationKind::VerifyAfterWrite
                            && (scope.path.workspace_relative_path.is_empty()
                                || exact_path
                                || requested
                                    .workspace_relative_path
                                    .strip_prefix(&scope.path.workspace_relative_path)
                                    .is_some_and(|suffix| suffix.starts_with('/')));
                        same_workspace
                            && (exact_path || descendant_verification)
                            && (scope.coverage
                                == harness_contract::context::EvidenceCoverageKind::ExactContent
                                || obligation.kind
                                    == harness_contract::context::EvidenceObligationKind::VerifyAfterWrite)
                    })
                .then(|| obligation.obligation_id.clone())
            })
            .collect::<Vec<_>>();
        ids.sort();
        ids.dedup();
        ids
    }

    pub(super) fn internal_checkpoint_input(
        &self,
        input: serde_json::Value,
    ) -> Result<serde_json::Value, ToolError> {
        let mut object = input
            .as_object()
            .cloned()
            .ok_or_else(|| ToolError::new("Runtime checkpoint input must be a JSON object"))?;
        if let Some(value) = object.get("paths") {
            // The prepared operation owns its range. A Team lease is only its
            // ceiling: it must never replace this range or erase it to `None`.
            if object.get("action").and_then(serde_json::Value::as_str) != Some("create") {
                return Err(ToolError::new(
                    "explicit Runtime checkpoint requires action create",
                ));
            }
            let paths = value
                .as_array()
                .filter(|paths| !paths.is_empty())
                .ok_or_else(|| {
                    ToolError::new("Runtime checkpoint requires explicit nonempty paths")
                })?;
            for value in paths {
                let raw = value
                    .as_str()
                    .ok_or_else(|| ToolError::new("Runtime checkpoint path must be a string"))?;
                let path = std::path::Path::new(raw);
                if raw.is_empty() || raw.trim() != raw || raw.contains('\\')
                    || path.components().any(|part| !matches!(part, std::path::Component::Normal(name) if name != ".cowd"))
                    || path.components().collect::<std::path::PathBuf>().to_str() != Some(raw)
                {
                    return Err(ToolError::new("Runtime checkpoint path is not a canonical workspace range"));
                }
                let identity = self
                    .path_identity_resolver
                    .resolve_planned_file(raw)
                    .map_err(|error| ToolError::new(error.to_string()))?;
                if identity.workspace_relative_path != raw {
                    return Err(ToolError::new(
                        "Runtime checkpoint path identity changed its range",
                    ));
                }
                // Match the original checkpoint executor's no-symlink rule,
                // including a dangling link. It checks again at actual use.
                let mut cursor = self.workspace_root.clone();
                for component in path.components() {
                    cursor.push(component.as_os_str());
                    match std::fs::symlink_metadata(&cursor) {
                        Ok(metadata) if metadata.file_type().is_symlink() => {
                            return Err(ToolError::new(
                                "Runtime checkpoint path crosses a symlink",
                            ));
                        }
                        Ok(_) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                        Err(error) => return Err(ToolError::new(error.to_string())),
                    }
                }
                if let Some(scopes) = self.resource_scopes.as_deref() {
                    if !resource_path_is_authorized(&self.path_identity_resolver, raw, scopes, true)
                        && !crate::tool_host::whole_workspace_resource_lease(scopes, true)
                    {
                        return Err(ToolError::new(
                            "Runtime checkpoint path is outside the Team write scope",
                        ));
                    }
                }
            }
            // Preserve even ordering and duplicate values: the original
            // descriptor, signed input digest and executor see the same JSON.
            return Ok(input);
        }
        // Retain the original legacy no-path contract. It is not a fallback
        // for any malformed, empty or unauthorized explicit range above.
        object.insert("action".to_string(), serde_json::json!("create"));
        if let Some(scopes) = self.resource_scopes.as_deref() {
            let mut paths = scopes
                .iter()
                .filter_map(|scope| {
                    scope
                        .strip_prefix("write:")
                        .or_else(|| scope.strip_prefix("workspace:"))
                })
                .map(str::trim)
                .filter(|path| !path.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            paths.sort();
            paths.dedup();
            if paths.is_empty() {
                return Err(ToolError::new(
                    "Runtime checkpoint requires a bounded Team write scope",
                ));
            }
            if paths.iter().any(|path| path == ".") {
                // checkpoint_create defines an omitted/empty path set as a
                // whole-workspace snapshot. Passing the lexical root `.` is
                // rejected by its traversal guard, even though `write:.` is
                // the valid Team authority for that same workspace.
                object.remove("paths");
            } else {
                object.insert("paths".to_string(), serde_json::json!(paths));
            }
        }
        Ok(serde_json::Value::Object(object))
    }

    pub(super) async fn execute_internal_checkpoint(
        &self,
        input: &str,
        authorization: harness_contract::tool::ToolExecutionAuthorization,
        binding: Option<&crate::step_snapshot::StepCallBinding>,
    ) -> Result<String, ToolError> {
        let input = serde_json::from_str::<serde_json::Value>(input).map_err(|error| {
            ToolError::new(format!("invalid Runtime checkpoint input: {error}"))
        })?;
        let input = self.internal_checkpoint_input(input)?;
        if self
            .host
            .delegated_tool_effect_descriptor("checkpoint", &input)
            .is_none()
        {
            return Err(ToolError::new(
                "Runtime ToolHost has no checkpoint_create capability",
            ));
        }
        let request = RuntimeToolExecutionRequest {
            compiled_invocation: binding
                .map(|binding| {
                    binding.prepare_invocation(self.host.pin_step_catalog(), "checkpoint", &input)
                })
                .transpose()
                .map_err(|error| ToolError::new(error.to_string()))?,
            step_binding: binding.cloned(),
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: 1,
            observation_wave_sequence: 0,
            idempotency_key: binding
                .map(crate::step_snapshot::StepCallBinding::effect_idempotency_key)
                .or_else(|| authorization.idempotency_key.clone())
                .unwrap_or_else(|| format!("agent-checkpoint:{}", uuid::Uuid::new_v4())),
            tool_use_id: binding
                .map(|binding| binding.origin.call_id.clone())
                .unwrap_or_else(|| format!("agent-checkpoint:{}", uuid::Uuid::new_v4())),
            tool_name: "checkpoint".to_string(),
            input: serde_json::to_string(&input).map_err(|error| {
                ToolError::new(format!("serialize Runtime checkpoint input: {error}"))
            })?,
            category: crate::ToolSafetyCategory::WriteLocal,
            authorization: Some(authorization),
            session_id: Some(self.session_id.clone()),
            sandbox_posture: self.sandbox_posture,
            policy_revision: self.policy_revision,
            authorized_scopes: Vec::new(),
            memory_context: Some(self.memory_context.clone()),
            reality_context: self.reality_context.clone(),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            parent_execution_attempt: Some(self.attempt),
            execution_decision: None,
            // An Agent evaluation Binding is candidate provenance, not the
            // tool-free Judge surface. The exact Team resource ceiling above
            // remains the business-effect sandbox. This checkpoint is a
            // Runtime-owned guard and is deliberately not an Agent effect.
            evaluation_isolated: self.evaluation_isolated(),
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        };
        if let Some(stop) = crate::bound_tool_batch::current_tool_stop() {
            stop.check()?;
            stop.effect_entered.store(true, Ordering::Release);
        }
        let outcome = self.host.execute_runtime_tool(&request).await;
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => Ok(outcome.output.unwrap_or_default()),
            RuntimeToolExecutionStatus::BlockedPermission => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "checkpoint blocked by policy".into()),
            )),
            RuntimeToolExecutionStatus::Failed => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "checkpoint creation failed".into()),
            )),
            RuntimeToolExecutionStatus::Unknown => Err(ToolError::unknown(
                outcome.error.unwrap_or_else(|| "checkpoint outcome unknown".into()),
            )),
        }
    }

    pub(super) async fn execute_delegated_runtime_tool(
        &self,
        tool_name: &str,
        input: &str,
        authorization: harness_contract::tool::ToolExecutionAuthorization,
        step_binding: Option<&crate::step_snapshot::StepCallBinding>,
    ) -> Result<String, ToolError> {
        let input = serde_json::from_str::<serde_json::Value>(input).map_err(|error| {
            ToolError::new(format!("invalid Runtime delegated tool input: {error}"))
        })?;
        let descriptor = step_binding
            .map(|binding| {
                binding
                    .resolve(self.host.pin_step_catalog())
                    .map(|step| step.lease().describe_effect(tool_name, &input))
            })
            .transpose()
            .map_err(|error| ToolError::new(error.to_string()))?
            .or_else(|| {
                self.host
                    .delegated_tool_effect_descriptor(tool_name, &input)
            })
            .ok_or_else(|| {
                ToolError::new("Runtime delegated tool has no registered effect descriptor")
            })?;
        let category = crate::ToolSafetyCategory::from_effect(&descriptor);
        let read_sequence = if tool_name == "evidence_retrieve" {
            self.next_receipt_sequence
                .fetch_add(1, Ordering::SeqCst)
                .saturating_add(1)
        } else {
            0
        };
        // A governed action already has an authorized request owned by its
        // ToolBatch. Carry that exact request into the child graph: rebuilding
        // it here changes the wave/scopes under the same effect identity.
        let original = if harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name) {
            match crate::tool_host::ORIGINAL_EFFECT_REQUEST.try_with(|slot| {
                slot.lock().map_err(|_| ToolError::unknown("original request channel poisoned"))?.clone()
                    .ok_or_else(|| ToolError::unknown("original delegated action request missing"))
            }) {
                Ok(result) => Some(result?),
                // A direct Process producer has its own original owner; it is
                // not a Ticket call and continues through that existing path.
                Err(_) if crate::tool_host::ORIGINAL_MEMBER_PROGRESS.try_with(|_| ()).is_ok() =>
                    return Err(ToolError::unknown("original Ticket action request channel missing")),
                Err(_) => None,
            }
        } else { None };
        let direct_process_origin = original.is_none();
        let request = if let Some(original) = original {
            if original.tool_progress.original_fresh_effect()
                .map_err(ToolError::unknown)?.is_none()
            {
                // The parent refused this delegated action before it was
                // admitted. Name the missing admission, carry the facts that
                // identify the refused member, and read the parent's own
                // named refusal back from the observation this child was bound
                // to, so the run's error text says which conjunct refused it.
                let parent_reason = crate::agent_in_process_worker::current_process_member_custody(&original)
                    .ok()
                    .flatten()
                    .and_then(|custody| custody.parent_refusal_reason(&original.tool_use_id));
                return Err(ToolError::unknown(original_effect_admission_missing_reason(
                    &original.tool_use_id,
                    &original.idempotency_key,
                    tool_name,
                    self.attempt,
                    parent_reason.as_deref(),
                )));
            }
            let original_input = serde_json::from_str::<serde_json::Value>(&original.input)
                .map_err(|error| ToolError::unknown(format!("original delegated input invalid: {error}")))?;
            let expected_parent = harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(), node_id: self.node_id.clone(),
            };
            if original.tool_name != tool_name
                || original_input != input
                || original.authorization.as_ref() != Some(&authorization)
                || original.step_binding.as_ref() != step_binding
                || original.session_id.as_deref() != Some(self.session_id.as_str())
                || original.parent_execution.as_ref() != Some(&expected_parent)
                || original.parent_execution_attempt != Some(self.attempt)
                || original.sandbox_posture != self.sandbox_posture
                || original.policy_revision != self.policy_revision
            {
                return Err(ToolError::unknown("original delegated action request identity mismatch"));
            }
            (*original).clone()
        } else { RuntimeToolExecutionRequest {
            compiled_invocation: step_binding
                .map(|binding| {
                    binding.prepare_invocation(self.host.pin_step_catalog(), tool_name, &input)
                })
                .transpose()
                .map_err(|error| ToolError::new(error.to_string()))?,
            step_binding: step_binding.cloned(),
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: 1,
            observation_wave_sequence: read_sequence,
            idempotency_key: step_binding
                .map(crate::step_snapshot::StepCallBinding::effect_idempotency_key)
                .or_else(|| authorization.idempotency_key.clone())
                .unwrap_or_else(|| format!("agent-runtime-tool:{}", uuid::Uuid::new_v4())),
            tool_use_id: step_binding
                .map(|binding| binding.origin.call_id.clone())
                .unwrap_or_else(|| {
                    format!("agent-runtime-tool:{}:{}", tool_name, uuid::Uuid::new_v4())
                }),
            tool_name: tool_name.to_string(),
            input: serde_json::to_string(&input).map_err(|error| {
                ToolError::new(format!("serialize Runtime delegated tool input: {error}"))
            })?,
            category,
            authorization: Some(authorization),
            session_id: Some(self.session_id.clone()),
            sandbox_posture: self.sandbox_posture,
            policy_revision: self.policy_revision,
            authorized_scopes: self.authorized_scopes_for_tool(),
            memory_context: Some(self.memory_context.clone()),
            reality_context: self.reality_context.clone(),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            parent_execution_attempt: Some(self.attempt),
            execution_decision: None,
            evaluation_isolated: self.evaluation_isolated(),
            managed_invocation: None,
            tool_progress: crate::ToolProgressSink::default(),
        }};
        if direct_process_origin && harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name) {
            request.tool_progress.install_original_effect_custody(crate::tool_host::OriginalEffectCustodyOrigin::DirectProcess)
                .map_err(ToolError::unknown)?;
        }
        if tool_name == "agent_action_batch" {
            if crate::agent_in_process_worker::current_original_process_batch_call().is_some() {
                let pinned = self.host.pin_step_catalog().ok_or_else(|| ToolError::unknown("Process original ToolHost lease missing"))?;
                crate::agent_in_process_worker::capture_original_process_members(&request, pinned)
                    .map_err(ToolError::unknown)?;
            }
            let owner = if crate::agent_in_process_worker::current_original_process_batch_call().is_some() {
                crate::agent_in_process_worker::original_process_batch_policy_owner().map_err(ToolError::unknown)?
            } else {
                crate::tool_host::DELEGATED_BATCH_POLICY_OWNER.try_with(Arc::clone)
                    .map_err(|_| ToolError::new("original delegated batch policy owner missing"))?
            };
            request.tool_progress.install_batch_policy_owner(owner).map_err(ToolError::new)?;
        }
        let outcome = if tool_name == "agent_action_batch"
            || harness_contract::agent_action::AGENT_ACTION_TOOL_IDS.contains(&tool_name) {
            // The original dispatcher owns packet, node ticket and admission.
            // A direct Host call has none of those signing credentials.
            let dispatcher = self.tool_batch.as_ref().ok_or_else(||
                ToolError::new("unknown: original delegated action owner unavailable"))?;
            dispatcher.execute(request.clone()).await?
        } else if tool_name == "artifact_materialize" {
            if let Some(dispatcher) = &self.tool_batch {
                dispatcher.execute(request.clone()).await?
            } else {
                if let Some(stop) = crate::bound_tool_batch::current_tool_stop() {
                    stop.check()?;
                    stop.effect_entered.store(true, Ordering::Release);
                }
                self.host.execute_runtime_tool(&request).await
            }
        } else {
            if let Some(stop) = crate::bound_tool_batch::current_tool_stop() {
                stop.check()?;
                stop.effect_entered.store(true, Ordering::Release);
            }
            self.host.execute_runtime_tool(&request).await
        };
        // Unknown is not a completed read and must not mint a read receipt.
        if outcome.status == RuntimeToolExecutionStatus::Unknown {
            return Err(ToolError::unknown(outcome.error.unwrap_or_else(||
                format!("{tool_name} outcome unknown"))));
        }
        // Process direct calls have no outer receipt owner, even when they
        // carry a Step. Only suppress this commit inside the exact matching
        // outer governed read's task-local ownership scope.
        if tool_name == "evidence_retrieve" && !crate::bound_tool_batch::outer_read_receipt_owned(&request) {
            if let Some(commit) = &self.commit_service {
                commit
                    .commit_readonly_tool_receipts(&[(request.clone(), outcome.clone())])
                    .map_err(|error| {
                        ToolError::new(format!(
                            "evidence read completed but durable receipt commit failed: {error}"
                        ))
                    })?;
            }
        }
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => Ok(outcome.output.unwrap_or_default()),
            RuntimeToolExecutionStatus::BlockedPermission => {
                Err(ToolError::new(outcome.error.unwrap_or_else(|| {
                    format!("{tool_name} blocked by policy").into()
                })))
            }
            RuntimeToolExecutionStatus::Failed => {
                Err(ToolError::new(outcome.error.unwrap_or_else(|| {
                    format!("{tool_name} execution failed").into()
                })))
            }
            RuntimeToolExecutionStatus::Unknown => Err(ToolError::unknown(
                outcome.error.unwrap_or_else(|| format!("{tool_name} outcome unknown")),
            )),
        }
    }

    pub(super) fn authorized_scopes_for_tool(&self) -> Vec<String> {
        let mut scopes = self.resource_scopes.clone().unwrap_or_default();
        let session_scope = format!("session:{}", self.session_id);
        if !scopes.iter().any(|scope| scope == &session_scope) {
            scopes.push(session_scope);
        }
        scopes
    }

    pub(super) fn evaluation_isolated(&self) -> bool {
        self.resource_scopes.as_ref().is_some_and(|scopes| {
            scopes
                .iter()
                .any(|scope| scope.starts_with("write:.cowd/evaluation/"))
        })
    }

    pub(super) fn enforce_resource_ceiling(
        &self,
        tool_name: &str,
        input: &str,
    ) -> Result<(), ToolError> {
        let Some(allowed_scopes) = self.resource_scopes.as_deref() else {
            return Ok(());
        };
        // Context retrieval is bounded by the Runtime-issued MemoryTurnContext
        // and durable Session actor/workspace checks, not by a filesystem path.
        // Treating its read-only runtime scope as an unbounded path would make
        // Team Agents lose the context continuity that the primary Agent has.
        if matches!(
            tool_name,
            "context_retrieve" | "working_context" | "private_note"
        ) || is_agent_action_tool(tool_name)
        {
            return Ok(());
        }
        let value = serde_json::from_str::<serde_json::Value>(input)
            .map_err(|error| ToolError::new(format!("invalid scoped tool input: {error}")))?;
        let descriptor = self
            .host
            .delegated_tool_effect_descriptor(tool_name, &value)
            .ok_or_else(|| ToolError::new("tool has no enforceable Runtime effect descriptor"))?;
        self.enforce_original_effect_ceiling(tool_name, &value, &descriptor, allowed_scopes)
    }

    pub(super) fn enforce_original_effect_ceiling(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
        descriptor: &harness_contract::tool::ToolEffectDescriptor,
        allowed_scopes: &[String],
    ) -> Result<(), ToolError> {
        if tool_name == "glob_search" {
            enforce_glob_scope(input, allowed_scopes)?;
        }
        if !crate::tool_host::effect_has_resource_lease(
            descriptor,
            allowed_scopes,
            |path, scopes, write| {
                resource_path_is_authorized(&self.path_identity_resolver, path, scopes, write)
                    || (crate::tool_host::whole_workspace_resource_lease(scopes, write)
                        && self
                            .path_identity_resolver
                            .resolve_planned_file(path)
                            .is_ok())
            },
        ) {
            return Err(ToolError::new(format!(
                "tool `{tool_name}` is outside the Agent network/File operation resource lease"
            )));
        }
        Ok(())
    }

    pub(super) async fn execute_scoped(
        &self,
        tool_name: &str,
        input: &str,
        authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
        provider_invocation_id: Option<&str>,
    ) -> Result<String, ToolError> {
        self.execute_scoped_bound(
            tool_name,
            input,
            authorization,
            provider_invocation_id,
            None,
        )
        .await
    }

    pub(super) async fn execute_scoped_bound(
        &self,
        tool_name: &str,
        input: &str,
        authorization: Option<harness_contract::tool::ToolExecutionAuthorization>,
        provider_invocation_id: Option<&str>,
        step_binding: Option<&crate::step_snapshot::StepCallBinding>,
    ) -> Result<String, ToolError> {
        let parsed_input = serde_json::from_str::<serde_json::Value>(input)
            .map_err(|error| ToolError::new(format!("invalid scoped tool input: {error}")))?;
        let descriptor = step_binding
            .map(|binding| {
                binding
                    .resolve(self.host.pin_step_catalog())
                    .map(|step| step.lease().describe_effect(tool_name, &parsed_input))
            })
            .transpose()
            .map_err(|error| ToolError::new(error.to_string()))?
            .or_else(|| {
                self.host
                    .delegated_tool_effect_descriptor(tool_name, &parsed_input)
            })
            .ok_or_else(|| ToolError::new("tool has no enforceable Runtime effect descriptor"))?;
        let requested = crate::governed_tool_plan::resource_scope_from_effect(&descriptor);
        let resource_scopes = crate::tool_host::effect_resource_evidence(&descriptor);
        let sequence = self
            .next_receipt_sequence
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        let idempotency_key = step_binding
            .map(crate::step_snapshot::StepCallBinding::effect_idempotency_key)
            .or_else(|| {
                authorization
                    .as_ref()
                    .and_then(|value| value.idempotency_key.clone())
            })
            .unwrap_or_else(|| {
                deterministic_scoped_tool_idempotency_key(
                    &self.execution_id,
                    &self.node_id,
                    self.attempt,
                    sequence,
                    tool_name,
                    input,
                )
            });
        let request = RuntimeToolExecutionRequest {
            compiled_invocation: step_binding
                .map(|binding| {
                    binding.prepare_invocation(
                        self.host.pin_step_catalog(),
                        tool_name,
                        &parsed_input,
                    )
                })
                .transpose()
                .map_err(|error| ToolError::new(error.to_string()))?,
            step_binding: step_binding.cloned(),
            governed_plan_id: self.execution_id.clone(),
            governed_plan_revision: sequence,
            observation_wave_sequence: sequence,
            idempotency_key,
            tool_use_id: step_binding
                .map(|binding| binding.origin.call_id.clone())
                .unwrap_or_else(|| {
                    format!(
                        "agent-tool:{}:{}:{}:{tool_name}",
                        self.node_id, self.attempt, sequence
                    )
                }),
            tool_name: tool_name.to_string(),
            input: input.to_string(),
            category: crate::ToolSafetyCategory::from_effect(&descriptor),
            authorization,
            session_id: Some(self.session_id.clone()),
            sandbox_posture: self.sandbox_posture,
            policy_revision: self.policy_revision,
            authorized_scopes: self.authorized_scopes_for_tool(),
            memory_context: Some(self.memory_context.clone()),
            reality_context: self.reality_context.clone(),
            model_lease: Some(self.model_lease.clone()),
            parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: self.execution_id.clone(),
                node_id: self.node_id.clone(),
            }),
            parent_execution_attempt: Some(self.attempt),
            execution_decision: None,
            evaluation_isolated: self.evaluation_isolated(),
            managed_invocation: self.managed_invocation.clone(),
            tool_progress: crate::ToolProgressSink::default(),
        };
        if tool_name == "agent_action_batch" {
            if crate::agent_in_process_worker::current_original_process_batch_call().is_some() {
                let pinned = self.host.pin_step_catalog().ok_or_else(|| ToolError::unknown("Process original ToolHost lease missing"))?;
                crate::agent_in_process_worker::capture_original_process_members(&request, pinned)
                    .map_err(ToolError::unknown)?;
            }
            // C's retained OriginalAgentChildCall carries this same in-process
            // sink through the physical owner. The serialized child payload
            // cannot recreate or authorize the original policy owner.
            let owner = if crate::agent_in_process_worker::current_original_process_batch_call().is_some() {
                crate::agent_in_process_worker::original_process_batch_policy_owner().map_err(ToolError::unknown)?
            } else {
                crate::tool_host::DELEGATED_BATCH_POLICY_OWNER.try_with(Arc::clone)
                    .map_err(|_| ToolError::unknown("original delegated batch policy owner unavailable"))?
            };
            request.tool_progress.install_batch_policy_owner(owner).map_err(ToolError::unknown)?;
        }
        let mut outcome = if let Some(dispatcher) = &self.tool_batch {
            dispatcher.execute(request.clone()).await?
        } else {
            crate::bound_tool_batch::execute_bound_agent_tool(
                self.host.as_ref(),
                self.commit_service.as_ref(),
                &self.path_identity_resolver,
                &self.scope_locks,
                &request,
                &descriptor,
            )
            .await?
        };
        // The delegated read receipt is now committed (or was recovered from
        // that committed receipt). Bind any typed observation to that durable
        // Runtime event before the Agent terminal consumes it. Gateway may
        // initially expose the observation with an unavailable raw-artifact
        // selector because transcript compaction happens later; the effect
        // receipt itself is already durable and is the correct provenance
        // carrier for acceptance.
        if outcome.status == RuntimeToolExecutionStatus::Executed
            && descriptor.effect_kind == harness_contract::tool::ToolEffectKind::Read
            && self.commit_service.is_some()
            && !outcome.observed_evidence.is_empty()
        {
            let output = outcome.output.as_deref().unwrap_or_default();
            // Composable orchestration outputs carry a `calls` trace. Bind its
            // canonical hash into the evidence resource id so the sub-call set
            // is independently identified, not just embedded in the blob.
            let sub_calls_suffix = serde_json::from_str::<serde_json::Value>(output)
                .ok()
                .and_then(|value| value.get("calls").cloned())
                .map(|calls| {
                    format!(
                        ":sub-calls:{:x}",
                        Sha256::digest(calls.to_string().as_bytes())
                    )
                })
                .unwrap_or_default();
            let access = harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed(
                    "delegated_agent_read_receipt",
                    format!("{}:read-receipt{sub_calls_suffix}", request.idempotency_key),
                ),
                format!("sha256:{:x}", Sha256::digest(output.as_bytes())),
                u64::try_from(output.len().max(1)).unwrap_or(u64::MAX),
                "application/vnd.cowd.tool-receipt+json",
                format!(
                    "event://execution-effect/{}/{}:read-receipt{sub_calls_suffix}",
                    request.idempotency_key, request.idempotency_key
                ),
                format!("session:{}", self.session_id),
            );
            for observed in &mut outcome.observed_evidence {
                if observed
                    .evidence_ref
                    .as_ref()
                    .is_none_or(|current| !current.is_durable())
                {
                    observed.evidence_ref = Some(access.clone());
                }
            }
        }
        match outcome.status {
            RuntimeToolExecutionStatus::Executed => {
                let observed_evidence = outcome.observed_evidence.clone();
                let prior_states = requested
                    .paths
                    .iter()
                    .filter_map(|path| {
                        let identity = self
                            .path_identity_resolver
                            .resolve_planned_file(path)
                            .ok()?;
                        observed_evidence
                            .iter()
                            .find_map(|evidence| match &evidence.target {
                                harness_contract::context::EvidenceTargetIdentity::Workspace {
                                    scope,
                                } if scope.path.workspace_id == identity.workspace_id
                                    && scope.path.repository_id == identity.repository_id
                                    && scope.path.workspace_relative_path
                                        == identity.workspace_relative_path =>
                                {
                                    evidence
                                        .workspace_prior_state
                                        .clone()
                                        .map(|state| (path.clone(), state))
                                }
                                _ => None,
                            })
                    })
                    .collect::<BTreeMap<_, _>>();
                let after_digests = requested
                    .paths
                    .iter()
                    .map(|path| {
                        let identity = self.path_identity_resolver.resolve_planned_file(path).ok();
                        let digest =
                            identity.as_ref().and_then(|identity| {
                                observed_evidence.iter().find_map(|evidence| {
                                    match &evidence.target {
                                harness_contract::context::EvidenceTargetIdentity::Workspace {
                                    scope,
                                } if scope.path.workspace_id == identity.workspace_id
                                    && scope.path.repository_id == identity.repository_id
                                    && scope.path.workspace_relative_path
                                        == identity.workspace_relative_path =>
                                {
                                    scope.path.observed_revision_or_digest.clone()
                                }
                                _ => None,
                            }
                                })
                            });
                        (path.clone(), digest)
                    })
                    .collect::<BTreeMap<_, _>>();
                let output = outcome.output.unwrap_or_default();
                let observed_bytes = if requested.paths.len() == 1 {
                    tool_output_byte_length(&output)
                        .map(|bytes| BTreeMap::from([(requested.paths[0].clone(), bytes)]))
                        .unwrap_or_default()
                } else {
                    BTreeMap::new()
                };
                self.receipts
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(ScopedToolExecutionReceipt {
                        sequence: if let Some(commit) = &self.commit_service {
                            commit.exact_delegated_receipt_sequence(&request)
                                .map_err(|error| ToolError::new(error.to_string()))?
                        } else { sequence },
                        provider_invocation_id: provider_invocation_id.map(str::to_string),
                        tool_name: tool_name.to_string(),
                        effect_kind: descriptor.effect_kind,
                        resource_scopes,
                        paths: requested.paths,
                        prior_states,
                        after_digests,
                        observed_bytes,
                        observed_evidence,
                    });
                // J3: remember the owning Session for a freshly started
                // background job (delegated-agent path).
                if let Ok(value) = serde_json::from_str::<serde_json::Value>(&output) {
                    if let Some(job_id) = value.get("backgroundTaskId").and_then(|id| id.as_str()) {
                        tools::background_jobs::register_owner(
                            &self.workspace_root,
                            job_id,
                            &self.session_id,
                        );
                    }
                }
                Ok(output)
            }
            RuntimeToolExecutionStatus::BlockedPermission => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "tool blocked by policy".into()),
            )),
            RuntimeToolExecutionStatus::Failed => Err(ToolError::new(
                outcome
                    .error
                    .unwrap_or_else(|| "tool execution failed".into()),
            )),
            RuntimeToolExecutionStatus::Unknown => Err(ToolError::unknown(
                outcome.error.unwrap_or_else(|| "tool outcome unknown".into()),
            )),
        }
    }
}

/// The error text for a delegated action whose original effect was never
/// admitted: the missing admission is named and the refused member is
/// identified. It reports facts, not an interpretation.
pub(crate) fn original_effect_admission_missing_reason(
    tool_use_id: &str,
    effect_key: &str,
    tool_name: &str,
    attempt: u32,
    parent_reason: Option<&str>,
) -> String {
    let mut reason = format!(
        "original_effect_admission_missing tool_use_id={tool_use_id} effect_key={effect_key} tool_name={tool_name} attempt={attempt}"
    );
    // The parent's own named refusal, carried verbatim. It is the admission
    // owner's wording, so this line never invents a reason of its own.
    if let Some(parent_reason) = parent_reason {
        reason.push_str(" parent_refusal=");
        reason.push_str(parent_reason);
    }
    reason
}

#[cfg(test)]
mod original_effect_admission_tests {
    use super::original_effect_admission_missing_reason;

    /// The refusal must name the missing admission and identify the member.
    #[test]
    fn the_missing_admission_is_named_and_the_member_identified() {
        let reason = original_effect_admission_missing_reason(
            "chatcmpl-tool-1",
            "sha256:key",
            "task_publish",
            3,
            None,
        );
        assert!(reason.starts_with("original_effect_admission_missing"), "{reason}");
        assert!(reason.contains("tool_use_id=chatcmpl-tool-1"), "{reason}");
        assert!(reason.contains("effect_key=sha256:key"), "{reason}");
        assert!(reason.contains("tool_name=task_publish"), "{reason}");
        assert!(reason.contains("attempt=3"), "{reason}");
    }

    /// When the node's owner recorded a named refusal for this call, it is
    /// carried verbatim: the run's error text then names the conjunct instead
    /// of only saying the admission is missing.
    #[test]
    fn the_parent_named_refusal_is_carried_verbatim() {
        let reason = original_effect_admission_missing_reason(
            "chatcmpl-tool-1",
            "sha256:key",
            "task_publish",
            3,
            Some("tool_effect_authority: claim_status_mismatch task_ref=task:3e7f"),
        );
        assert!(reason.contains("attempt=3"), "{reason}");
        assert!(
            reason.contains("parent_refusal=tool_effect_authority: claim_status_mismatch task_ref=task:3e7f"),
            "{reason}"
        );
        assert!(!reason.contains("parent_refusal=tool_effect_authority: claim_status_mismatch task_ref=task:3e7f parent_refusal"), "{reason}");
    }
}
