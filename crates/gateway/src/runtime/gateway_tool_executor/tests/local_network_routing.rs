    /// M3: the gateway routes a network read tool into the joined local-network
    /// path only when that path implements it, and a call that never enters
    /// the joined path mints no `LocalNetworkFact`.
    ///
    /// Every fixture here is one the gateway suite already owns: the executor's
    /// own Step attachment, the signed test authorization, the Runtime's
    /// governed invocation compiler, and the request skeleton of the runtime
    /// binding lifetime test. Nothing new is invented for this file.
    mod local_network_routing {
        use super::*;

        const SESSION: &str = "security-session";
        const POLICY_REVISION: u64 = 7;

        /// A gateway executor with a live Session policy, so a tool request
        /// reaches the routing decision instead of stopping in admission.
        fn routing_executor() -> (GatewayToolExecutor, Arc<runtime::RuntimeServices>) {
            let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
            let services = runtime::RuntimeServices::in_memory().expect("runtime services");
            executor
                .bind_runtime_services(&services)
                .expect("bind runtime services");
            services.publish_session_execution_policy(
                SESSION,
                runtime::permissions::SessionExecutionPolicyControl::from_policy(
                    harness_contract::policy::SessionExecutionPolicy::from_profile(
                        harness_contract::policy::AutonomyProfileId::Yolo,
                        POLICY_REVISION,
                        harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                    ),
                ),
            );
            (executor, services)
        }

        /// The request skeleton of the runtime binding lifetime test, carrying
        /// the governed invocation the router reads.
        fn routing_request(
            executor: &GatewayToolExecutor,
            services: &runtime::RuntimeServices,
            tool_name: &str,
            input: serde_json::Value,
        ) -> runtime::RuntimeToolExecutionRequest {
            let policy = services
                .session_execution_policy(SESSION)
                .expect("live Session policy");
            let dispatch = runtime::tool_dispatch::ToolRequest {
                tool_use_id: format!("localnet-{tool_name}"),
                tool_name: tool_name.to_string(),
                input: input.to_string(),
                depends_on: Vec::new(),
            };
            let mut request = runtime::RuntimeToolExecutionRequest::from_tool_request(&dispatch);
            request.session_id = Some(SESSION.to_string());
            request.policy_revision = POLICY_REVISION;
            request.sandbox_posture = policy.sandbox_posture;
            request.category = runtime::ToolSafetyCategory::ReadOnly;
            request.authorization = Some(signed_test_authorization(
                executor,
                tool_name,
                &input,
                POLICY_REVISION,
            ));
            attach_gateway_step(executor, &mut request);
            // The original call carries the Step's own effect identity, the
            // way the Runtime builds it for a bound call.
            request.idempotency_key = request
                .step_binding
                .as_ref()
                .expect("bound Step")
                .effect_idempotency_key();
            // The router decides on the governed invocation the Runtime
            // compiled for this call, bound to the same Step origin.
            let mut invocation = executor
                .prepare_governed_invocations(std::slice::from_ref(&dispatch))
                .into_iter()
                .next()
                .expect("governed invocation");
            invocation.origin = request
                .step_binding
                .as_ref()
                .map(|binding| binding.origin.clone());
            request.compiled_invocation = Some(invocation);
            request
        }

        /// How many `LocalNetworkFact` observations this outcome carries.
        fn local_network_facts(outcome: &runtime::RuntimeToolExecutionOutcome) -> usize {
            outcome
                .observed_evidence
                .iter()
                .filter(|observed| {
                    matches!(
                        observed.target,
                        harness_contract::context::EvidenceTargetIdentity::LocalNetwork { .. }
                    )
                })
                .count()
        }

        /// M3, first and second proposition: a network read tool the joined
        /// path does not implement runs the ordinary path, and the ordinary
        /// path mints no local-network fact.
        ///
        /// The ordinary path really runs the search and answers with the
        /// search's own receipt; it never answers with the joined path's
        /// refusal vocabulary, because the joined path is the only path that
        /// refuses for a local-network reason and the only path that can mint
        /// a fact. What this test pins is the path, not the provider's
        /// answer: a provider that is unavailable still answers here.
        #[tokio::test]
        async fn web_search_runs_the_ordinary_path_and_mints_no_local_network_fact() {
            let (executor, services) = routing_executor();
            let request = routing_request(
                &executor,
                &services,
                "web_search",
                json!({"query": "cowd routing behavior"}),
            );

            let outcome = runtime::RuntimeExecutionHost::execute_runtime_tool(&executor, &request).await;

            let error = outcome.error.clone().unwrap_or_default();
            assert!(
                !error.contains("local network"),
                "web_search must not take the joined local-network path: {error}"
            );
            assert_eq!(
                local_network_facts(&outcome),
                0,
                "the ordinary path mints no local-network fact: {outcome:?}"
            );
            // The ordinary path ran to its own end: a search receipt, not a
            // joined refusal.
            assert_eq!(
                outcome.status,
                runtime::RuntimeToolExecutionStatus::Executed,
                "{outcome:?}"
            );
            let receipt: serde_json::Value = serde_json::from_str(
                &outcome.output.clone().unwrap_or_default(),
            )
            .expect("ordinary search receipt json");
            assert!(
                receipt.get("sources").is_some(),
                "the ordinary path returns its search receipt: {receipt}"
            );
        }

        /// M3, third proposition: the joined path still owns `web_fetch` and
        /// still refuses a call whose original identity does not match. The
        /// refusal text is the joined path's own vocabulary, which the
        /// ordinary path never produces.
        #[tokio::test]
        async fn web_fetch_still_takes_the_joined_local_network_path() {
            let (executor, services) = routing_executor();
            let request = routing_request(
                &executor,
                &services,
                "web_fetch",
                json!({"url": "https://example.invalid/read", "prompt": "read the page"}),
            );

            let outcome = runtime::RuntimeExecutionHost::execute_runtime_tool(&executor, &request).await;

            let error = outcome.error.clone().unwrap_or_default();
            assert!(
                error.contains("local network original"),
                "web_fetch must still be routed into the joined path: {outcome:?}"
            );
            assert_eq!(
                outcome.status,
                runtime::RuntimeToolExecutionStatus::Failed,
                "the joined path refuses this call, it does not fall through: {outcome:?}"
            );
            assert_eq!(
                local_network_facts(&outcome),
                0,
                "a refused joined call seals no fact: {outcome:?}"
            );
        }

        /// M2-gateway: the gateway resolves the requested spelling before the
        /// routing decision, so the router and the joined executor read one
        /// accepted set. A separator or case variant of a joined tool still
        /// lands in the joined path, and a variant of a tool the joined path
        /// does not implement still does not.
        #[test]
        fn the_gateway_normalizes_the_spelling_before_the_routing_decision() {
            let executor = GatewayToolExecutor::new(None, false, GatewayToolRegistry::builtin());
            let lease = executor.tool_host.pin_snapshot();
            let catalog = &lease.snapshot().catalog;
            for (requested, canonical, joined) in [
                ("web_fetch", "web_fetch", true),
                ("web-fetch", "web_fetch", true),
                ("WEB-FETCH", "web_fetch", true),
                ("WebFetch", "web_fetch", true),
                ("web_search", "web_search", false),
                ("web-search", "web_search", false),
                ("websearch", "web_search", false),
            ] {
                let resolved = catalog
                    .canonical_name(requested)
                    .unwrap_or_else(|| panic!("{requested} must resolve in the gateway catalog"));
                assert_eq!(resolved, canonical, "{requested}");
                // The router reads the resolved name, so the spelling the
                // provider used cannot move a tool across the boundary.
                assert_eq!(
                    tools::host::joined_local_network_tool(&resolved),
                    joined,
                    "{requested} resolves to {canonical}"
                );
                // The executor accepts a joined call only when the name it saw
                // and the name it canonicalized are both implemented, so it is
                // never wider than the router.
                let executor_accepts = tools::host::joined_local_network_tool(requested)
                    && tools::host::joined_local_network_tool(&resolved);
                assert!(
                    !executor_accepts || joined,
                    "{requested}: the executor must never accept more than the router"
                );
            }
            // The raw spelling is not what the router may read: for `web-fetch`
            // the raw name is outside the accepted set while the resolved name
            // is inside it, so a router that read the raw spelling would send
            // it down the ordinary path and disagree with the executor.
            assert!(!tools::host::joined_local_network_tool("web-fetch"));
            assert!(tools::host::joined_local_network_tool("web_fetch"));
        }
    }
