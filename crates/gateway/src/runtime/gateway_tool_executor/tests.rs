#[cfg(test)]
mod tests {
    use super::*;

    fn attach_gateway_step(
        executor: &GatewayToolExecutor,
        request: &mut runtime::RuntimeToolExecutionRequest,
    ) {
        let lease = executor.tool_host.pin_snapshot();
        let advertised = lease
            .snapshot()
            .catalog
            .definitions(None)
            .into_iter()
            .filter(|definition| definition.name == request.tool_name)
            .map(|definition| runtime::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        assert_eq!(advertised.len(), 1);
        let step = runtime::step_snapshot::BoundStep::bind(
            harness_contract::step::StepIdentity {
                step_id: uuid::Uuid::new_v4().to_string(),
                session_id: request.session_id.clone().expect("session identity"),
                turn_id: "gateway-test-turn".to_string(),
                execution_id: None,
                actor_ref: None,
                run_ref: None,
                attempt: Some(1),
                epoch: None,
                goal_ref: None,
                program_ref: None,
                task_ref: None,
                build_ref: "gateway-unit-fixture".to_string(),
            },
            lease,
            &advertised,
            &json!({}),
            &json!({}),
            std::collections::BTreeMap::from([(
                "policy".to_string(),
                request.policy_revision.to_string(),
            )]),
            chrono::Utc::now().timestamp_millis() + 60_000,
        )
        .expect("actual catalog snapshot");
        request.step_binding = Some(runtime::step_snapshot::StepCallBinding::provider(
            Arc::new(step),
            &request.tool_use_id,
        ));
    }

    include!("tests/agentic_and_evidence.rs");
    include!("tests/content_publication_823c.rs");
    include!("tests/authorization.rs");
    include!("tests/context_retrieval.rs");
    include!("tests/receipts.rs");
    include!("tests/runtime_binding_lifetime.rs");
}
