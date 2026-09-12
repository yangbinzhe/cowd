impl GatewayToolExecutor {
    async fn execute_authorized_output_with_progress(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
        progress: Option<&std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let tool_name = <Self as ToolExecutor>::resolve_tool_name(self, tool_name)
            .ok_or_else(|| ToolError::new(format!("tool `{tool_name}` is not registered")))?;
        let value: serde_json::Value = serde_json::from_str(input)
            .map_err(|error| self.input_contract_error(&tool_name, error))?;
        if tool_name == "tool_search"
            || is_gateway_runtime_control_tool(&tool_name)
            || is_gateway_context_tool(&tool_name)
        {
            if tool_name != "tool_search" {
                self.tool_host
                    .pin_snapshot()
                    .validate_authorization(authorization, &tool_name, &value)
                    .map_err(|error| ToolError::new(error.to_string()))?;
            }
            return self
                .execute_runtime_tool_with_binding(
                    &tool_name,
                    value,
                    RuntimeToolExecutionBinding {
                        action_id: None,
                        session_id: self.runtime_session_id.as_deref(),
                        authorized_scopes: &[],
                        memory_context: self.runtime_memory_context.as_ref(),
                reality_context: None,
                        model_lease: self.runtime_model_lease.as_deref(),
                        parent_execution: None,
                        execution_decision: None,
                        permission_ceiling: authorization.authorization_lease.ceiling,
                    },
                )
                .await
                .map(harness_contract::context::ToolOutputDraft::bounded_inline);
        }
        let tool_host = Arc::clone(&self.tool_host);
        let authorization = authorization.clone();
        let output = if tool_name == "bash" {
            let progress = progress.cloned();
            tool_host
                .pin_snapshot()
                .execute_async_with_progress(
                    &authorization,
                    &tool_name,
                    &value,
                    progress.map(|callback| {
                        let callback: std::sync::Arc<
                            dyn Fn(tools::bash::BashProgressSample) + Send + Sync,
                        > = std::sync::Arc::new(move |sample| {
                            callback(&format!(
                                "stdout_bytes={} stderr_bytes={} at_ms={}",
                                sample.stdout_bytes, sample.stderr_bytes, sample.at_ms
                            ));
                        });
                        callback
                    }),
                )
                .await
                .map_err(|error| ToolError::new(error.to_string()))?
        } else {
            runtime::ToolExecutionPlane::adapt_blocking(move || {
                tool_host
                    .pin_snapshot()
                    .execute(&authorization, &tool_name, &value)
                    .map_err(|error| ToolError::new(error.to_string()))
            })
            .await
            .map_err(|error| ToolError::new(error.to_string()))??
        };
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            output,
        ))
    }

    async fn execute_evidence_retrieve(
        &self,
        input: EvidenceRetrieveToolRequest,
        session_id: Option<&str>,
        authorized_scopes: &[String],
    ) -> Result<String, ToolError> {
        let services = self.runtime_services.get().cloned().ok_or_else(|| {
            ToolError::new("evidence_retrieve requires the workspace RuntimeServices")
        })?;
        let selector = input.evidence_ref.clone();
        if !selector.starts_with("tool://") && !selector.starts_with("artifact://") && !selector.starts_with("approval:v1:") {
            return serde_json::to_string_pretty(&serde_json::json!({
                "kind": "evidence_retrieve",
                "evidence_ref": input.evidence_ref,
                "available": false,
                "reason": "unsupported_ref",
                "hint": "Use durable tool://, artifact:// or approval:v1: references; memory:/session:// refs must be read through context_retrieve",
            }))
            .map_err(|error| ToolError::new(error.to_string()));
        }
        let store = services.artifact_store();
        let artifact = if selector.starts_with("approval:v1:") {
            let session_id = session_id.ok_or_else(|| ToolError::new("external decisions require an authenticated Session"))?;
            services.approval_result_content(session_id, &selector).await
                .map_err(ToolError::new)?.0
        } else if let Some(evidence_id) = selector.strip_prefix("tool://") {
            let Some(session_id) = session_id else {
                return serde_json::to_string_pretty(&serde_json::json!({
                    "kind": "evidence_retrieve",
                    "evidence_ref": input.evidence_ref,
                    "available": false,
                    "reason": "session_id_required",
                    "hint": "tool:// evidence is resolved through the authenticated Session journal",
                }))
                .map_err(|error| ToolError::new(error.to_string()));
            };
            let access = services
                .session_evidence_access(session_id, evidence_id)
                .await
                .map_err(|error| {
                    ToolError::new(format!("evidence_retrieve Session resolve failed: {error}"))
                })?;
            let Some(access) = access else {
                return serde_json::to_string_pretty(&serde_json::json!({
                    "kind": "evidence_retrieve",
                    "evidence_ref": input.evidence_ref,
                    "available": false,
                    "reason": "not_found",
                    "hint": "No canonical durable receipt maps this logical evidence id to an Artifact",
                }))
                .map_err(|error| ToolError::new(error.to_string()));
            };
            harness_contract::context::ArtifactRef::durable(
                access.retrieval_selector,
                access.sha256,
                access.bytes,
                access.media_type,
                access.visibility_scope,
            )
        } else {
            store.resolve(&selector).map_err(|error| {
                ToolError::new(format!("evidence_retrieve resolve failed: {error}"))
            })?
        };
        let mut effective_scopes = authorized_scopes.to_vec();
        if let Some(session) = session_id {
            let session_scope = format!("session:{session}");
            if !effective_scopes.contains(&session_scope) {
                effective_scopes.push(session_scope);
            }
        }
        if !evidence_scope_allowed(&effective_scopes, &artifact.visibility_scope) {
            return serde_json::to_string_pretty(&serde_json::json!({
                "kind": "evidence_retrieve",
                "evidence_ref": input.evidence_ref,
                "available": false,
                "reason": "not_authorized_scope",
                "hint": "This evidence reference is outside the current session/team authorized scopes",
            }))
            .map_err(|error| ToolError::new(error.to_string()));
        }
        let bytes: Vec<u8> = store
            .read(&artifact, &artifact.visibility_scope, None)
            .await
            .map_err(|error| ToolError::new(format!("evidence_retrieve read failed: {error}")))?;
        let (content, encoding) = match std::str::from_utf8(&bytes) {
            Ok(text) => (text.to_string(), "utf8"),
            Err(_) => {
                if input.query.is_some() { return Err(ToolError::new("binary evidence does not support a text query; read without query or materialize the exact artifact")); }
                use base64::Engine;
                (base64::engine::general_purpose::STANDARD.encode(&bytes), "base64")
            }
        };
        let page = evidence_content_page(&content, &artifact.sha256, &input)?;
        serde_json::to_string_pretty(&serde_json::json!({
            "kind": "evidence_retrieve",
            "evidence_ref": input.evidence_ref,
            "available": true,
            "bytes": bytes.len(),
            "media_type": artifact.media_type,
            "encoding": encoding,
            "query": input.query,
            "sha256": artifact.sha256,
            "chunks": page["chunks"],
            "total_chunks": page["total_chunks"],
            "truncated": page["truncated"],
            "next_cursor": page["next_cursor"],
            "next_request": page["next_request"],
            "coverage": page["coverage"],
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }
}
