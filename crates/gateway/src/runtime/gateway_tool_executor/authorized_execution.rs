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
        if !selector.starts_with("tool://") && !selector.starts_with("artifact://") {
            return serde_json::to_string_pretty(&serde_json::json!({
                "kind": "evidence_retrieve",
                "evidence_ref": input.evidence_ref,
                "available": false,
                "reason": "unsupported_ref",
                "hint": "Only durable tool:// raw-output or artifact:// content references are resolvable here; memory:/session:// refs must be read through context_retrieve",
            }))
            .map_err(|error| ToolError::new(error.to_string()));
        }
        let store = services.artifact_store();
        let store_selector = selector
            .strip_prefix("tool://")
            .map_or_else(|| selector.clone(), |id| format!("artifact://{id}"));
        let artifact = store.resolve(&store_selector).map_err(|error| {
            ToolError::new(format!("evidence_retrieve resolve failed: {error}"))
        })?;
        let fallback_scopes = session_id
            .map(|session| vec![format!("session:{session}")])
            .unwrap_or_default();
        let effective_scopes: &[String] = if authorized_scopes.is_empty() {
            &fallback_scopes
        } else {
            authorized_scopes
        };
        if !evidence_scope_allowed(effective_scopes, &artifact.visibility_scope) {
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
        let content = String::from_utf8_lossy(&bytes);
        let limit = input.limit.unwrap_or(8).clamp(1, 16);
        let query_terms = input
            .query
            .as_deref()
            .unwrap_or_default()
            .split(|character: char| !character.is_alphanumeric())
            .filter(|term| !term.trim().is_empty())
            .map(str::to_lowercase)
            .collect::<Vec<_>>();
        let all_chunks = content
            .chars()
            .collect::<Vec<_>>()
            .chunks(1_500)
            .map(|chunk| chunk.iter().collect::<String>())
            .collect::<Vec<_>>();
        let mut selected = all_chunks
            .iter()
            .enumerate()
            .filter(|(_, chunk)| {
                if query_terms.is_empty() {
                    true
                } else {
                    let normalized = chunk.to_lowercase();
                    query_terms.iter().any(|term| normalized.contains(term))
                }
            })
            .take(limit)
            .map(|(index, content)| {
                serde_json::json!({
                    "index": index,
                    "content": content,
                })
            })
            .collect::<Vec<_>>();
        if selected.is_empty() && !all_chunks.is_empty() {
            selected.push(serde_json::json!({
                "index": 0,
                "content": all_chunks[0],
                "query_match": false,
            }));
        }
        let selected_count = selected.len();
        serde_json::to_string_pretty(&serde_json::json!({
            "kind": "evidence_retrieve",
            "evidence_ref": input.evidence_ref,
            "available": true,
            "bytes": bytes.len(),
            "media_type": artifact.media_type,
            "query": input.query,
            "chunks": selected,
            "total_chunks": all_chunks.len(),
            "truncated": selected_count < all_chunks.len(),
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }
}
