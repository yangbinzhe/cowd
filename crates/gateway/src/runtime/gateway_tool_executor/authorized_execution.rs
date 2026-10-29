/// The Program-logical artifact key an `artifact://` selector spells, if it is one.
///
/// Runtime names a one-shot Task deliverable `artifact:<24 hex>` and repeats that
/// spelling in the completion gap and the citable-refs projection. A model that
/// adds the `://` itself produces a selector no ArtifactStore ever issued, so the
/// store either refuses it or - worse - resolves it against an unrelated entry.
/// Only the 24-hex shape is treated as logical: a real selector carries `art_`
/// or `sha256:` and keeps going straight to the store.
fn program_logical_artifact_key(selector: &str) -> Option<String> {
    let target = selector.strip_prefix("artifact://")?;
    if target.len() == 24 && target.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Some(format!("artifact:{target}"))
    } else {
        None
    }
}

impl GatewayToolExecutor {
    fn validated_bounded_process_lease(
        &self,
        request: &runtime::RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, ToolError> {
        if request.tool_name != "execute_code"
            || self
                .allowed_tools
                .as_ref()
                .is_some_and(|allowed| !allowed.contains("execute_code"))
        {
            return Err(ToolError::new("bounded process tool is not enabled"));
        }
        if request.evaluation_isolated
            && request.category != runtime::ToolSafetyCategory::ReadOnly
            && !request
                .authorized_scopes
                .iter()
                .any(|scope| scope.starts_with("write:.cowd/evaluation/"))
        {
            return Err(ToolError::new(
                "paired evaluation mutation is missing its Runtime-issued isolated output lease",
            ));
        }
        let services = self
            .upgrade_runtime_services()
            .ok_or_else(|| ToolError::new("bounded process Runtime unavailable"))?;
        let session = request
            .session_id
            .as_deref()
            .ok_or_else(|| ToolError::new("bounded process Session missing"))?;
        let policy = services
            .session_execution_policy(session)
            .ok_or_else(|| ToolError::new("bounded process live policy missing"))?;
        if request.policy_revision == 0
            || policy.revision != request.policy_revision
            || policy.sandbox_posture != request.sandbox_posture
        {
            return Err(ToolError::new("bounded process live policy mismatch"));
        }
        let binding = request
            .step_binding
            .as_ref()
            .ok_or_else(|| ToolError::new("production_tool_origin_required"))?;
        let step = binding
            .resolve(Some(self.tool_host.pin_snapshot()))
            .map_err(|error| ToolError::new(error.to_string()))?;
        step.validate_call(
            &binding.origin,
            session,
            &request.tool_use_id,
            &request.tool_name,
            &request.input,
            chrono::Utc::now().timestamp_millis(),
        )
        .map_err(|error| ToolError::new(error.to_string()))?;
        let authorization = request
            .authorization
            .as_ref()
            .ok_or_else(|| ToolError::new("bounded process authorization missing"))?;
        if authorization.policy_revision != policy.revision {
            return Err(ToolError::new("bounded process grant policy mismatch"));
        }
        let input = harness_contract::step::parse_strict_json(&request.input)
            .map_err(|error| ToolError::new(error.to_string()))?;
        self.reject_orchestrated_tools_outside_allow_list("execute_code", &input)?;
        step.lease()
            .validate_authorization(authorization, "execute_code", &input)
            .map_err(|error| ToolError::new(error.to_string()))?;
        Ok(step.lease().clone())
    }

    async fn execute_authorized_output_with_progress(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        tool_name: &str,
        input: &str,
        progress: Option<&std::sync::Arc<dyn Fn(&str) + Send + Sync>>,
        step_lease: Option<tools::ToolHostLease>,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let is_step_bound = step_lease.is_some();
        let lease = step_lease.unwrap_or_else(|| self.tool_host.pin_snapshot());
        let tool_name = lease
            .snapshot()
            .catalog
            .canonical_name(tool_name)
            .ok_or_else(|| ToolError::new(format!("tool `{tool_name}` is not registered")))?;
        let value: serde_json::Value = serde_json::from_str(input)
            .map_err(|error| self.input_contract_error(&tool_name, error))?;
        self.reject_orchestrated_tools_outside_allow_list(&tool_name, &value)?;
        if tool_name == "execute_code" {
            return Err(ToolError::new(
                "governed execute_code requires the original bounded process owner",
            ));
        }
        if tool_name == "tool_search"
            || is_gateway_runtime_control_tool(&tool_name)
            || is_gateway_context_tool(&tool_name)
        {
            if tool_name != "tool_search" {
                lease
                    .validate_authorization(authorization, &tool_name, &value)
                    .map_err(|error| ToolError::new(error.to_string()))?;
            }
            return self
                .execute_runtime_tool_with_binding(
                    &tool_name,
                    value,
                    RuntimeToolExecutionBinding {
                        tool_lease: is_step_bound.then_some(&lease),
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
        let authorization = authorization.clone();
        if is_step_bound && tool_name == "tool_orchestrate" {
            let effect = lease.describe_effect(&tool_name, &value);
            if effect.effect_kind == harness_contract::tool::ToolEffectKind::Read
                && !effect.uses_network && !effect.spawns_process
            {
                let (output, reads) = runtime::ToolExecutionPlane::adapt_blocking(move || {
                    lease.execute_with_orchestrated_read_trace(&authorization, &value)
                        .map_err(|error| ToolError::new(error.to_string()))
                }).await.map_err(|error| ToolError::new(error.to_string()))??;
                return Ok(match reads {
                    Some(reads) => harness_contract::context::ToolOutputDraft::InProcess {
                        content: output,
                        evidence: harness_contract::context::InProcessToolEvidence::new(reads),
                    },
                    None => harness_contract::context::ToolOutputDraft::bounded_inline(output),
                });
            }
        }
        let output = if tool_name == "bash" {
            let progress = progress.cloned();
            lease
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
                lease
                    .execute(&authorization, &tool_name, &value)
                    .map_err(|error| ToolError::new(error.to_string()))
            })
            .await
            .map_err(|error| ToolError::new(error.to_string()))??
        };
        // J3: remember which Session owns a freshly started background job so a
        // later Session deletion can fence and reap it.
        if let (Some(session_id), Ok(value)) = (
            self.runtime_session_id.as_deref(),
            serde_json::from_str::<serde_json::Value>(&output),
        ) {
            if let Some(job_id) = value.get("backgroundTaskId").and_then(|id| id.as_str()) {
                let workspace = self.tool_host.pin_snapshot().workspace_root().to_path_buf();
                tools::background_jobs::register_owner(&workspace, job_id, session_id);
            }
        }
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
        let services = self.upgrade_runtime_services().ok_or_else(|| {
            ToolError::new("evidence_retrieve requires the workspace RuntimeServices")
        })?;
        let selector = input.evidence_ref.clone();
        if !selector.starts_with("tool://")
            && !selector.starts_with("artifact://")
            && !selector.starts_with("approval:v1:")
        {
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
            let session_id = session_id.ok_or_else(|| {
                ToolError::new("external decisions require an authenticated Session")
            })?;
            services
                .approval_result_content(session_id, &selector)
                .await
                .map_err(ToolError::new)?
                .0
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
        } else if let Some(logical_key) = program_logical_artifact_key(&selector) {
            // A 24-hex `artifact://` target is a Program-logical key by
            // construction: real content selectors are `artifact://art_<hex>` or
            // `artifact://sha256:<hex>`. Resolving it through the store first can
            // succeed against an unrelated entry and silently return the wrong
            // bytes - one live run read a deliverable four times and kept being
            // told it had not read it, because every read came back with the
            // source file instead of the report. Map the logical key to the
            // selector the Program recorded, and only fall back to the store when
            // no Program claims the key.
            let mapped = services
                .agent_action_service()
                .list_programs()
                .ok()
                .and_then(|programs| {
                    programs.iter().find_map(|program| {
                        program
                            .artifacts
                            .get(&logical_key)
                            .map(|artifact| artifact.content_ref.clone())
                    })
                });
            match mapped {
                Some(content_ref) => store.resolve(&content_ref).map_err(|error| {
                    ToolError::new(format!("evidence_retrieve resolve failed: {error}"))
                })?,
                None => store.resolve(&selector).map_err(|error| {
                    ToolError::new(format!("evidence_retrieve resolve failed: {error}"))
                })?,
            }
        } else {
            match store.resolve(&selector) {
                Ok(artifact) => artifact,
                Err(store_error) => {
                    // Runtime spells a one-shot Task deliverable `artifact:<24 hex>`
                    // and repeats that spelling in a completion gap. A model that
                    // adds the `://` itself gets a selector the ArtifactStore never
                    // issued. The Program projection holds the mapping from that
                    // logical key to the real selector, so consult it before
                    // refusing: the read is legitimate and the identity unambiguous.
                    let logical =
                        format!("artifact:{}", selector.trim_start_matches("artifact://"));
                    let mapped = services
                        .agent_action_service()
                        .list_programs()
                        .ok()
                        .and_then(|programs| {
                            programs.iter().find_map(|program| {
                                program
                                    .artifacts
                                    .get(&logical)
                                    .map(|artifact| artifact.content_ref.clone())
                            })
                        });
                    match mapped {
                        Some(content_ref) => store.resolve(&content_ref).map_err(|error| {
                            ToolError::new(format!("evidence_retrieve resolve failed: {error}"))
                        })?,
                        None => {
                            return Err(ToolError::new(format!(
                                "evidence_retrieve resolve failed: {store_error}"
                            )))
                        }
                    }
                }
            }
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
        let (max_bytes, continuation) = evidence_read_selection(&artifact, &input)?;
        let read = services
            .read_authorized_publication_page(
                &artifact.selector,
                &artifact.sha256,
                session_id.unwrap_or_default(),
                &effective_scopes,
                max_bytes,
                continuation.as_ref(),
            )
            .await
            .map_err(|e| ToolError::new(format!("evidence_retrieve read failed:{e}")))?;
        let page = publication_evidence_page(&read, &input)?;
        serde_json::to_string_pretty(&serde_json::json!({
            "kind": "evidence_retrieve",
            "evidence_ref": input.evidence_ref,
            "available": true,
            "bytes": artifact.bytes,
            "read_bytes": read.body.len(),
            "media_type": artifact.media_type,
            "encoding": page["encoding"],
            "query": input.query,
            "sha256": artifact.sha256,
            "chunks": page["chunks"],
            "total_bytes": page["total_bytes"],
            "chunk_bytes": page["chunk_bytes"],
            "total_chunks": page["total_chunks"],
            "truncated": page["truncated"],
            "next_cursor": page["next_cursor"],
            "next_request": page["next_request"],
            "coverage": page["coverage"],
        }))
        .map_err(|error| ToolError::new(error.to_string()))
    }
}

#[cfg(test)]
mod program_logical_key_tests {
    use super::program_logical_artifact_key;

    #[test]
    fn only_a_twenty_four_hex_artifact_selector_is_a_program_logical_key() {
        // Runtime names a one-shot Task deliverable `artifact:<24 hex>` and the
        // model routinely adds the `://` itself. That selector used to be
        // resolved against the store first, which could succeed against an
        // unrelated entry: one live run read a deliverable four times and kept
        // being told it had not read it, because every read returned the source
        // file instead of the report.
        assert_eq!(
            program_logical_artifact_key("artifact://576a1504640073bfc867d006").as_deref(),
            Some("artifact:576a1504640073bfc867d006")
        );
        // A real content selector is not a logical key and keeps going straight
        // to the store.
        assert_eq!(
            program_logical_artifact_key("artifact://art_4ada628cd226485d8e9ba4c14517d101"),
            None
        );
        assert_eq!(
            program_logical_artifact_key("artifact://sha256:07741e4ec372d6ce7d0391928a8ea9d5"),
            None
        );
        // Other schemes and malformed targets are not ours to reinterpret.
        assert_eq!(program_logical_artifact_key("tool://tool-raw-abc"), None);
        assert_eq!(
            program_logical_artifact_key("artifact://not-hex-at-all"),
            None
        );
        assert_eq!(
            program_logical_artifact_key("artifact://576a1504640073bfc867d00"),
            None
        );
    }
}
