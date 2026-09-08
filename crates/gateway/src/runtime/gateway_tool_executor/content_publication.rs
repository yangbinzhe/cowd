impl GatewayToolExecutor {
    fn ensure_publication_file_scope(
        &self,
        path: &str,
        write: bool,
        binding: RuntimeToolExecutionBinding<'_>,
    ) -> Result<(), ToolError> {
        let lease = self.tool_host.pin_snapshot();
        let requested = lease
            .path_policy()
            .resolve(path)
            .map_err(|error| ToolError::new(error.to_string()))?;
        if binding.parent_execution.is_none() {
            return Ok(());
        }
        let allowed = binding.authorized_scopes.iter().any(|scope| {
            let Some((mode, path)) = scope.split_once(':') else {
                return false;
            };
            if !matches!(mode, "write" | "workspace") && (write || mode != "read") {
                return false;
            }
            lease
                .path_policy()
                .resolve(path)
                .is_ok_and(|root| requested.starts_with(root))
        });
        if !allowed {
            return Err(ToolError::new("file is outside delegated resource scope"));
        }
        Ok(())
    }

    async fn execute_content_publication(
        &self,
        tool_name: &str,
        value: serde_json::Value,
        binding: RuntimeToolExecutionBinding<'_>,
    ) -> Result<String, ToolError> {
        use harness_contract::content_publication::{
            ArtifactMaterializeInput, ArtifactPublishInput,
        };
        use sha2::{Digest, Sha256};
        let services = self
            .runtime_services
            .get()
            .ok_or_else(|| ToolError::new("publication requires RuntimeServices"))?;
        let session_id = binding
            .session_id
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| ToolError::new("publication requires authenticated Session"))?;
        if tool_name == "artifact_materialize" {
            let request: ArtifactMaterializeInput = serde_json::from_value(value)
                .map_err(|error| self.input_contract_error(tool_name, error))?;
            let (artifact, bytes) = services
                .read_authorized_publication(
                    &request.content_ref,
                    &request.sha256,
                    session_id,
                    binding.authorized_scopes,
                )
                .await
                .map_err(ToolError::new)?;
            self.ensure_publication_file_scope(&request.path, true, binding)?;
            let policy = self.tool_host.pin_snapshot().path_policy().clone();
            let path = request.path;
            let (target, created) = runtime::ToolExecutionPlane::adapt_blocking(move || {
                tools::file_ops::materialize_file(&policy, &path, &bytes)
            })
            .await
            .map_err(|error| ToolError::new(error.to_string()))?
            .map_err(|error| ToolError::new(error.to_string()))?;
            return Ok(
                serde_json::json!({"content_ref": artifact.selector, "sha256": artifact.sha256,
                "bytes": artifact.bytes, "path": target, "created": created, "status": "materialized"})
                .to_string(),
            );
        }
        let request: ArtifactPublishInput = serde_json::from_value(value)
            .map_err(|error| self.input_contract_error(tool_name, error))?;
        let artifact = match request {
            ArtifactPublishInput::File {
                path,
                sha256,
                media_type,
            } => {
                self.ensure_publication_file_scope(&path, false, binding)?;
                let policy = self.tool_host.pin_snapshot().path_policy().clone();
                let expected = sha256.clone();
                let bytes = runtime::ToolExecutionPlane::adapt_blocking(move || {
                    tools::file_ops::snapshot_file(&policy, &path, &expected)
                })
                .await
                .map_err(|error| ToolError::new(error.to_string()))?
                .map_err(|error| ToolError::new(error.to_string()))?;
                services
                    .publish_authorized_content(&bytes, &sha256, &media_type, session_id)
                    .await
                    .map_err(ToolError::new)?
            }
            ArtifactPublishInput::MessageBlock {
                message_id,
                block_index,
                sha256,
                media_type,
            } => {
                let history = services
                    .session_history_reader()
                    .ok_or_else(|| ToolError::new("Session history is unavailable"))?;
                let message = history
                    .message_by_stable_id(session_id, &message_id)
                    .await
                    .map_err(|error| ToolError::new(error.to_string()))?
                    .ok_or_else(|| ToolError::new("authorized Session message not found"))?;
                let blocks: Vec<serde_json::Value> = serde_json::from_str(&message.content_json)
                    .map_err(|error| ToolError::new(error.to_string()))?;
                let block = blocks
                    .get(block_index)
                    .ok_or_else(|| ToolError::new("message block out of range"))?;
                let encoded =
                    serde_json::to_vec(block).map_err(|error| ToolError::new(error.to_string()))?;
                runtime::content_publication::verify_content_revision(&encoded, &sha256)
                    .map_err(ToolError::new)?;
                if block.get("type").and_then(serde_json::Value::as_str) != Some("text") {
                    return Err(ToolError::new(
                        "only an explicit text block can be published",
                    ));
                }
                let text = block
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .ok_or_else(|| ToolError::new("text block has no text"))?;
                let text_hash = format!("{:x}", Sha256::digest(text.as_bytes()));
                services
                    .publish_authorized_content(
                        text.as_bytes(),
                        &text_hash,
                        &media_type,
                        session_id,
                    )
                    .await
                    .map_err(ToolError::new)?
            }
            ArtifactPublishInput::Artifact {
                content_ref,
                sha256,
            } => {
                services
                    .read_authorized_publication(
                        &content_ref,
                        &sha256,
                        session_id,
                        binding.authorized_scopes,
                    )
                    .await
                    .map_err(ToolError::new)?
                    .0
            }
        };
        Ok(
            serde_json::json!({"content_ref": artifact.selector, "sha256": artifact.sha256,
            "bytes": artifact.bytes, "media_type": artifact.media_type,
            "read_request": {"evidence_ref": artifact.selector},
            "commit_input": {"content_ref": artifact.selector},
            "status": "published"})
            .to_string(),
        )
    }
}
