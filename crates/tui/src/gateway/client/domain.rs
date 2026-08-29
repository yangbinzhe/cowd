use super::*;
impl GatewayApiClient {
    pub async fn tool_registry(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_TOOLS.template())
            .await
    }

    pub async fn tool_execute(
        &self,
        name: &str,
        input: serde_json::Value,
        mode: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_EXECUTE.template(),
            serde_json::json!({
                "name": name,
                "input": input,
                "mode": mode,
            }),
        )
        .await
    }

    pub async fn tool_cache_stats(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_TOOLS_CACHE.template())
            .await
    }

    pub async fn tool_batch_readonly(
        &self,
        calls: Vec<serde_json::Value>,
        max_concurrency: usize,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_BATCH_READONLY.template(),
            serde_json::json!({
                "calls": calls,
                "max_concurrency": max_concurrency,
            }),
        )
        .await
    }

    pub async fn tool_mutation_preview(
        &self,
        edits: Vec<serde_json::Value>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_MUTATIONS_PREVIEW.template(),
            serde_json::json!({ "edits": edits }),
        )
        .await
    }

    pub async fn tool_mutation_apply(
        &self,
        edits: Vec<serde_json::Value>,
        expected_hashes: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_MUTATIONS_APPLY.template(),
            serde_json::json!({
                "edits": edits,
                "expected_hashes": expected_hashes,
            }),
        )
        .await
    }

    pub async fn tool_checkpoints(&self) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(surface::gateway_api::paths::API_TOOLS_CHECKPOINTS.template())
            .await
    }

    pub async fn tool_checkpoint_create(
        &self,
        label: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_CHECKPOINTS.template(),
            serde_json::json!({ "label": label }),
        )
        .await
    }

    pub async fn tool_checkpoint_diff(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.get_json(&crate::gateway_client_routes::render_route(
            surface::gateway_api::paths::API_TOOLS_CHECKPOINTS_BY_ID_DIFF,
            &[(url_encode(id)).to_string()],
        ))
        .await
    }

    pub async fn tool_checkpoint_restore(
        &self,
        id: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            &crate::gateway_client_routes::render_route(
                surface::gateway_api::paths::API_TOOLS_CHECKPOINTS_BY_ID_RESTORE,
                &[(url_encode(id)).to_string()],
            ),
            serde_json::json!({}),
        )
        .await
    }

    pub async fn tool_intent_plan(
        &self,
        prompt: &str,
        selected_tools: Vec<String>,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_INTENT_PLAN.template(),
            serde_json::json!({
                "prompt": prompt,
                "selected_tools": selected_tools,
            }),
        )
        .await
    }

    pub async fn tool_context_fanout_plan(
        &self,
        prompt: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        self.post_json(
            surface::gateway_api::paths::API_TOOLS_CONTEXT_FANOUT_PLAN.template(),
            serde_json::json!({ "prompt": prompt }),
        )
        .await
    }

    /// Execute an APP-selected JSON request through Cowd-owned credentials.
    ///
    /// The external panel can select only an in-process Gateway path and
    /// non-reserved metadata. It cannot override the terminal surface,
    /// observer identity, authentication or HTTP framing.
    pub(crate) async fn app_json_request(
        &self,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
        headers: &BTreeMap<String, String>,
    ) -> Result<(u16, serde_json::Value), AppTransportFailure> {
        let method = app_method(method)?;
        validate_app_path(path)?;
        let headers = app_headers(headers)?;
        let mut request = self.authorize(
            self.client
                .request(method, format!("{}{}", self.base_url, path)),
        );
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(app_transport_failure)?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(app_transport_failure)?;
        let body = decode_app_json_or_text(&bytes);
        if !status.is_success() {
            return Err(AppTransportFailure {
                status: Some(status.as_u16()),
                body: Some(body.clone()),
                message: format!("Gateway API returned {status}: {body}"),
            });
        }
        Ok((status.as_u16(), body))
    }

    /// Consume a versioned declarative APP view stream with bounded delivery.
    pub(crate) async fn subscribe_app_view_stream(
        &self,
        stream_request: AppViewStreamRequest,
        mut cancel: watch::Receiver<bool>,
        tx: CowdEventSender,
    ) -> Result<(), AppTransportFailure> {
        let AppViewStreamRequest {
            app_id,
            view_id,
            request: protocol_request,
            session_id,
            authority_generation,
        } = stream_request;
        validate_app_route_identifier(&app_id, 128)?;
        validate_app_route_identifier(&view_id, 256)?;
        protocol_request
            .validate()
            .map_err(|error| AppTransportFailure {
                status: None,
                body: None,
                message: format!("APP stream request is invalid: {error}"),
            })?;
        if protocol_request.view_id != view_id {
            return Err(AppTransportFailure {
                status: None,
                body: None,
                message: "APP stream request view does not match its route".to_owned(),
            });
        }
        let path = app_view_stream_path(&app_id, &view_id);
        let request = self.authorize(
            self.sse_client
                .post(format!("{}{}", self.base_url, path))
                .json(&protocol_request)
                .header("Accept", "text/event-stream"),
        );
        let response = request.send().await.map_err(app_transport_failure)?;
        let status = response.status();
        if !status.is_success() {
            let bytes = response.bytes().await.map_err(app_transport_failure)?;
            let body = decode_app_json_or_text(&bytes);
            return Err(AppTransportFailure {
                status: Some(status.as_u16()),
                body: Some(body.clone()),
                message: format!("Gateway SSE returned {status}: {body}"),
            });
        }

        let mut buffer = Vec::new();
        let mut stream = response.bytes_stream();
        loop {
            tokio::select! {
                changed = cancel.changed() => {
                    if changed.is_err() || *cancel.borrow() {
                        return Ok(());
                    }
                }
                chunk = stream.next() => {
                    let Some(chunk) = chunk else {
                        tx.send_wait(session_scoped_event(
                            &session_id,
                            authority_generation,
                            CowdEvent::AppSurface {
                                event: AppSurfaceEvent::StreamDisconnected {
                                    app_id,
                                    view_id,
                                    error: "Gateway closed the APP view stream".to_string(),
                                },
                            },
                        )).await.map_err(|_| AppTransportFailure {
                            status: None,
                            body: None,
                            message: "TUI event receiver stopped while APP SSE ended".to_string(),
                        })?;
                        return Ok(());
                    };
                    let chunk = chunk.map_err(app_transport_failure)?;
                    buffer.extend_from_slice(&chunk);
                    while let Some(frame) = take_gateway_sse_frame(&mut buffer).map_err(app_transport_failure)? {
                        if u64::try_from(frame.len()).unwrap_or(u64::MAX) > MAX_STREAM_FRAME_BYTES {
                            return Err(AppTransportFailure {
                                status: None,
                                body: None,
                                message: "APP stream frame exceeded the protocol byte limit".to_string(),
                            });
                        }
                        let Some(data) = gateway_sse_frame_data(&frame) else {
                            continue;
                        };
                        let parsed = serde_json::from_str::<AppStreamFrameV1>(&data)
                            .map_err(|error| AppTransportFailure {
                                status: None,
                                body: None,
                                message: format!("APP stream frame is invalid: {error}"),
                            })?;
                        parsed.validate().map_err(|error| AppTransportFailure {
                            status: None,
                            body: None,
                            message: format!("APP stream frame is invalid: {error}"),
                        })?;
                        tx.send_wait(session_scoped_event(
                            &session_id,
                            authority_generation,
                            CowdEvent::AppSurface {
                                event: AppSurfaceEvent::StreamFrame {
                                    app_id: app_id.clone(),
                                    view_id: view_id.clone(),
                                    frame: parsed,
                                },
                            },
                        )).await.map_err(|_| AppTransportFailure {
                            status: None,
                            body: None,
                            message: "TUI event receiver stopped while APP SSE was active".to_string(),
                        })?;
                    }
                    if u64::try_from(buffer.len()).unwrap_or(u64::MAX) > MAX_STREAM_FRAME_BYTES {
                        return Err(AppTransportFailure {
                            status: None,
                            body: None,
                            message: "APP stream frame exceeded the protocol byte limit".to_string(),
                        });
                    }
                }
            }
        }
    }

    pub(super) async fn get_json(&self, path: &str) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.get(url));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    pub(super) async fn post_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.post(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    pub(super) async fn put_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.put(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    pub(super) async fn patch_json(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.patch(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response.json().await.map_err(GatewayApiError::Http)
    }

    pub(super) async fn delete_json(
        &self,
        path: &str,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.delete(url));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        let body = response.text().await.map_err(GatewayApiError::Http)?;
        if body.trim().is_empty() {
            Ok(serde_json::json!({ "ok": true }))
        } else {
            serde_json::from_str(&body).map_err(|error| GatewayApiError::Url(error.to_string()))
        }
    }

    pub(super) async fn delete_json_with_body(
        &self,
        path: &str,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.delete(url).json(&body));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        let body = response.text().await.map_err(GatewayApiError::Http)?;
        if body.trim().is_empty() {
            Ok(serde_json::json!({ "ok": true }))
        } else {
            serde_json::from_str(&body).map_err(|error| GatewayApiError::Url(error.to_string()))
        }
    }

    pub(super) async fn get_bytes(&self, path: &str) -> Result<Vec<u8>, GatewayApiError> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.get(url));
        let response = request.send().await.map_err(GatewayApiError::Http)?;
        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(gateway_status_error(status, body));
        }
        response
            .bytes()
            .await
            .map(|bytes| bytes.to_vec())
            .map_err(GatewayApiError::Http)
    }
}
