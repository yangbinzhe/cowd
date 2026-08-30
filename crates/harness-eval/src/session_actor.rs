use reqwest::blocking::{Client, RequestBuilder};
use serde_json::{json, Value};

/// Evaluation-side owner for one real Gateway session writer lifecycle.
///
/// This is deliberately an HTTP client, not a Runtime shortcut. Evaluations
/// therefore exercise the same attachment, lease and mutation admission
/// contract as every other Surface.
pub(crate) struct SessionActor<'a> {
    client: &'a Client,
    base_url: String,
    surface_id: String,
    observer_id: String,
    session_id: String,
    active: bool,
    trace: Vec<Value>,
}

impl<'a> SessionActor<'a> {
    pub(crate) fn create(
        client: &'a Client,
        base_url: &str,
        model: Option<&str>,
        surface_id: &str,
    ) -> Result<Self, String> {
        let base_url = base_url.trim_end_matches('/').to_string();
        let observer_id = format!("{surface_id}:{}", uuid::Uuid::new_v4());
        let body = model
            .filter(|value| !value.trim().is_empty())
            .map_or_else(|| json!({}), |model| json!({"model": model}));
        let session = send_json(
            client
                .post(format!("{base_url}/api/sessions"))
                .header("x-cowd-surface-id", surface_id)
                .header("x-cowd-requested-capabilities", "mission.observe")
                .json(&body),
        )
        .map_err(|error| format!("create_session:{error}"))?;
        let session_id = session
            .get("id")
            .or_else(|| session.get("session_id"))
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| format!("create_session:missing_session_id:{session}"))?;
        let mut actor = Self {
            client,
            base_url,
            surface_id: surface_id.to_string(),
            observer_id,
            session_id,
            active: false,
            trace: vec![trace_entry("POST", "/api/sessions", body, Ok(&session))],
        };
        actor.attach_and_acquire()?;
        Ok(actor)
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn post_mutation(&mut self, path: &str, body: Value) -> Result<Value, String> {
        let response = send_json(
            self.writer_request(self.client.post(self.url(path)))
                .json(&body),
        );
        self.trace.push(trace_entry(
            "POST",
            path,
            body,
            response.as_ref().map_err(String::as_str),
        ));
        response
    }

    /// Issue a maintenance command for this actor's own session execution.
    ///
    /// The capability is requested only for failure cleanup; normal scenario
    /// traffic keeps the narrower writer principal.
    pub(crate) fn post_control_mutation(
        &mut self,
        path: &str,
        body: Value,
    ) -> Result<Value, String> {
        let response = send_json(
            self.request_with_capabilities(
                self.client.post(self.url(path)),
                "runtime.maintenance.manage",
            )
            .json(&body),
        );
        self.trace.push(trace_entry(
            "POST",
            path,
            body,
            response.as_ref().map_err(String::as_str),
        ));
        response
    }

    pub(crate) fn drain_trace(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.trace)
    }

    pub(crate) fn finish(&mut self) -> Result<(), String> {
        if !self.active {
            return Ok(());
        }
        let release_body = json!({"session_id": self.session_id});
        let release = send_json(
            self.writer_request(
                self.client
                    .post(self.url("/api/runtime/session-leases/release")),
            )
            .json(&release_body),
        );
        self.trace.push(trace_entry(
            "POST",
            "/api/runtime/session-leases/release",
            release_body,
            release.as_ref().map_err(String::as_str),
        ));

        let detach_path = format!("/api/sessions/{}/detach", self.session_id);
        let detach_body = json!({"surface": self.surface_id});
        let detach = send_json(
            self.writer_request(self.client.post(self.url(&detach_path)))
                .json(&detach_body),
        );
        self.trace.push(trace_entry(
            "POST",
            &detach_path,
            detach_body,
            detach.as_ref().map_err(String::as_str),
        ));
        self.active = false;

        match (release, detach) {
            (Ok(_), Ok(_)) => Ok(()),
            (release, detach) => Err(format!(
                "session_actor_cleanup_failed:release={};detach={}",
                result_summary(&release),
                result_summary(&detach)
            )),
        }
    }

    fn attach_and_acquire(&mut self) -> Result<(), String> {
        let attach_path = format!("/api/sessions/{}/attach", self.session_id);
        let attach_body = json!({"surface": self.surface_id, "role": "writer"});
        let attach = send_json(
            self.writer_request(self.client.post(self.url(&attach_path)))
                .json(&attach_body),
        );
        self.trace.push(trace_entry(
            "POST",
            &attach_path,
            attach_body,
            attach.as_ref().map_err(String::as_str),
        ));
        attach.map_err(|error| format!("attach_writer:{error}"))?;
        // Attachment success establishes cleanup ownership even when lease
        // acquisition fails; Drop must still detach the partially initialized
        // actor.
        self.active = true;

        let acquire_body = json!({"session_id": self.session_id, "mode": "collaborative"});
        let acquire = send_json(
            self.writer_request(
                self.client
                    .post(self.url("/api/runtime/session-leases/acquire")),
            )
            .json(&acquire_body),
        );
        self.trace.push(trace_entry(
            "POST",
            "/api/runtime/session-leases/acquire",
            acquire_body,
            acquire.as_ref().map_err(String::as_str),
        ));
        acquire.map_err(|error| format!("acquire_writer_lease:{error}"))?;
        Ok(())
    }

    fn writer_request(&self, request: RequestBuilder) -> RequestBuilder {
        self.request_with_capabilities(request, "mission.observe")
    }

    fn request_with_capabilities(
        &self,
        request: RequestBuilder,
        capabilities: &str,
    ) -> RequestBuilder {
        request
            .header("x-cowd-surface-id", &self.surface_id)
            .header("x-cowd-observer-id", &self.observer_id)
            .header("x-cowd-requested-capabilities", capabilities)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl Drop for SessionActor<'_> {
    fn drop(&mut self) {
        if let Err(error) = self.finish() {
            eprintln!("{error}");
        }
    }
}

fn send_json(request: RequestBuilder) -> Result<Value, String> {
    let response = request.send().map_err(|error| error.to_string())?;
    let status = response.status();
    let text = response.text().map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|error| format!("{error}: {text}"))
}

fn trace_entry(method: &str, path: &str, body: Value, response: Result<&Value, &str>) -> Value {
    match response {
        Ok(response) => json!({
            "method": method,
            "path": path,
            "request": body,
            "status": "ok",
            "response": response,
        }),
        Err(error) => json!({
            "method": method,
            "path": path,
            "request": body,
            "status": "failed",
            "error": error,
        }),
    }
}

fn result_summary(result: &Result<Value, String>) -> String {
    match result {
        Ok(_) => "ok".to_string(),
        Err(error) => error.clone(),
    }
}
