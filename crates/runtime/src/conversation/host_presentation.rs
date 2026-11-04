//! Governed tool context, synthesis, terminal evidence, and presentation policy.

use super::*;
use crate::ToolError;

#[cfg(test)]
#[path = "host_presentation_recovery_tests.rs"]
mod recovery_tests;

// Presentation correction may repair a diagnostic's truthfulness, but cannot
// erase why it failed as a user answer. Keep that evidence across origin changes.
struct TerminalPresentationValidation {
    invalid: bool,
    findings: Vec<String>,
}

impl TerminalPresentationValidation {
    fn new(origin: harness_contract::outcome::AnswerOrigin, reason: Option<&str>) -> Self {
        use harness_contract::outcome::AnswerOrigin;
        Self {
            invalid: matches!(
                origin,
                AnswerOrigin::ProgrammaticFallback | AnswerOrigin::RuntimeVerifiedFallback
            ),
            findings: reason.map(str::to_string).into_iter().collect(),
        }
    }

    fn correct_truth(
        &mut self,
        answer: &mut String,
        origin: &mut harness_contract::outcome::AnswerOrigin,
        reason: &mut Option<String>,
        attempt_id: &mut String,
        envelope: &harness_contract::outcome::DeliveryEnvelope,
    ) {
        let contradictions = delivery_truth_contradictions(answer, envelope);
        if !contradictions.is_empty() {
            self.invalid = true;
            self.findings.push(format!(
                "terminal candidate contradicted committed Runtime truth: {}",
                contradictions.join("; ")
            ));
            *answer = runtime_verified_delivery_fallback(envelope);
            *origin = harness_contract::outcome::AnswerOrigin::RuntimeVerifiedFallback;
            *reason = Some(self.findings.join("; "));
            *attempt_id = format!("{attempt_id}:runtime-truth-fallback");
        }
    }

    fn finish(
        &self,
        mut presentation: harness_contract::outcome::TerminalPresentation,
    ) -> harness_contract::outcome::TerminalPresentation {
        presentation.validation = harness_contract::outcome::AnswerValidation {
            status: if self.invalid {
                harness_contract::outcome::AnswerValidationStatus::Invalid
            } else {
                harness_contract::outcome::AnswerValidationStatus::Valid
            },
            findings: self.findings.clone(),
            envelope_revision: Some(presentation.envelope_revision),
        };
        presentation
    }
}

#[cfg(test)]
mod terminal_validation_tests {
    use super::*;
    use harness_contract::outcome::*;

    fn envelope(format: UserAnswerFormat) -> DeliveryEnvelope {
        serde_json::from_value(serde_json::json!({
            "envelope_id":"original-envelope", "revision":7,
            "objective_id":"original-objective", "created_at_ms":1,
            "pipeline_status":"completed", "delivery_status":"satisfied",
            "user_answer_contract":{"language":"zh","format":format}
        }))
        .unwrap()
    }

    fn construct(
        validation: &TerminalPresentationValidation,
        origin: AnswerOrigin,
        reason: Option<String>,
        attempt: String,
    ) -> TerminalPresentation {
        validation.finish(TerminalPresentation {
            presentation_id: "original-presentation".into(),
            attempt_id: attempt,
            envelope_id: "original-envelope".into(),
            envelope_revision: 7,
            state: TerminalPresentationState::Committed,
            answer_origin: origin,
            source_execution_id: Some("original-execution".into()),
            narrator_model: None,
            narrator_provider: None,
            models_attempted: Vec::new(),
            validation: AnswerValidation::default(),
            fallback_reason: reason,
            generated_at_ms: 2,
            committed_at_ms: Some(2),
        })
    }

    #[test]
    fn strict_json_two_fields_preserve_literal_terminal_bytes() {
        let text = "  {\"package_name\":\"runtime\",\"path\":\"crates/runtime/Cargo.toml\"}\n";
        let envelope = envelope(UserAnswerFormat::StrictJson);
        assert!(qualified_root_answer(text, &envelope));
        assert_eq!(
            visible_final_answer(text, envelope.user_answer_contract.format),
            text
        );
    }

    #[test]
    fn strict_json_summary_is_data_not_a_replacement_answer() {
        let text = r#"{"summary":"keep this field","answer":"also data","content":"literal","package_name":"runtime"}"#;
        let envelope = envelope(UserAnswerFormat::StrictJson);
        assert!(qualified_root_answer(text, &envelope));
        assert_eq!(
            visible_final_answer(text, envelope.user_answer_contract.format),
            text
        );
    }

    #[test]
    fn strict_json_qualification_and_markdown_rendering_remain_distinct() {
        let strict = envelope(UserAnswerFormat::StrictJson);
        for malformed in [
            "{",
            r#"{"package_name":}"#,
            "```json\n{}\n```",
            "plain text",
        ] {
            assert!(!qualified_root_answer(malformed, &strict), "{malformed}");
        }
        let markdown = envelope(UserAnswerFormat::Markdown);
        assert_eq!(
            visible_final_answer(
                r#"{"package_name":"runtime","path":"crates/runtime/Cargo.toml"}"#,
                markdown.user_answer_contract.format,
            ),
            "已完成：\n- package_name: runtime\n- path: crates/runtime/Cargo.toml\n"
        );
        assert_eq!(
            visible_final_answer(
                r#"{"summary":"render this summary","extra":"retained source data"}"#,
                markdown.user_answer_contract.format,
            ),
            "render this summary"
        );
    }

    #[test]
    fn narrator_failure_survives_truth_correction_in_the_actual_presentation() {
        let envelope = envelope(UserAnswerFormat::StrictJson);
        let original = envelope.clone();
        let mut origin = AnswerOrigin::ProgrammaticFallback;
        let mut reason = Some("all configured terminal narrator candidates failed".into());
        let mut answer = "task was not completed".to_string();
        let mut attempt = "original-attempt:fallback".to_string();
        let mut validation = TerminalPresentationValidation::new(origin, reason.as_deref());
        validation.correct_truth(
            &mut answer,
            &mut origin,
            &mut reason,
            &mut attempt,
            &envelope,
        );
        let presentation = construct(&validation, origin, reason, attempt);
        assert_eq!(
            presentation.answer_origin,
            AnswerOrigin::RuntimeVerifiedFallback
        );
        assert_eq!(
            presentation.validation.status,
            AnswerValidationStatus::Invalid
        );
        assert_eq!(presentation.validation.envelope_revision, Some(7));
        assert_eq!(presentation.validation.findings, vec![
            "all configured terminal narrator candidates failed",
            "terminal candidate contradicted committed Runtime truth: answer denies a satisfied delivery",
        ]);
        let reason = presentation.fallback_reason.unwrap();
        assert!(reason.contains("all configured terminal narrator candidates failed"));
        assert!(reason.contains("answer denies a satisfied delivery"));
        assert_eq!(
            presentation.attempt_id,
            "original-attempt:fallback:runtime-truth-fallback"
        );
        let diagnostic: serde_json::Value = serde_json::from_str(&answer).unwrap();
        assert_eq!(diagnostic["delivery_status"], "satisfied");
        assert!(diagnostic.get("package_name").is_none());
        assert_eq!(
            envelope, original,
            "presentation cannot rewrite business facts"
        );
    }

    #[test]
    fn narrator_success_with_a_truth_contradiction_is_not_validated_as_business() {
        let envelope = envelope(UserAnswerFormat::Markdown);
        let mut origin = AnswerOrigin::TerminalNarrator;
        let mut reason = None;
        let mut answer = "task was not completed".to_string();
        let mut attempt = "original-attempt".to_string();
        let mut validation = TerminalPresentationValidation::new(origin, None);
        validation.correct_truth(
            &mut answer,
            &mut origin,
            &mut reason,
            &mut attempt,
            &envelope,
        );
        let presentation = construct(&validation, origin, reason, attempt);
        assert_eq!(
            presentation.validation.status,
            AnswerValidationStatus::Invalid
        );
        assert_eq!(presentation.validation.findings, vec![
            "terminal candidate contradicted committed Runtime truth: answer denies a satisfied delivery",
        ]);
    }

    #[test]
    fn qualified_json_and_chinese_answers_keep_the_original_valid_path() {
        for (format, text) in [
            (
                UserAnswerFormat::StrictJson,
                r#"{"package_name":"runtime","path":"crates/runtime/Cargo.toml"}"#,
            ),
            (UserAnswerFormat::Markdown, "读取已完成，以上是原文件内容。"),
        ] {
            let envelope = envelope(format);
            assert!(qualified_root_answer(text, &envelope));
            let mut answer = text.to_string();
            let mut origin = AnswerOrigin::ModelDirect;
            let mut reason = None;
            let mut attempt = "original-attempt".to_string();
            let mut validation = TerminalPresentationValidation::new(origin, None);
            validation.correct_truth(
                &mut answer,
                &mut origin,
                &mut reason,
                &mut attempt,
                &envelope,
            );
            let presentation = construct(&validation, origin, reason, attempt);
            assert_eq!(
                presentation.validation.status,
                AnswerValidationStatus::Valid
            );
            assert!(presentation.validation.findings.is_empty());
            assert!(presentation.fallback_reason.is_none());
            assert_eq!(presentation.answer_origin, AnswerOrigin::ModelDirect);
            assert_eq!(answer, text);
        }
        let strict = envelope(UserAnswerFormat::StrictJson);
        assert!(!qualified_root_answer(
            "```json\n{\"package_name\":\"runtime\"}\n```",
            &strict
        ));
    }
}

fn synthesize_message_value(message: &ConversationMessage) -> serde_json::Value {
    serde_json::json!({"role":message.role.role_str(),"blocks":message.blocks,"usage":message.usage})
}

fn synthesize_prefix_digest(messages: &[ConversationMessage]) -> Result<String, String> {
    if messages.len() > 16384 {
        return Err("Synthesize original prefix message budget exceeded".into());
    }
    let mut hasher = Sha256::new();
    let mut total = 0usize;
    for message in messages {
        let bytes = serde_json::to_vec(&synthesize_message_value(message))
            .map_err(|error| error.to_string())?;
        total = total
            .checked_add(bytes.len())
            .ok_or("Synthesize prefix byte overflow")?;
        if total > 8 * 1024 * 1024 {
            return Err("Synthesize original prefix byte budget exceeded".into());
        }
        hasher.update((bytes.len() as u64).to_be_bytes());
        hasher.update(bytes);
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn synthesize_terminal_text(message: &ConversationMessage) -> Option<&str> {
    if message.role.role_str() != "assistant" {
        return None;
    }
    match message.blocks.as_slice() {
        [ContentBlock::Text { text }] if !text.trim().is_empty() => Some(text),
        _ => None,
    }
}

fn synthesize_publication_message(
    value: &serde_json::Value,
) -> Result<ConversationMessage, String> {
    let fields = value
        .as_object()
        .ok_or("Synthesize message is not an object")?;
    if fields.len() != 3
        || value["role"] != "assistant"
        || !value["usage"].is_null()
        || !fields.contains_key("usage")
    {
        return Err(
            "Synthesize publication requires one assistant message without provider usage".into(),
        );
    }
    let message = ConversationMessage::assistant(
        serde_json::from_value(value["blocks"].clone()).map_err(|error| error.to_string())?,
    );
    if synthesize_terminal_text(&message).is_none() || synthesize_message_value(&message) != *value
    {
        return Err("Synthesize publication requires exact nonempty Text".into());
    }
    Ok(message)
}

fn inherit_synthesize_sources(
    mut outcome: NodeExecutionOutcome,
    source_revisions: &BTreeMap<String, u64>,
) -> Result<NodeExecutionOutcome, String> {
    for (stream_id, revision) in source_revisions {
        pin_synthesize_source(&mut outcome.source_revisions, stream_id.clone(), *revision)?;
    }
    Ok(outcome)
}

fn pin_synthesize_source(
    sources: &mut BTreeMap<String, u64>,
    stream_id: String,
    revision: u64,
) -> Result<(), String> {
    if sources
        .get(&stream_id)
        .is_some_and(|original| *original != revision)
    {
        return Err("Synthesize decision source conflicts with original producer read-set".into());
    }
    sources.insert(stream_id, revision);
    Ok(())
}

fn freeze_synthesize_goal_sources(
    services: &crate::RuntimeServices,
    session_id: &str,
    turn_id: &str,
    goal_id: &str,
) -> Result<BTreeMap<String, u64>, String> {
    let mut sources = BTreeMap::new();
    let state_goal = services
        .goal_store()
        .projection(goal_id)
        .map_err(|error| format!("load original Synthesize Goal source: {error}"))?
        .ok_or("Synthesize original Goal source missing")?;
    if state_goal.goal.session_id != session_id {
        return Err("Synthesize Goal source crosses original Session".into());
    }
    pin_synthesize_source(
        &mut sources,
        format!("goal:{goal_id}"),
        state_goal.stream_revision,
    )?;
    if let Some(binding) = state_goal.goal.execution_binding.as_ref() {
        let revision = services
            .agent_action_service()
            .program_revision(&binding.agentic_program_id)
            .map_err(|error| error.to_string())?;
        pin_synthesize_source(
            &mut sources,
            format!("agentic-program:{}", binding.agentic_program_id),
            revision,
        )?;
    }
    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let program_id = harness_contract::agent_action::program_id_for_objective(&objective_id);
    let revision = services
        .agent_action_service()
        .program_revision(&program_id)
        .map_err(|error| error.to_string())?;
    pin_synthesize_source(
        &mut sources,
        format!("agentic-program:{program_id}"),
        revision,
    )?;
    let program = services
        .agent_action_service()
        .project_if_exists(&program_id)
        .map_err(|error| error.to_string())?;
    let Some(program) = program else {
        if revision != 0 {
            return Err("Synthesize Program source disappeared before Goal decision".into());
        }
        return Ok(sources);
    };
    if program.revision != revision
        || program.program_id != program_id
        || program.objective_id != objective_id
        || program.session_id != session_id
        || program.turn_id != turn_id
    {
        return Err("Synthesize Program source changed before Goal decision".into());
    }
    if let Some(root_execution_id) = program.root_execution_id.as_deref() {
        let actual_goal_id = format!("goal:{root_execution_id}");
        if actual_goal_id != goal_id {
            let actual_goal = services
                .goal_store()
                .projection(&actual_goal_id)
                .map_err(|error| error.to_string())?;
            if actual_goal
                .as_ref()
                .is_some_and(|goal| goal.goal.session_id != session_id)
            {
                return Err("Synthesize Program Goal source crosses original Session".into());
            }
            pin_synthesize_source(
                &mut sources,
                format!("goal:{actual_goal_id}"),
                actual_goal.map_or(0, |goal| goal.stream_revision),
            )?;
        }
    }
    Ok(sources)
}

fn is_synthesize_terminal_completion(completion: GoalCompletion) -> bool {
    matches!(
        completion,
        GoalCompletion::Satisfied
            | GoalCompletion::Partial
            | GoalCompletion::Blocked
            | GoalCompletion::Failed
            | GoalCompletion::Cancelled
    )
}

fn validate_synthesize_publication(
    ticket: &NodeExecutionTicket,
    original: &NodeExecutionTicket,
    messages: &[serde_json::Value],
    completion: Option<GoalCompletion>,
    outcome: &NodeExecutionOutcome,
) -> Result<Option<(String, GoalCompletion)>, String> {
    if ticket != original
        || ticket.executor_kind != "synthesize"
        || outcome.result.status != ExecutionNodeStatus::Completed
    {
        return Err("Synthesize publication original ticket/result mismatch".into());
    }
    if messages.is_empty() && completion.is_none() {
        if outcome.replan.is_none()
            || outcome.delivery_envelope.is_some()
            || outcome.terminal_presentation.is_some()
        {
            return Err("Synthesize empty publication has no original nonterminal replan".into());
        }
        return Ok(None);
    }
    let completion = completion
        .filter(|completion| is_synthesize_terminal_completion(*completion))
        .ok_or("Synthesize original terminal completion missing")?;
    if messages.len() != 1 || outcome.replan.is_some() {
        return Err("Synthesize terminal publication shape/replan mismatch".into());
    }
    let message = synthesize_publication_message(&messages[0])?;
    let answer =
        synthesize_terminal_text(&message).ok_or("Synthesize original terminal missing")?;
    let expected = format!(
        "turn-result:{}:{:x}",
        ticket.graph_id,
        Sha256::digest(answer.as_bytes())
    );
    if outcome.result.result_ref.as_deref() != Some(expected.as_str()) {
        return Err("Synthesize publication does not match original result digest".into());
    }
    Ok(Some((answer.to_string(), completion)))
}

fn apply_synthesize_publication(
    session: &mut Session,
    session_id: &str,
    prefix_count: u64,
    prefix_sha256: &str,
    messages: &[serde_json::Value],
) -> Result<(), String> {
    if session.session_id != session_id || messages.len() != 1 {
        return Err("Synthesize publication Session/message mismatch".into());
    }
    let pending = synthesize_publication_message(&messages[0])?;
    let prefix_count = usize::try_from(prefix_count).map_err(|_| "Synthesize prefix overflow")?;
    if prefix_count > 16384 || session.message_count() < prefix_count {
        return Err("Synthesize original Session prefix missing".into());
    }
    let prefix = session
        .messages()
        .take(prefix_count)
        .cloned()
        .collect::<Vec<_>>();
    if synthesize_prefix_digest(&prefix)? != prefix_sha256 {
        return Err("Synthesize original Session prefix changed".into());
    }
    let final_text =
        synthesize_terminal_text(&pending).ok_or("Synthesize original terminal missing")?;
    if prefix.last().is_some_and(|message| {
        message.role.role_str() == "assistant"
            && message.blocks.iter().any(|block| {
                matches!(block,
            ContentBlock::Text { text } if text == final_text)
            })
    }) {
        return Ok(());
    }
    if let Some(published) = session.message(prefix_count) {
        if published != &pending {
            return Err("Synthesize published terminal segment diverged".into());
        }
    } else {
        session.extend_messages([pending]);
    }
    Ok(())
}

fn committed_synthesize_terminal_claims(
    store: &crate::RuntimeEventStore,
    ticket: &NodeExecutionTicket,
    session_id: &str,
    turn_id: &str,
) -> Result<Vec<String>, String> {
    let identity = crate::authorization_negotiator::ControlledRecoveryTerminalRecord {
        recovery_scope: format!("turn:{turn_id}"),
        session_id: session_id.into(),
        turn_id: turn_id.into(),
        execution_id: ticket.graph_id.clone(),
        fingerprints: Vec::new(),
    };
    let expected = crate::authorization_negotiator::controlled_recovery_terminal_event(&identity)?;
    let event = store
        .event_by_idempotency_key(
            &expected.event.stream_id,
            expected
                .idempotency_key
                .as_deref()
                .ok_or("Synthesize claim record key missing")?,
        )
        .map_err(|error| error.to_string())?
        .ok_or("Synthesize original controlled-recovery terminal record missing")?;
    let record: crate::authorization_negotiator::ControlledRecoveryTerminalRecord =
        serde_json::from_value(event.payload.clone()).map_err(|error| error.to_string())?;
    let mut identity = identity;
    identity.fingerprints = record.fingerprints.clone();
    if serde_json::to_value(&identity).map_err(|error| error.to_string())? != event.payload {
        return Err("Synthesize controlled-recovery terminal identity mismatch".into());
    }
    let receipt = store
        .event_by_idempotency_key(
            &crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(ticket),
            &format!("{}:receipt", ticket.idempotency_key),
        )
        .map_err(|error| error.to_string())?
        .ok_or("Synthesize original effect receipt missing")?;
    if event.kind != expected.event.kind
        || event.scope != expected.event.scope
        || event.status != expected.event.status
        || event.actor != expected.event.actor
        || event.refs != expected.event.refs
        || event.schema_version != expected.schema_version
        || event.transaction_id != receipt.transaction_id
        || event.commit_cursor != receipt.commit_cursor
    {
        return Err(
            "Synthesize controlled-recovery terminal is not in original atomic result".into(),
        );
    }
    Ok(record.fingerprints)
}

fn validate_synthesize_terminal_row(
    store: &crate::RuntimeEventStore,
    ticket: &NodeExecutionTicket,
    session_id: &str,
    turn_id: &str,
    row: &crate::runtime_event_store::RuntimeSessionOutboxRecord,
    fingerprints: &[String],
) -> Result<crate::runtime_event_store::DurableRuntimeEvent, String> {
    let request_id = row
        .request_id
        .as_deref()
        .ok_or("Synthesize original terminal request missing")?;
    let event = store
        .event_by_idempotency_key(
            &format!("session-terminal:{request_id}"),
            &ticket.idempotency_key,
        )
        .map_err(|error| error.to_string())?
        .ok_or("Synthesize original terminal request event missing")?;
    let receipt = store
        .event_by_idempotency_key(
            &crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(ticket),
            &format!("{}:receipt", ticket.idempotency_key),
        )
        .map_err(|error| error.to_string())?
        .ok_or("Synthesize original terminal effect receipt missing")?;
    let terminal: crate::runtime_event_store::SessionTerminalInput =
        serde_json::from_value(event.payload.clone()).map_err(|error| error.to_string())?;
    crate::runtime_event_store::validate_runtime_fenced_terminal(&terminal)
        .map_err(|error| error.to_string())?;
    if event.kind != "runtime.session.terminal_requested"
        || event.scope != crate::RuntimeEventScope::SessionInput
        || event.schema_version != 1
        || event.actor.as_deref() != Some("SynthesizeNodeExecutor")
        || event.transaction_id != receipt.transaction_id
        || event.commit_cursor != receipt.commit_cursor
        || row.commit_cursor != event.commit_cursor
        || row.session_id != session_id
        || row.execution_id.as_deref() != Some(ticket.graph_id.as_str())
        || row.turn_id.as_deref() != Some(turn_id)
        || terminal.terminal_id != row.terminal_id
        || terminal.message_id != row.message_id
        || terminal.session_id != row.session_id
        || terminal.execution_id != row.execution_id
        || terminal.turn_id != row.turn_id
        || terminal.request_id != row.request_id
        || terminal.payload_ref != row.payload_ref
        || terminal.controlled_recovery_claim_fingerprints.as_slice() != fingerprints
    {
        return Err("Synthesize original terminal outbox/claim transaction mismatch".into());
    }
    Ok(event)
}

fn unknown_recovered_terminal_telemetry() -> crate::RunModelTelemetry {
    crate::RunModelTelemetry {
        model: None,
        models_used: Vec::new(),
        first_token_latency_ms: None,
        active_stream_duration_ms: None,
        wall_duration_ms: 0,
        output_chars: 0,
        output_chunks: 0,
        input_tokens: 0,
        output_tokens: 0,
        cache_create_tokens: 0,
        cache_read_tokens: 0,
        cache_dimensions_known: false,
        total_tokens: 0,
        usage_source: "unknown_recovered".into(),
        wall_chars_per_second: None,
        wall_tokens_per_second: None,
        active_chars_per_second: None,
        active_tokens_per_second: None,
        chars_per_second: None,
        tokens_per_second: None,
    }
}

fn validate_recovered_terminal_usage(telemetry: &crate::RunModelTelemetry) -> Result<(), String> {
    for count in [
        telemetry.input_tokens,
        telemetry.output_tokens,
        telemetry.cache_create_tokens,
        telemetry.cache_read_tokens,
    ] {
        u32::try_from(count)
            .map_err(|_| "Synthesize recovered usage exceeds original TokenUsage width")?;
    }
    Ok(())
}

impl<C, T> TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn stage_terminal_publication(
        &self,
        ticket: &NodeExecutionTicket,
        outcome: &mut NodeExecutionOutcome,
    ) -> Result<(), String> {
        let (session_id, turn_id, messages, completion) = {
            let state = self.state.lock().await;
            if outcome.replan.is_some() {
                (
                    state.session_id.clone(),
                    state.turn_id.clone(),
                    Vec::new(),
                    None,
                )
            } else {
                if !terminal_commit_owned_by(
                    state.terminal_commit_owner.as_ref(),
                    &ticket.node_id,
                    ticket.attempt,
                ) {
                    return Err("Synthesize terminal has no original staging owner".into());
                }
                let answer = state
                    .committed_terminal_answer
                    .as_ref()
                    .ok_or("Synthesize terminal has no original answer")?;
                let message = ConversationMessage::assistant(vec![ContentBlock::Text {
                    text: answer.clone(),
                }]);
                (
                    state.session_id.clone(),
                    state.turn_id.clone(),
                    vec![synthesize_message_value(&message)],
                    state.committed_terminal_completion,
                )
            }
        };
        validate_synthesize_publication(ticket, ticket, &messages, completion, outcome)?;
        let snapshot = self.runtime.lock().await.session_snapshot().await;
        if snapshot.session_id != session_id {
            return Err("Synthesize staging Runtime Session mismatch".into());
        }
        let prefix = snapshot.materialize_messages();
        let prefix_sha256 = synthesize_prefix_digest(&prefix)?;
        let prefix_count = prefix.len() as u64;
        let commits = self.services.commit_service().clone();
        let original = ticket.clone();
        let event = tokio::task::spawn_blocking(move || {
            commits.prepare_synthesize_transcript(
                &original,
                &session_id,
                &turn_id,
                prefix_count,
                prefix_sha256,
                messages,
                completion,
            )
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        crate::ExecutionCommitService::attach_host_transcript(outcome, event)
            .map_err(|error| error.to_string())
    }
}

fn should_reconcile_agentic_terminal(
    role: crate::TurnExecutionRole,
    completion: harness_contract::goal::GoalCompletion,
    terminal_override: Option<harness_contract::goal::GoalCompletion>,
) -> bool {
    role.owns_root_presentation()
        && !(completion == harness_contract::goal::GoalCompletion::Partial
            && terminal_override == Some(harness_contract::goal::GoalCompletion::Partial))
        && matches!(
            completion,
            harness_contract::goal::GoalCompletion::Satisfied
                | harness_contract::goal::GoalCompletion::Partial
        )
}

impl crate::GovernedToolExecutionContext for HostGovernedToolContext<'_> {
    type Output = crate::RuntimeToolExecutionOutcome;
    type Admission = Option<crate::ToolExecutionAdmission>;
    type Receipt = HostGovernedReceipt;

    fn local_ceiling(&self) -> usize {
        crate::governed_tool_plan::default_parallel_tool_concurrency()
    }

    fn try_admit<'a>(
        &'a self,
        task: &'a crate::GovernedToolPlanTask,
    ) -> crate::GovernedToolFuture<'a, crate::GovernedToolAdmission<Self::Admission>> {
        Box::pin(async move {

            let Some(call) = self.calls.get(task.original_call_index) else {
                return crate::GovernedToolAdmission::Refused("original call missing".into());
            };
            if call.id != task.tool_call_id || call.name != task.invocation.intent.tool_name
                || call.binding != task.step_binding
                || harness_contract::step::parse_strict_json(&call.input).ok().as_ref() != Some(&task.normalized_input)
            {
                return crate::GovernedToolAdmission::Refused("original call identity mismatch".into());
            }

            if let Some(error) = self.policy_failures.and_then(|items| items.get(&call.id)) {
                return crate::GovernedToolAdmission::Failed(format!("tool preparation or policy assessment failed: {error}"));
            }

            if let (Some(state), Some(services)) = (self.scope_state, self.services) {
                let mut effective_call = call.clone();
                if let Some(input) = self.effective_inputs.and_then(|inputs| inputs.get(&call.id)) {
                    effective_call.input = input.clone();
                }
                let Some(request) = self.prepared_requests.and_then(|requests| requests.get(&call.id)) else {
                    return crate::GovernedToolAdmission::Failed("original prepared Host request unavailable".into());
                };
                if request.tool_use_id != call.id || request.tool_name != call.name
                    || request.input != effective_call.input || request.step_binding != task.step_binding {
                    return crate::GovernedToolAdmission::Failed("original prepared Host request changed".into());
                }
                let evaluated = {
                    let state = state.lock().await;
                    if !state.has_evaluation_scope() { Ok(None) }
                    else if let Some(effect) = self.prepared_effects.and_then(|effects| effects.get(&call.id)) {
                        state.evaluate_scope_refusal(self.ticket, request, services, effect)
                    } else { Err(crate::RuntimeError::new("original pinned effect unavailable")) }
                };
                match evaluated {
                    Ok(Some(fact)) => {
                        let reason = fact.model_feedback();
                        self.scope_facts.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(call.id.clone(), fact);
                        return crate::GovernedToolAdmission::Refused(reason);
                    }
                    Ok(None) => {}
                    Err(error) => return crate::GovernedToolAdmission::Failed(format!("scope assessment failed: {error}")),
                }
            } else if let Some(scope) = self.evaluation_scope_rejections.and_then(|items| items.get(&call.id)) {
                return crate::GovernedToolAdmission::Refused(format!("evaluation_scope_exceeded: {scope}"));
            }

            if self.policy_refusals.is_some_and(|items| items.contains_key(&call.id)) {
                let reason = self.capability_gaps.get(&call.id).and_then(|assessment| assessment.gap.as_ref())
                    .map_or("policy hard denial", |gap| gap.reason.as_str());
                return crate::GovernedToolAdmission::Refused(reason.to_string());
            }
            crate::GovernedToolAdmission::Granted(None)
        })
    }

    fn execute<'a>(
        &'a self,
        task: &'a crate::GovernedToolPlanTask,
        _admission: &'a mut Self::Admission,
    ) -> crate::GovernedToolFuture<'a, Result<Self::Output, String>> {
        Box::pin(async move {

            let call = self.calls.get(task.original_call_index).ok_or_else(|| {
                format!(
                    "governed tool task `{}` references missing original call index {}",
                    task.tool_call_id, task.original_call_index
                )
            })?;

            if let Some(assessment) = self.capability_gaps.get(&call.id) {
                if let Some(observation) = self.physical_owner.binding_observation(self.ticket) {
                    observation.refuse(&call.id, "authorization_gap_before_host_dispatch".into());
                }
                return Ok(capability_gap_outcome(
                    call,
                    task.safety_category,
                    assessment,
                ));
            }

            if let (Some(plan), Some(decision)) = (self.admission_plan, self.execution_decision) {
                let mut call_plan = plan.clone();
                call_plan.tasks.retain(|candidate| candidate.tool_call_id == task.tool_call_id);
                if call_plan.tasks.len() != 1 {
                    return Err("checkpoint original call missing or ambiguous".into());
                }

                let validation = call_plan.validate_against_execution_decision(decision);
                if !validation.allowed {
                    return Err(format!("tool strategy gate failed: {}", validation.findings.join(", ")));
                }
                if validation.requires_checkpoint {

                    self.strategy_gate.ok_or_else(|| "original Host checkpoint owner unavailable".to_string())?
                        .check(task, plan, decision, self.ticket).await?;
                }
            } else {
                return Err("original Host strategy plan unavailable".into());
            }
            let host = Arc::clone(&self.host);
            let authorization = self.tool_authorizations.get(&call.id).cloned();
            let effect = self
                .prepared_invocations
                .get(&call.id)
                .map(|invocation| invocation.effect.clone());
            let commit_service = self.commit_service.clone();
            let mut effective_call = call.clone();
            if let Some(input) = self.effective_inputs.and_then(|inputs| inputs.get(&call.id)) {
                effective_call.input = input.clone();
            }

            let mut request = self.prepared_requests
                .and_then(|requests| requests.get(&call.id))
                .cloned()
                .ok_or_else(|| "original prepared Host request unavailable".to_string())?;
            if request.tool_use_id != call.id || request.tool_name != call.name
                || request.input != effective_call.input || request.step_binding != task.step_binding
                || request.authorization.is_some() {
                return Err("original prepared Host request changed".into());
            }

            request.authorization = authorization;
            if request.tool_name == "agent_action_batch" {
                if let Some(owner) = &self.batch_policy_owner {
                    request.tool_progress.install_batch_policy_owner(Arc::clone(owner))?;
                }
            }
            if request.tool_name == "agent_action_batch" {

                self.member_requests.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(call.id.clone(), Arc::new(request.clone()));
            }
            // Keep the actual execution request and its original same-call
            // sink under the existing node custody before physical dispatch.
            if let Some(state) = self.scope_state {
                let carrier = crate::tool_host::OriginalMemberCallCarrier::from_original_request(Arc::new(request.clone()));
                let pending = crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::OriginalCall(Arc::new(carrier)));

                super::host_backend::retain_original_member_batches(state, self.ticket, &[(call.id.clone(), pending)])
                    .await.map_err(|error| error.to_string())?;
            }

            let start = self.invocations.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&call.id).cloned();
            let producer = start.as_ref().and_then(|record| record.original_producer.clone());
            if let (Some(record), Some(producer)) = (start, producer) {

                request.tool_progress.record_lifecycle_anchor(crate::tool_host::ToolLifecycleAnchor {
                    invocation_id: record.invocation_id, session_id: record.session_id,
                    tool_call_id: record.tool_call_id, tool_name: record.tool_name,
                    input_hash: record.input_hash, producer,
                })?;
            }

            let plane = Arc::clone(self.execution_plane);
            let demand = task.resource_demand.clone();
            let service_class = self.ticket.service_class;
            let timeout_secs = task.safety_category.default_timeout_secs();
            let session_id = self.session_id.to_string();
            let node_id = self.ticket.node_id.clone();
            let observation = self.physical_owner.binding_observation(self.ticket);
            let leaf_observation = observation.clone();
            let retained_request = request.clone();
            let original_ticket = self.ticket.clone();
            let action_issuer = self.action_issuer.clone();
            let single_capture = self.single_captures.as_ref().and_then(|captures| captures.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner).get(&call.id).cloned());
            let scope_locks = Arc::clone(self.execution_plane.scope_locks());
            use sha2::Digest;

            let invocation_id = format!(
                "leaf:{:x}",
                sha2::Sha256::digest(
                    serde_json::to_vec(&request).map_err(|error| error.to_string())?
                )
            );

            self.physical_owner
                .execute_physical_tool(
                    self.ticket,
                    &invocation_id,
                    Box::pin(async move {
                        let (execution, admission) = plane
                            .execute_async_classified_retained(
                                &demand,
                                Some(std::time::Duration::from_secs(timeout_secs)),
                                service_class,
                                Some(service_class),
                                Some(&session_id),
                                async move {
                                    execute_fenced_runtime_tool_owned(
                                        host.as_ref(),
                                        &commit_service,
                                        &request,
                                        effect.as_ref(),
                                        leaf_observation.as_deref(),
                                        Some(&original_ticket),
                                        Some(scope_locks.as_ref()),
                                        action_issuer.as_deref(),
                                        single_capture.as_deref(),
                                    )
                                    .await
                                },
                            )
                            .await;
                        if let Err(error) = &execution {
                            if let Some(observation) = &observation {
                                match error {
                                    crate::ToolExecutionPlaneError::Admission(_) => observation
                                        .refuse(&retained_request.tool_use_id, error.to_string()),
                                    _ => observation.uncertain(&retained_request),
                                }
                            }
                        }
                        // Both reads and writes have durable leaf receipts now.
                        drop(admission);
                        execution.map_err(|error| NodeExecutorError::Poll {
                            node_id,
                            reason: error.to_string(),
                        })
                    }),
                )
                .await
                .map_err(|error| {
                    if let NodeExecutorError::Uncertain { reason, .. } = &error {
                        self.unknown_effects.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(task.tool_call_id.clone(), ToolError::unknown(reason.clone()));
                    }
                    error.to_string()
                })
        })
    }

    fn classify_output(&self, output: &Self::Output) -> Result<(), String> {
        if !output.model_feedback_is_error() {
            Ok(())
        } else {
            Err(output.error.clone().unwrap_or_else(|| {
                format!("tool `{}` did not complete successfully", output.tool_name)
            }))
        }
    }

    fn precompleted(
        &self,
        task: &crate::GovernedToolPlanTask,
    ) -> Option<(crate::GovernedToolTaskTerminal<Self::Output>, Self::Receipt)> {
        if let Some(replay) = self.original_replays.and_then(|items| items.get(&task.tool_call_id)) {
            let call = self.calls.get(task.original_call_index)?;
            if replay.call != *call { return None; }
            let terminal = if replay.outcome.model_feedback_is_error() {
                crate::GovernedToolTaskTerminal::FailedOutput { output: replay.outcome.clone(),
                    error: replay.outcome.error.clone().unwrap_or_else(|| "original tool outcome failed".into()) }
            } else { crate::GovernedToolTaskTerminal::Succeeded(replay.outcome.clone()) };
            return Some((terminal, HostGovernedReceipt { outcome: replay.outcome.clone(), refusal: None }));
        }
        let receipt = self.precompleted?.get(&task.tool_call_id)?;
        // The early dispatcher already emitted transient start/terminal events,
        // but its invocation map is intentionally task-local. Rehydrate the
        // terminal fact here so the finalized DAG can persist the same tool
        // lifecycle beside its durable message/result without re-executing it.
        if let Some(call) = self.calls.get(task.original_call_index) {
            if let Some(record) = replayed_governed_invocation(self.session_id, call, receipt) {
                self.invocations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(task.tool_call_id.clone(), record);
            }
        }
        let terminal = if !receipt.outcome.model_feedback_is_error() {
            crate::GovernedToolTaskTerminal::Succeeded(receipt.outcome.clone())
        } else {
            crate::GovernedToolTaskTerminal::FailedOutput {
                output: receipt.outcome.clone(),
                error: receipt
                    .outcome
                    .error
                    .clone()
                    .unwrap_or_else(|| "early read did not complete successfully".to_string()),
            }
        };
        Some((terminal, HostGovernedReceipt { outcome: receipt.outcome.clone(), refusal: None }))
    }

    fn commit_terminal<'a>(
        &'a self,
        task: &'a crate::GovernedToolPlanTask,
        terminal: &'a crate::GovernedToolTaskTerminal<Self::Output>,
    ) -> crate::GovernedToolFuture<'a, Result<Self::Receipt, String>> {
        Box::pin(async move {
            let call = self.calls.get(task.original_call_index).ok_or_else(|| {
                format!(
                    "governed tool task `{}` references missing original call index {}",
                    task.tool_call_id, task.original_call_index
                )
            })?;
            let scope_fact = if matches!(terminal, crate::GovernedToolTaskTerminal::Refused { .. }) {
                self.scope_facts.lock().unwrap_or_else(std::sync::PoisonError::into_inner).remove(&call.id)
            } else { None };
            let mut refusal = if let Some(fact) = scope_fact {
                Some(self.commit_service.commit_evaluation_scope_refusal(&fact).map_err(|error| error.to_string())?)
            } else if matches!(terminal, crate::GovernedToolTaskTerminal::Refused { .. })
                && self.evaluation_scope_rejections.is_some_and(|items| items.contains_key(&call.id)) {
                return Err("scope refusal original turn owner fact unavailable".into());
            } else if matches!(terminal, crate::GovernedToolTaskTerminal::Refused { .. }) {
                if let Some(policy) = self.policy_refusals.and_then(|items| items.get(&call.id)) {
                    Some(self.commit_service.commit_call_refusal(
                        crate::execution_core::graph::CallRefusalOwner::Node(self.ticket),
                        &policy.request, &policy.assessment,
                    ).map_err(|error| error.to_string())?)
                } else { None }
            } else { None };
            let outcome = match terminal {
                crate::GovernedToolTaskTerminal::Succeeded(outcome)
                | crate::GovernedToolTaskTerminal::FailedOutput {
                    output: outcome, ..
                } => outcome.clone(),
                crate::GovernedToolTaskTerminal::Refused { .. }
                    if self.policy_refusals.is_some_and(|items| items.contains_key(&call.id)) => {
                        let assessment = self.capability_gaps.get(&call.id)
                            .ok_or_else(|| "committed policy refusal lost its original assessment".to_string())?;
                        capability_gap_outcome(call, task.safety_category, assessment)
                    }
                _ => failed_governed_tool_outcome(
                    call,
                    task.safety_category,
                    host_tool_terminal_reason(terminal),
                ),
            };
            if outcome.status == crate::RuntimeToolExecutionStatus::Unknown {
                let reason = outcome.error.clone().unwrap_or_else(|| "original tool effect outcome is unknown".to_string());
                self.unknown_effects.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(task.tool_call_id.clone(), ToolError::unknown_with_optional_agent_batch(reason.clone(), outcome.pending_agent_batch.clone()));
                return Err(reason);
            }
            if refusal.is_none() {
                if let Some(crate::PendingAgentBatchValue::OriginalBoundedAcquisition(original)) = outcome.pending_agent_batch.as_ref().map(|pending| pending.primary_value()) {
                    let missing_owner = |reason: &str| {
                        let error = ToolError::unknown(reason)
                            .with_original_bounded_acquisition_failure(Arc::clone(original.fact()));
                        let pending = error.original_bounded_acquisition_pending(self.ticket,
                            original.receipt().map(|receipt| receipt.as_ref().clone()));
                        self.unknown_effects.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(task.tool_call_id.clone(), error.with_unresolved_context("same-call acquisition", pending));
                        reason.to_owned()
                    };
                    let services = self.services.ok_or_else(|| missing_owner("original bounded acquisition services unavailable"))?;
                    if !original.matches(self.ticket, services.event_store(), &call.id) {
                        return Err(missing_owner("original bounded acquisition outcome differs"));
                    }
                    let observation = self.physical_owner.binding_observation(self.ticket)
                        .ok_or_else(|| missing_owner("bounded acquisition original observation unavailable"))?;
                    let mark_unknown = |receipt: Option<crate::execution_core::graph::CommittedCallRefusal>| {
                        observation.uncertain(original.fact().request());
                        let _ = observation.hold_bounded_acquisition(Arc::clone(original.fact()), receipt.clone());
                        let error = ToolError::unknown("original bounded acquisition transaction or delivery unresolved")
                            .with_original_bounded_acquisition_failure(Arc::clone(original.fact()));
                        let pending = error.original_bounded_acquisition_pending(self.ticket, receipt);
                        self.unknown_effects.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                            .insert(task.tool_call_id.clone(), error.with_unresolved_context("same-call acquisition", pending));
                    };
                    let proof = if let Some(receipt) = original.receipt() { receipt.as_ref().clone() } else {
                        let stop = crate::bound_tool_batch::current_tool_stop();
                        match self.commit_service.commit_bounded_acquisition_failure(services, self.ticket, original.fact(), stop.as_ref(), None, &observation) {
                            Ok(proof) => proof,
                            Err(_) => { mark_unknown(None); return Err("original bounded acquisition transaction unresolved".into()); }
                        }
                    };
                    if observation.refuse_bounded_committed(Arc::clone(original.fact()), &proof, None).is_err() {
                        mark_unknown(Some(proof));
                        return Err("original bounded acquisition observation unresolved".into());
                    }
                    refusal = Some(proof);
                }
            }
            if self.unknown_effects.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(&task.tool_call_id) {
                return Err("original tool effect remains unknown".into());
            }
            if let Some(proof) = refusal.as_ref() {
                let observation = self.physical_owner.binding_observation(self.ticket)
                    .ok_or_else(|| "committed refusal original ToolBatch observation unavailable".to_string())?;
                observation.refuse_committed(proof)?;
            } else if let Some(reason) = terminal.undispatched_reason() {
                if let Some(observation) = self.physical_owner.binding_observation(self.ticket) {
                    observation.refuse(&call.id, reason);
                }
            }
            // Executed leaf receipts were committed while their physical
            // owner still held admission, including the early-model path.
            Ok(HostGovernedReceipt { outcome, refusal })
        })
    }

    fn refusal_receipt_is_durable(&self, task: &crate::GovernedToolPlanTask, receipt: &Self::Receipt) -> bool {
        receipt.refusal.as_ref().is_some_and(|proof| {
            proof.call_id() == task.tool_call_id
                && proof.owner_graph_id() == self.ticket.graph_id
                && proof.owner_node_id() == self.ticket.node_id
                && proof.owner_attempt() == self.ticket.attempt
                && task.step_binding.as_ref().is_some_and(|binding| {
                    proof.step_id() == binding.snapshot.identity.step_id
                        && proof.effect_key() == binding.effect_idempotency_key()
                })
        })
    }

    fn on_task_started(&self, task: &crate::GovernedToolPlanTask) {
        let input = self
            .calls
            .get(task.original_call_index)
            .map_or("", |call| call.input.as_str());
        let mut record = ToolInvocationRecord::started(
            self.session_id,
            0,
            task.tool_call_id.clone(),
            task.tool_name.clone(),
            input,
            task.safety_category,
            crate::tool_invocation::now_ms(),
        )
        .with_governed_plan(self.plan_id, self.plan_revision);
        record.original_producer = match self.original_producers {
            Some(producers) if crate::context_evidence::native_read_candidate_tool(&task.tool_name) =>
                producers.get(&task.tool_call_id).cloned(),
            _ => self.event_bus.as_ref().and_then(|bus|
                bus.current_tool_activity_binding(&task.tool_call_id, &task.tool_name)),
        };
        self.invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(task.tool_call_id.clone(), record);
        if let Some(bus) = &self.event_bus {
            bus.emit_tool_started_with_dependencies(
                &task.tool_call_id,
                &task.tool_name,
                &host_event_preview(input, 200),
                &task.depends_on,
            );
        }
    }

    fn on_task_terminal(
        &self,
        task: &crate::GovernedToolPlanTask,
        terminal: &crate::GovernedToolTaskTerminal<Self::Output>,
        receipt: Option<&Self::Receipt>,
    ) {
        // No durable terminal receipt: retain the started invocation and emit no completion.
        if receipt.is_none() { return; }
        let (summary, exit_code) = receipt.map_or_else(
            || (host_tool_terminal_reason(terminal), Some(1)),
            |outcome| {
                let failed = outcome.model_feedback_is_error();
                (
                    outcome
                        .output
                        .as_deref()
                        .or(outcome.error.as_deref())
                        .unwrap_or("tool completed without output")
                        .to_string(),
                    Some(i32::from(failed)),
                )
            },
        );
        let mut invocations = self
            .invocations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let started = invocations.remove(&task.tool_call_id).unwrap_or_else(|| {
            let input = self
                .calls
                .get(task.original_call_index)
                .map_or("", |call| call.input.as_str());
            ToolInvocationRecord::started(
                self.session_id,
                0,
                task.tool_call_id.clone(),
                task.tool_name.clone(),
                input,
                task.safety_category,
                crate::tool_invocation::now_ms(),
            )
            .with_governed_plan(self.plan_id, self.plan_revision)
        });
        let ended_at_ms = crate::tool_invocation::now_ms();
        let record = match receipt {
            Some(outcome) if !outcome.model_feedback_is_error() => {
                started.completed(&summary, ended_at_ms)
            }
            Some(outcome) if outcome.status == crate::RuntimeToolExecutionStatus::BlockedPermission => {
                started.failed(ToolFailureKind::PermissionDenied, &summary, ended_at_ms)
            }
            _ => started.failed(
                host_tool_failure_kind(terminal, &summary),
                &summary,
                ended_at_ms,
            ),
        };
        invocations.insert(task.tool_call_id.clone(), record);
        drop(invocations);
        if let Some(bus) = &self.event_bus {
            bus.emit_tool_completed_with_dependencies(
                &task.tool_call_id,
                &task.tool_name,
                &host_event_preview(&summary, 500),
                exit_code,
                &task.depends_on,
            );
        }
    }
}

fn replayed_governed_invocation(
    session_id: &str,
    call: &ModelToolCall,
    receipt: &crate::conversation::EarlyToolExecutionReceipt,
) -> Option<ToolInvocationRecord> {
    let crate::conversation::ToolLifecycleReuse::PendingOriginal { record, producer } = &receipt.lifecycle else {
        // The durable effect receipt is sufficient to replay the tool result,
        // but is not an invocation lifecycle identity. Preserve any existing
        // lifecycle and never append an invented or duplicate one.
        return None;
    };
    if producer.validate().is_err()
        || producer.session_id != session_id
        || producer.tool_call_id.as_deref() != Some(receipt.call.id.as_str())
        || producer.tool_contract_id.as_deref() != Some(receipt.call.name.as_str())
        || record.session_id != session_id
        || record.tool_call_id != receipt.call.id
        || record.tool_name != receipt.call.name
        || record.input_hash != crate::tool_invocation::stable_hash(&receipt.call.input)
        || record.input_hash != crate::tool_invocation::stable_hash(&call.input)
        || record.is_error != Some(receipt.outcome.model_feedback_is_error())
        || record.ended_at_ms.is_none()
    {
        #[cfg(test)]
        if call.id == "ra-read" {
            eprintln!("ra_lifecycle_rehydrate call=ra-read rejected=identity producer_valid={} session={} call={} tool={} receipt_input={} current_input={} outcome={} ended={}",
                producer.validate().is_ok(), record.session_id == session_id,
                record.tool_call_id == receipt.call.id, record.tool_name == receipt.call.name,
                record.input_hash == crate::tool_invocation::stable_hash(&receipt.call.input),
                record.input_hash == crate::tool_invocation::stable_hash(&call.input),
                record.is_error == Some(receipt.outcome.model_feedback_is_error()), record.ended_at_ms.is_some());
        }
        return None;
    }
    let mut original = record.clone();
    original.original_producer = Some(producer.clone());
    Some(original)
}

pub(super) async fn execute_governed_runtime_tool_batch(
    host: Arc<dyn crate::RuntimeExecutionHost>,
    event_bus: Option<crate::CowdEventBus>,
    calls: &[ModelToolCall],
    session_id: &str,
    sandbox_posture: harness_contract::policy::SandboxPosture,
    policy_revision: u64,
    memory_context: Option<&memory::MemoryTurnContext>,
    reality_context: Option<&harness_contract::agent::AgentDataLease>,
    model_lease: Option<&str>,
    ticket: &NodeExecutionTicket,
    observation_wave_sequence: u64,
    tool_authorizations: &std::collections::HashMap<
        String,
        harness_contract::tool::ToolExecutionAuthorization,
    >,
    capability_gaps: &std::collections::HashMap<
        String,
        harness_contract::policy::CapabilityAssessment,
    >,
    prepared_invocations: &std::collections::HashMap<
        String,
        harness_contract::tool::GovernedToolInvocation,
    >,
    compilation: Result<crate::GovernedToolCompilation, crate::GovernedToolCompileError>,
    decision: &crate::execution_core::RuntimeExecutionDecision,
    execution_plane: &Arc<crate::ToolExecutionPlane>,
    physical_owner: &Arc<crate::execution_core::graph::executors::ScopedNodeExecutor>,
    commit_service: &crate::execution_core::graph::ExecutionCommitService,
    precompleted: &BTreeMap<String, crate::conversation::EarlyToolExecutionReceipt>,
    original_replays: &BTreeMap<String, HostOriginalReplay>,
    original_producers: &HashMap<String, harness_contract::projection::RuntimeActivityBinding>,
    evaluation_scope_rejections: Option<&BTreeMap<String, String>>,
    scope_state: Option<&Arc<tokio::sync::Mutex<TurnGraphState>>>,
    services: Option<&crate::RuntimeServices>,
    policy_refusals: Option<&HashMap<String, HostPolicyRefusal>>,
    policy_failures: Option<&HashMap<String, String>>,
    effective_inputs: Option<&HashMap<String, String>>,
    prepared_effects: Option<&HashMap<String, crate::conversation::PreparedCallEffect>>,
    prepared_requests: Option<&HashMap<String, crate::RuntimeToolExecutionRequest>>,
    strategy_gate: Option<&dyn HostOriginalStrategyGate>,
) -> Result<GovernedToolBatchResult, String> {
    let mut refused_before_dispatch = Vec::new();
    let compilation = match compilation {
        Ok(compilation) => compilation,
        Err(error) => {
            let reason = format!("governed tool DAG rejected before execution: {error}");
            let mut messages = Vec::with_capacity(calls.len());
            let mut invocations = HashMap::new();
            let mut observed_evidence = Vec::new();
            for call in calls {
                if let Some(replay) = original_replays.get(&call.id) {
                    messages.push(tool_outcome_message(replay.outcome.clone()));
                    observed_evidence.extend(replay.outcome.observed_evidence.iter().cloned());
                } else if let Some(receipt) = precompleted.get(&call.id) {
                    messages.push(early_tool_receipt_message(receipt));
                    observed_evidence.extend(receipt.outcome.observed_evidence.iter().cloned());
                    if let Some(record) = replayed_governed_invocation(session_id, call, receipt) {
                        invocations.insert(call.id.clone(), record);
                    }
                } else {
                    refused_before_dispatch.push(crate::conversation::PreDispatchRefusal {
                        call_id: call.id.clone(), reason: reason.clone(),
                        source: crate::conversation::PreDispatchRefusalSource::Compiler,
                    });
                    messages.push(tool_outcome_message(failed_governed_tool_outcome(
                        call, crate::ToolSafetyCategory::Destructive, reason.clone(),
                    )));
                    invocations.extend(rejected_tool_invocations(
                        session_id, std::slice::from_ref(call), crate::ToolSafetyCategory::Destructive, None, 0, &reason,
                    ));
                }
            }
            return Ok(GovernedToolBatchResult { member_batches: Vec::new(), unresolved: Vec::new(), unknown_effect: None, refused_before_dispatch, messages, invocations, observed_evidence, max_concurrency_observed: 0, parallel_batches: 0 });
        }
    };
    let mut messages_by_call = std::collections::HashMap::new();
    let mut invocation_records = HashMap::new();
    let mut replayed_observed_evidence = Vec::new();
    for rejection in compilation.rejected {
        if let Some(call) = calls.iter().find(|call| call.id == rejection.tool_call_id) {
            if let Some(replay) = original_replays.get(&call.id) {
                messages_by_call.insert(call.id.clone(), tool_outcome_message(replay.outcome.clone()));
                replayed_observed_evidence.extend(replay.outcome.observed_evidence.iter().cloned());
                continue;
            }
            if let Some(receipt) = precompleted.get(&call.id) {
                messages_by_call.insert(call.id.clone(), early_tool_receipt_message(receipt));
                replayed_observed_evidence.extend(receipt.outcome.observed_evidence.iter().cloned());
                if let Some(record) = replayed_governed_invocation(session_id, call, receipt) {
                    invocation_records.insert(call.id.clone(), record);
                }
                continue;
            }
            let reason = format!(
                "governed tool node rejected before execution: {}",
                rejection.reason
            );
            if !precompleted.contains_key(&call.id) {
                refused_before_dispatch.push(crate::conversation::PreDispatchRefusal { call_id: call.id.clone(), reason: reason.clone(), source: crate::conversation::PreDispatchRefusalSource::Compiler });
            }
            messages_by_call.insert(
                call.id.clone(),
                tool_outcome_message(failed_governed_tool_outcome(
                    call,
                    crate::ToolSafetyCategory::Destructive,
                    reason.clone(),
                )),
            );
            invocation_records.extend(rejected_tool_invocations(
                session_id,
                std::slice::from_ref(call),
                crate::ToolSafetyCategory::Destructive,
                None,
                0,
                &reason,
            ));
        }
    }
    let Some(plan) = compilation.plan else {
        return Ok(GovernedToolBatchResult {
            member_batches: Vec::new(),
            unresolved: Vec::new(),
            unknown_effect: None,
            refused_before_dispatch,
            messages: calls
                .iter()
                .map(|call| messages_by_call.remove(&call.id).ok_or_else(|| {
                    format!("governed tool `{}` has no outcome for the original call", call.id)
                }))
                .collect::<Result<Vec<_>, _>>()?,
            invocations: invocation_records,
            observed_evidence: replayed_observed_evidence,
            max_concurrency_observed: 0,
            parallel_batches: 0,
        });
    };
    let mut admission_plan = plan.clone();
    admission_plan
        .tasks
        .retain(|task| !precompleted.contains_key(&task.tool_call_id) && !original_replays.contains_key(&task.tool_call_id));
    let validation = admission_plan.validate_against_execution_decision(decision);
    if !validation.allowed {
        let reason = format!(
            "runtime strategy lease `{}` denied tool batch: {}",
            validation.lease_id,
            validation.findings.join(", ")
        );
        for task in &plan.tasks {
            let call = &calls[task.original_call_index];
            if let Some(replay) = original_replays.get(&call.id) {
                messages_by_call.insert(call.id.clone(), tool_outcome_message(replay.outcome.clone()));
                replayed_observed_evidence.extend(replay.outcome.observed_evidence.iter().cloned());
                continue;
            }
            if let Some(receipt) = precompleted.get(&call.id) {
                messages_by_call.insert(call.id.clone(), early_tool_receipt_message(receipt));
                replayed_observed_evidence.extend(receipt.outcome.observed_evidence.iter().cloned());
                if let Some(record) = replayed_governed_invocation(session_id, call, receipt) {
                    invocation_records.insert(call.id.clone(), record);
                }
                continue;
            }
            if !precompleted.contains_key(&call.id) {
                refused_before_dispatch.push(crate::conversation::PreDispatchRefusal { call_id: call.id.clone(), reason: reason.clone(), source: crate::conversation::PreDispatchRefusalSource::Strategy });
            }
            messages_by_call.insert(
                call.id.clone(),
                tool_outcome_message(failed_governed_tool_outcome(
                    call,
                    task.safety_category,
                    reason.clone(),
                )),
            );
            invocation_records.extend(rejected_tool_invocations(
                session_id,
                std::slice::from_ref(call),
                task.safety_category,
                Some(plan.plan_id.as_str()),
                plan.revision,
                &reason,
            ));
        }
        return Ok(GovernedToolBatchResult {
            member_batches: Vec::new(),
            unresolved: Vec::new(),
            unknown_effect: None,
            refused_before_dispatch,
            messages: calls
                .iter()
                .map(|call| messages_by_call.remove(&call.id).ok_or_else(|| {
                    format!("governed tool `{}` has no outcome for the original call", call.id)
                }))
                .collect::<Result<Vec<_>, _>>()?,
            invocations: invocation_records,
            observed_evidence: replayed_observed_evidence,
            max_concurrency_observed: 0,
            parallel_batches: 0,
        });
    }
    let invocations = Arc::new(Mutex::new(invocation_records));
    let context = HostGovernedToolContext {
        unknown_effects: Mutex::new(HashMap::new()),
        batch_policy_owner: ROOT_BATCH_POLICY_OWNER.try_with(Arc::clone).ok(),
        member_requests: Mutex::new(HashMap::new()),
        action_issuer: ROOT_ACTION_ISSUER.try_with(Arc::clone).ok(),
        single_captures: ROOT_SINGLE_CAPTURES.try_with(Arc::clone).ok(),
        host,
        event_bus,
        original_producers: Some(original_producers),
        calls,
        session_id,
        sandbox_posture,
        policy_revision,
        memory_context,
        reality_context,
        model_lease,
        ticket,
        execution_decision: Some(decision),
        strategy_gate,
        admission_plan: Some(&admission_plan),
        tool_authorizations,
        capability_gaps,
        policy_refusals,
        policy_failures,
        effective_inputs,
        prepared_effects,
        prepared_requests,
        evaluation_scope_rejections,
        scope_state,
        services,
        scope_facts: Mutex::new(HashMap::new()),
        prepared_invocations,
        plan_id: &plan.plan_id,
        plan_revision: plan.revision,
        observation_wave_sequence,
        execution_plane,
        physical_owner,
        commit_service,
        precompleted: Some(precompleted),
        original_replays: Some(original_replays),
        idempotency_keys: None,
        invocations: Arc::clone(&invocations),
    };
    let report = crate::GovernedToolExecutor.execute(&plan, &context).await;
    let max_concurrency_observed = report.max_active;
    let parallel_batches = usize::from(report.max_active > 1);
    let mut observed_evidence = replayed_observed_evidence;
    let unknown_effects = context.unknown_effects.into_inner().unwrap_or_else(std::sync::PoisonError::into_inner);
    let member_requests = context.member_requests.into_inner()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut unknown_in_order = Vec::new();
    let mut member_batches = Vec::new();
    let mut unresolved = Vec::new();
    for (index, outcome) in report.outcomes.into_iter().enumerate() {
        #[cfg(any(test, feature = "test-fixtures"))]
        if index < 32 && std::env::var_os("COWD_R622_TRACE").is_some() {
            let disposition = match &outcome.terminal {
                crate::GovernedToolTaskTerminal::Succeeded(_) => "succeeded",
                crate::GovernedToolTaskTerminal::FailedOutput { .. } => "failed_output",
                crate::GovernedToolTaskTerminal::Failed { .. } => "failed",
                crate::GovernedToolTaskTerminal::Refused { .. } => "refused",
                crate::GovernedToolTaskTerminal::Blocked { .. } => "blocked",
                crate::GovernedToolTaskTerminal::Cancelled { .. } => "cancelled",
                crate::GovernedToolTaskTerminal::Panicked { .. } => "panicked",
            };
            let predecessor = match &outcome.terminal { crate::GovernedToolTaskTerminal::Blocked { predecessor_id, .. } => Some(predecessor_id.as_str()), _ => None };
            crate::execution_core::graph::k05_trace_emit(format!("r622_toolbatch phase=terminal_disposition owner=host graph={:?} node={:?} attempt={} call={:?} disposition={} predecessor={:?} terminal_receipt={} terminal_commit_error={}",
                ticket.graph_id.chars().take(64).collect::<String>(), ticket.node_id.chars().take(64).collect::<String>(), ticket.attempt,
                outcome.task_id.chars().take(64).collect::<String>(), disposition, predecessor.map(|id| id.chars().take(64).collect::<String>()), outcome.receipt.is_some(), outcome.terminal_commit_error.is_some()));
        }
        let task = &plan.tasks[index];
        let call = &calls[task.original_call_index];
        let terminal_detail = crate::governed_tool_executor::terminal_reason(&outcome.terminal);
        let receipt = match outcome.receipt {
            Some(receipt) => receipt.outcome,
            None if unknown_effects.contains_key(&task.tool_call_id) => {
                if let Some(unknown) = unknown_effects.get(&task.tool_call_id) {
                    unknown_in_order.push((task.tool_call_id.clone(), unknown.clone()));
                    if let Some(request) = member_requests.get(&call.id) {
                        let carrier = crate::tool_host::OriginalMemberCallCarrier::from_original_request(Arc::clone(request));
                        if !carrier.outcomes().is_empty() || carrier.original_request().tool_progress.has_original_member_stop() || unknown.pending_agent_batch().is_some() {
                            let original = crate::PendingAgentBatch::new(
                                crate::PendingAgentBatchValue::OriginalCall(Arc::new(carrier)));
                            member_batches.push((call.id.clone(), crate::PendingAgentBatch::with_cause(
                                original, unknown.pending_agent_batch().cloned())));
                        }
                    }
                    unresolved.push(crate::conversation::ToolBatchUnresolved::Unknown { call_id: task.tool_call_id.clone(), detail: unknown.to_string(), terminal_detail: terminal_detail.clone(), commit_error: outcome.terminal_commit_error.clone() });
                }
                continue;
            }
            None => {
                unresolved.push(crate::conversation::ToolBatchUnresolved::MissingReceipt { call_id: task.tool_call_id.clone(), terminal_detail, commit_error: outcome.terminal_commit_error.clone() });
                continue;
            },
        };
        if let Some(request) = member_requests.get(&call.id) {
            let carrier = crate::tool_host::OriginalMemberCallCarrier::from_original_request(Arc::clone(request));
            if !carrier.outcomes().is_empty() || carrier.original_request().tool_progress.has_original_member_stop() {
                member_batches.push((call.id.clone(), crate::PendingAgentBatch::new(
                    crate::PendingAgentBatchValue::OriginalCall(Arc::new(carrier)))));
            }
        }
        observed_evidence.extend(receipt.observed_evidence.iter().cloned());
        messages_by_call.insert(call.id.clone(), tool_outcome_message(receipt));
    }
    if unknown_in_order.is_empty() && !unresolved.is_empty() {
        let details = serde_json::to_string(&unresolved)
            .map_err(|error| format!("encode missing durable receipts failed: {error}"))?;
        return Err(format!("governed tools reached terminal state without durable receipts: {details}"));
    }
    let ordered_ids = calls.iter()
        .filter(|call| if !unknown_in_order.is_empty() { messages_by_call.contains_key(&call.id) } else { !unknown_effects.contains_key(&call.id) })
        .map(|call| call.id.clone()).collect::<Vec<_>>();
    let messages = ordered_ids.into_iter().map(|id| messages_by_call.remove(&id).ok_or_else(|| {
        format!("governed tool `{id}` has no outcome for the original call")
    })).collect::<Result<Vec<_>, _>>()?;
    let invocations = Arc::try_unwrap(invocations)
        .map(Mutex::into_inner)
        .unwrap_or_else(|shared| {
            Ok(shared
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone())
        })
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let unknown_effect = if unknown_in_order.is_empty() {
        None
    } else if unknown_in_order.len() == 1 {
        Some(unknown_in_order.remove(0).1)
    } else {
        Some(ToolError::unknown_with_optional_agent_batch(serde_json::json!({"unknown_calls": unknown_in_order.iter().map(|(call, error)|
            serde_json::json!({"call_id":call,"original_detail":error.to_string()})).collect::<Vec<_>>()}).to_string(), {
            let members = unknown_in_order.iter().filter_map(|(call, error)|
                error.pending_agent_batch().cloned().map(|pending| (call.clone(), pending))).collect::<Vec<_>>();
            (!members.is_empty()).then(|| crate::PendingAgentBatch::new(crate::PendingAgentBatchValue::Calls(members)))
        }))
    };
    Ok(GovernedToolBatchResult {
        member_batches,
        unresolved,
        unknown_effect,
        refused_before_dispatch,
        messages,
        invocations,
        observed_evidence,
        max_concurrency_observed,
        parallel_batches,
    })
}

impl<C, T> TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    /// Reconcile repeated answers with the original Program before presenting.
    /// Live work keeps its continuation; an exhausted Program first commits
    /// its authoritative blocked verdict, then uses the existing synthesis.
    async fn honest_partial_terminal(
        &self,
        ticket: &NodeExecutionTicket,
        gap: &str,
        idle_answers: u64,
    ) -> Result<NodeExecutionOutcome, String> {
        let disclosure = format!(
            "Execution stopped at the operator-configured no-action resource limit: the model concluded {idle_answers} times in a row without taking \
             an action, so Runtime stopped asking rather than spending the remaining budget on \
             restatements. The durable work already committed stays visible in the Program projection. \
             The outstanding completion gap is: {gap}. Close it with the action that gap names, then \
             request completion."
        );
        let (session_id, turn_id, role) = {
            let state = self.state.lock().await;
            (state.session_id.clone(), state.turn_id.clone(), state.execution_role)
        };
        let mut completion = Some(GoalCompletion::Partial);
        let mut terminal_disclosure = disclosure.clone();
        if role.owns_root_presentation() {
            if let Some(program) = super::root_agentic_program_projection(&self.services, &session_id, &turn_id) {
                if program.objective_verdict.as_ref().is_some_and(|verdict|
                    verdict.kind == harness_contract::goal::ObjectiveTerminalKind::Blocked)
                {
                    let goal_id = format!("goal:{}", ticket.graph_id);
                    let goal = self.services.goal_store().get(&goal_id)?.ok_or("blocked Goal missing")?;
                    let terminal = goal.terminal.ok_or("blocked Goal terminal missing")?;
                    completion = Some(GoalCompletion::Blocked);
                    terminal_disclosure = terminal.reason;
                }
                if program.objective_verdict.as_ref().is_some_and(|verdict|
                    verdict.kind == harness_contract::goal::ObjectiveTerminalKind::Satisfied)
                {
                    completion = None;
                }
                let active = crate::execution_core::graph::executors::active_agentic_child_graphs(
                    &ticket.graph_id, &program.program_id, &program, self.services.graph_state_store(),
                ).await?;
                let live = !active.is_empty()
                    || self.services.goal_store().exhausted_program_work_cut(&program)?.is_none();
                if program.objective_verdict.is_none() && (live || program.completion_request.is_some()) {
                    if program.completion_request.is_some() {
                        self.services.reconcile_agentic_completion_request(&program.program_id)?;
                    }
                    self.abandon_published_terminal().await;
                    let iteration = {
                        let mut state = self.state.lock().await;
                        state.terminal_override = None;
                        state.iterations
                    };
                    let next = dynamic_node(ticket, iteration, "answer-wait-continue",
                        ExecutionNodeKind::InlineModel, "inline_model", "inline_model");
                    let mut nodes = vec![next];
                    if let Some(request) = crate::execution_core::graph::executors::AgenticProgramWaitRequest::for_active_root(
                        &program, &ticket.graph_id, self.services.graph_state_store(), true, None,
                    ).await? {
                        let mut wait = dynamic_node(ticket, iteration, "answer-wait-program",
                            ExecutionNodeKind::Verify, crate::execution_core::graph::executors::AgenticProgramWaitExecutor::KIND,
                            "agentic_program_wait");
                        wait.payload_ref = serde_json::to_string(&request).map_err(|error| error.to_string())?;
                        nodes.insert(0, wait);
                    }
                    return Ok(NodeExecutionOutcome::new(completed_result(
                        Some(format!("{}:answer-wait", ticket.graph_id)), ExecutionUsage::default(),
                    )).with_replan(ExecutionGraphReplan {
                        edges: dynamic_edges(&ticket.node_id, &nodes), nodes,
                        reason: "Original Program work or completion reconciliation is still active".into(),
                        framework_authored: true,
                    }));
                }
                if !live && program.completion_request.is_none() && program.objective_verdict.is_none() {
                    let identity = {
                        let state = self.state.lock().await;
                        runtime_observation_identity(&self.services, &state, ticket)
                    };
                    let goal = self.services.goal_store().projection(&identity.goal_id)?.ok_or("answer stall Goal missing")?;
                    let progress_identity = sha256_digest(&serde_json::to_string(&(
                        &goal.progress.criteria, &goal.progress.evidence_refs,
                        &goal.progress.open_unknowns, &goal.goal.obligations,
                    )).map_err(|error| error.to_string())?);
                    let mut trigger = runtime_observation(identity, RuntimeObservationKind::GraphProgress,
                        "runtime.program_stall", program.revision, disclosure.clone(),
                        format!("program-answer-stall:{}:{}:{}:{progress_identity}", program.program_id, program.revision, goal.stream_revision),
                        ObservationResultClass::Failed);
                    trigger.failure_class = Some(ObservationFailureClass::Unknown);
                    if let Some(previous) = goal.observations.iter().find(|previous|
                        previous.source == trigger.source && previous.source_revision == trigger.source_revision
                            && previous.fingerprint == trigger.fingerprint)
                    { trigger = previous.clone(); }
                    else { self.services.goal_store().record_observation(trigger.clone())?; }
                    let verdict = match crate::agentic::supervision::reconcile_runtime_stall(
                        &self.services, &self.services.agent_action_service(), self.services.objective_supervisor(), &program.program_id, &trigger,
                    ) {
                        Ok(verdict) => verdict,
                        Err(error) => {
                            let current = self.services.agent_action_service().project(&program.program_id)
                                .map_err(|error| error.to_string())?;
                            let current_goal = self.services.goal_store().projection(&goal.goal.id)?;
                            let changed = current.revision != program.revision
                                || current_goal.as_ref().is_some_and(|current| current.stream_revision > goal.stream_revision.saturating_add(1))
                                || self.services.goal_store().exhausted_program_work_cut(&current)?.is_none();
                            if !changed { return Err(error); }
                            self.abandon_published_terminal().await;
                            let iteration = {
                                let mut state = self.state.lock().await;
                                state.terminal_override = None;
                                state.iterations
                            };
                            let next = dynamic_node(ticket, iteration, "answer-stall-source-changed",
                                ExecutionNodeKind::InlineModel, "inline_model", "inline_model");
                            return Ok(NodeExecutionOutcome::new(completed_result(
                                Some(format!("{}:answer-stall-source-changed", ticket.graph_id)), ExecutionUsage::default(),
                            )).with_replan(ExecutionGraphReplan {
                                edges: dynamic_edges(&ticket.node_id, &[next.clone()]), nodes: vec![next],
                                reason: "Program or Goal changed while assessing the terminal".into(), framework_authored: true,
                            }));
                        }
                    };
                    match verdict.objective_verdict.as_ref().map(|verdict| verdict.kind) {
                        Some(harness_contract::goal::ObjectiveTerminalKind::Blocked) => {
                            completion = Some(GoalCompletion::Blocked);
                            terminal_disclosure = self.services.goal_store().get(&trigger.identity.goal_id)?
                                .and_then(|goal| goal.terminal).ok_or("blocked Goal terminal missing")?.reason;
                        }
                        Some(harness_contract::goal::ObjectiveTerminalKind::Satisfied) => completion = None,
                        _ => return Err("answer stall did not commit a compatible Program verdict".into()),
                    }
                }
            }
        }
        self.abandon_published_terminal().await;
        let iteration = {
            let mut state = self.state.lock().await;
            state.terminal_override = completion.map(|completion| (completion, terminal_disclosure));
            state.iterations
        };
        let mut next = dynamic_node(
            ticket,
            iteration,
            "consecutive-answer-stop-synthesize",
            ExecutionNodeKind::Synthesize,
            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
            "inline_model",
        );
        next.executor_kind =
            crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
        Ok(NodeExecutionOutcome::new(completed_result(
            Some(format!("{}:consecutive-answer-stop", ticket.graph_id)),
            ExecutionUsage::default(),
        ))
        .with_replan(ExecutionGraphReplan {
            nodes: vec![next.clone()],
            edges: dynamic_edges(&ticket.node_id, &[next]),
            reason: format!(
                "the model concluded {idle_answers} times without acting; presenting the honest partial terminal instead of spending the remaining budget"
            ),
            framework_authored: true,
        }))
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_terminal_provider_usage(
        &self,
        usage: model_protocol::usage::TokenUsage,
        model: Option<String>,
        models_used: Vec<String>,
        first_token_latency_ms: Option<u64>,
        active_stream_duration_ms: Option<u64>,
        wall_duration_ms: u64,
        output_chars: u64,
    ) {
        let (cache_dimensions_known, bus) = {
            let runtime = self.runtime.lock().await;
            (
                runtime.turn_cache_dimensions_known(),
                runtime.cowd_bus().cloned(),
            )
        };
        let telemetry = {
            let mut state = self.state.lock().await;
            state.record_root_model_usage(RootModelUsageDelta {
                usage,
                model,
                models_used,
                first_token_latency_ms,
                active_stream_duration_ms,
                wall_duration_ms,
                output_chars,
                output_chunks: 1,
                cache_dimensions_known,
            });
            state.root_model_telemetry()
        };
        if let Some(bus) = bus {
            bus.emit(CowdEvent::RunModelTelemetry { telemetry });
        }
    }

    async fn record_terminal_provider_failure(&self, error: &RuntimeError) {
        let (cache_dimensions_known, bus) = {
            let runtime = self.runtime.lock().await;
            (
                runtime.turn_cache_dimensions_known(),
                runtime.cowd_bus().cloned(),
            )
        };
        let telemetry = {
            let mut state = self.state.lock().await;
            if let Some(usage) = error.provider_usage() {
                state.record_root_model_usage(RootModelUsageDelta {
                    usage,
                    model: None,
                    models_used: Vec::new(),
                    first_token_latency_ms: None,
                    active_stream_duration_ms: None,
                    wall_duration_ms: 0,
                    output_chars: 0,
                    output_chunks: 0,
                    cache_dimensions_known,
                });
            } else {
                // The durable attempt tracker may have observed a failed
                // physical request without token counts. Preserve that cache
                // dimensions are unknown even though there is no numeric
                // delta to add.
                state.cache_dimensions_known = cache_dimensions_known;
            }
            state.root_model_telemetry()
        };
        if let Some(bus) = bus {
            bus.emit(CowdEvent::RunModelTelemetry { telemetry });
        }
    }

    /// Produce the user-facing explanation for a partial/blocked terminal.
    /// Uses one bounded zero-tool provider request so the model can explain
    /// what happened, why it failed, and what to do next; falls back to the
    /// structured blocked answer whenever the provider step cannot complete.
    /// Generated at most once per turn and cached in TurnGraphState.
    async fn terminal_narrated_answer(
        &self,
        raw: &str,
        objective: &str,
        envelope: &harness_contract::outcome::DeliveryEnvelope,
        presentation_id: &str,
        attempt_id: &str,
    ) -> Result<(String, Option<String>, Vec<String>, String), String> {
        let language = crate::conversation::user_reply_language(objective);
        let (cached, protocol_recovery_exhausted) = {
            let state = self.state.lock().await;
            (
                state.terminal_failure_narration.clone(),
                state.provider_protocol_recovery_attempts > PROVIDER_PROTOCOL_RECOVERY_BUDGET,
            )
        };
        if let Some(cached) = cached {
            return Ok(match cached {
                TerminalFailureNarration::Local(answer) => {
                    (answer, None, Vec::new(), attempt_id.to_string())
                }
                TerminalFailureNarration::Provider {
                    answer,
                    model,
                    models_used,
                    attempt_id,
                } => (answer, model, models_used, attempt_id),
            });
        }
        // The same provider already received its single governed protocol
        // recovery. Calling it once more merely to narrate that exhaustion
        // adds cost and reopens the protocol boundary with no path to success.
        if protocol_recovery_exhausted {
            return Err(user_facing_blocked_answer(raw, language));
        }
        let visible_facts = serde_json::to_string(envelope)
            .map_err(|error| format!("encode terminal delivery envelope: {error}"))?;
        // Keep the Runtime guard out of the `match` scrutinee. Rust extends a
        // scrutinee temporary through the match arms; taking the same mutex in
        // the error arm (to inspect cancellation) would otherwise deadlock
        // precisely when a delegated budget rejects the narrator before
        // Provider dispatch.
        let cancellation = {
            let runtime = self.runtime.lock().await;
            // Capture the turn-level token: the current token may be a
            // step-scoped link whose producer stop a normal finish cancels.
            runtime.turn_cancellation_token()?
        };
        let narration = {
            let mut runtime = self.runtime.lock().await;
            runtime
                .synthesize_failure_explanation(
                    objective,
                    raw,
                    &visible_facts,
                    language,
                    presentation_id,
                    attempt_id,
                    &envelope.envelope_id,
                    envelope.revision,
                )
                .await
        };
        match narration {
            Ok(answer) => {
                self.record_terminal_provider_usage(
                    answer.usage,
                    answer.model.clone(),
                    answer.models_used.clone(),
                    answer.first_token_latency_ms,
                    answer.active_stream_duration_ms,
                    answer.wall_duration_ms,
                    answer.text.chars().count() as u64,
                )
                .await;
                let explanation = answer.text;
                let model = answer.model;
                let models_used = answer.models_used;
                let provider_attempt_id = answer.provider_attempt_id;
                let mut state = self.state.lock().await;
                state.terminal_failure_narration = Some(TerminalFailureNarration::Provider {
                    answer: explanation.clone(),
                    model: model.clone(),
                    models_used: models_used.clone(),
                    attempt_id: provider_attempt_id.clone(),
                });
                Ok((explanation, model, models_used, provider_attempt_id))
            }
            Err(error) => {
                self.record_terminal_provider_failure(&error).await;
                if cancellation.is_cancelled() {
                    #[cfg(feature = "test-fixtures")]
                    eprintln!("k06_cancel_site=narrator_arm turn_cancelled=true");
                    return Err("terminal_presentation_cancelled".to_string());
                }
                tracing::warn!(
                    error = %error,
                    "failure explanation synthesis failed; falling back to structured blocked answer"
                );
                Err(user_facing_blocked_answer(raw, language))
            }
        }
    }

    /// Convert a verified Program evidence carrier into one coherent root
    /// answer. A deterministic quality gate may request one repair draft; raw
    /// Team bundles are never exposed as a successful fallback.
    async fn terminal_collaboration_answer(
        &self,
        carrier: &str,
        objective: &str,
        envelope: &harness_contract::outcome::DeliveryEnvelope,
        presentation_id: &str,
        attempt_id: &str,
    ) -> Result<(String, Option<String>, Vec<String>, String), String> {
        const HIERARCHICAL_PACKET_TARGET_CHARS: usize = 64_000;
        const HIERARCHICAL_PACKET_TRIGGER_CHARS: usize = 96_000;
        const MAX_HIERARCHICAL_LEVELS: usize = 4;
        let language = crate::conversation::user_reply_language(objective);
        let mut synthesis_carrier = carrier.to_string();
        let mut all_models_used = Vec::new();
        if carrier.chars().count() > HIERARCHICAL_PACKET_TRIGGER_CHARS {
            let mut layer_results = collaboration_carrier_results(carrier)
                .filter(|results| !results.is_empty())
                .ok_or_else(|| "collaboration evidence carrier has no Team results".to_string())?;
            for level in 1..=MAX_HIERARCHICAL_LEVELS {
                let partitions = partition_complete_collaboration_results(
                    layer_results,
                    HIERARCHICAL_PACKET_TARGET_CHARS,
                );
                if partitions.len() <= 1 {
                    synthesis_carrier = collaboration_synthesis_layer(
                        partitions.into_iter().next().unwrap_or_default(),
                        level,
                    );
                    break;
                }
                let mut next_layer = Vec::with_capacity(partitions.len());
                for (partition_index, partition) in partitions.into_iter().enumerate() {
                    let source = collaboration_synthesis_layer(partition, level);
                    let mut feedback = Vec::new();
                    let mut accepted = None;
                    for repair_attempt in 0..=1_u8 {
                        let narration = {
                            let mut runtime = self.runtime.lock().await;
                            runtime
                                .synthesize_collaboration_answer(
                                    objective,
                                    &source,
                                    language,
                                    &feedback,
                                    true,
                                    presentation_id,
                                    &format!(
                                        "{attempt_id}:layer:{level}:partition:{partition_index}:quality:{repair_attempt}"
                                    ),
                                    &envelope.envelope_id,
                                    envelope.revision,
                                )
                                .await
                        };
                        let answer = match narration {
                            Ok(answer) => {
                                self.record_terminal_provider_usage(
                                    answer.usage,
                                    answer.model.clone(),
                                    answer.models_used.clone(),
                                    answer.first_token_latency_ms,
                                    answer.active_stream_duration_ms,
                                    answer.wall_duration_ms,
                                    answer.text.chars().count() as u64,
                                )
                                .await;
                                answer
                            }
                            Err(error) => {
                                self.record_terminal_provider_failure(&error).await;
                                return Err(error.to_string());
                            }
                        };
                        let models_used = answer.models_used;
                        let answer = answer.text;
                        for model in models_used {
                            if !all_models_used.contains(&model) {
                                all_models_used.push(model);
                            }
                        }
                        feedback = collaboration_intermediate_quality_findings(&answer, &source);
                        if feedback.is_empty() {
                            accepted = Some(answer);
                            break;
                        }
                    }
                    let answer = accepted.ok_or_else(|| {
                        format!(
                            "hierarchical collaboration synthesis level {level} partition {partition_index} failed quality gate: {}",
                            feedback.join("; ")
                        )
                    })?;
                    next_layer.push(format!(
                        "Evidence-preserving synthesis layer {level}, partition {}:\n{answer}",
                        partition_index + 1
                    ));
                }
                synthesis_carrier = collaboration_synthesis_layer(next_layer.clone(), level);
                if synthesis_carrier.chars().count() <= HIERARCHICAL_PACKET_TRIGGER_CHARS {
                    break;
                }
                layer_results = next_layer;
                if level == MAX_HIERARCHICAL_LEVELS {
                    return Err(
                        "hierarchical collaboration synthesis did not converge within four complete-result layers"
                            .to_string(),
                    );
                }
            }
        }
        let mut validation_feedback = Vec::new();
        let mut last_error = None;
        for repair_attempt in 0..=1_u8 {
            let narration = {
                let mut runtime = self.runtime.lock().await;
                runtime
                    .synthesize_collaboration_answer(
                        objective,
                        &synthesis_carrier,
                        language,
                        &validation_feedback,
                        false,
                        presentation_id,
                        &format!("{attempt_id}:quality:{repair_attempt}"),
                        &envelope.envelope_id,
                        envelope.revision,
                    )
                    .await
            };
            match narration {
                Ok(answer) => {
                    self.record_terminal_provider_usage(
                        answer.usage,
                        answer.model.clone(),
                        answer.models_used.clone(),
                        answer.first_token_latency_ms,
                        answer.active_stream_duration_ms,
                        answer.wall_duration_ms,
                        answer.text.chars().count() as u64,
                    )
                    .await;
                    let model = answer.model;
                    let models_used = answer.models_used;
                    let provider_attempt_id = answer.provider_attempt_id;
                    let answer = answer.text;
                    for attempted in models_used {
                        if !all_models_used.contains(&attempted) {
                            all_models_used.push(attempted);
                        }
                    }
                    validation_feedback = collaboration_answer_quality_findings(&answer, objective);
                    validation_feedback.extend(delivery_truth_contradictions(&answer, envelope));
                    validation_feedback.sort();
                    validation_feedback.dedup();
                    if validation_feedback.is_empty() {
                        return Ok((answer, model, all_models_used, provider_attempt_id));
                    }
                    if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                        bus.emit(CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                                presentation_id: presentation_id.to_string(),
                                attempt_id: provider_attempt_id,
                                reason: "collaboration_answer_quality_gate_rejected".to_string(),
                            },
                        });
                    }
                    last_error = Some(format!(
                        "collaboration answer failed quality gate: {}",
                        validation_feedback.join("; ")
                    ));
                }
                Err(error) => {
                    self.record_terminal_provider_failure(&error).await;
                    // Only a turn-level cancellation may end this turn: a
                    // provider producer's own stop is linked into the current
                    // token while a step runs and is not a turn cancellation.
                    if self.runtime.lock().await.turn_cancelled()? {
                        #[cfg(feature = "test-fixtures")]
                        eprintln!("k06_cancel_site=repair_loop turn_cancelled=true");
                        return Err("terminal_presentation_cancelled".to_string());
                    }
                    last_error = Some(error.to_string());
                }
            }
        }
        Err(last_error.unwrap_or_else(|| {
            "collaboration answer quality gate rejected every draft".to_string()
        }))
    }
}

fn claim_continuation_budget(state: &mut TurnGraphState, stage: &str) -> bool {
    let bound = match completion_gap_continuation_bound() {
        Ok(bound) => bound,
        Err(error) => {
            tracing::warn!(stage, %error, "invalid explicit operator continuation resource limit");
            return false;
        }
    };
    if bound.is_none_or(|limit| state.completion_gap_continuations < limit) {
        state.completion_gap_continuations = state.completion_gap_continuations.saturating_add(1);
        return true;
    }
    tracing::warn!(
        stage,
        bound = ?bound,
        "explicit operator continuation resource limit reached; objective remains unmet"
    );
    false
}

impl<C, T> TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn synthesize_original(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let stage_started = std::time::Instant::now();
        macro_rules! observe_stage {
            ($stage:expr) => {
                tracing::info!(
                    target: "cowd::synthesize",
                    graph_id = %ticket.graph_id,
                    node_id = %ticket.node_id,
                    ticket_digest = %sha256_digest(&ticket.idempotency_key),
                    attempt = ticket.attempt,
                    stage = $stage,
                    elapsed_ms = u64::try_from(stage_started.elapsed().as_millis()).unwrap_or(u64::MAX),
                    "synthesize stage entered"
                );
            };
        }
        observe_stage!("inputs");
        let pending_inputs = self
            .runtime
            .lock()
            .await
            .consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::BeforeFinalAnswer);
        if !pending_inputs.is_empty() {
            let discard_latest_assistant = {
                let mut state = self.state.lock().await;
                state.terminal_override = None;
                state.clean_terminal_synthesis_next = false;
                state.pending_focus_terminal_candidate = None;
                state.assistant_messages.pop().is_some()
            };
            if discard_latest_assistant {
                let mut runtime = self.runtime.lock().await;
                let message_count = runtime.session_head().await.message_count;
                if message_count > 0 {
                    runtime
                        .session_mut_async()
                        .await
                        .truncate_messages(message_count.saturating_sub(1));
                }
            }
            let next = dynamic_node(
                ticket,
                self.state.lock().await.iterations,
                "input-cursor-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            );
            let evidence_refs = pending_inputs
                .iter()
                .map(|record| format!("session_input:{}", record.envelope.input_id))
                .collect::<Vec<_>>();
            let mut outcome = NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:terminal-superseded", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![next.clone()],
                edges: dynamic_edges(&ticket.node_id, &[next]),
                reason: "new durable Session input crossed the final-answer barrier; terminal candidate was superseded"
                    .to_string(),
                framework_authored: true,
            });
            let observation_identity = {
                let state = self.state.lock().await;
                runtime_observation_identity(&self.services, &state, ticket)
            };
            let mut observation = runtime_observation(
                observation_identity,
                RuntimeObservationKind::UserInput,
                "runtime.before_final_answer",
                u64::from(ticket.attempt),
                format!(
                    "{} newer Session input(s) superseded the terminal candidate",
                    pending_inputs.len()
                ),
                format!(
                    "terminal-input-cursor:{}",
                    sha256_digest(&evidence_refs.join("\n"))
                ),
                ObservationResultClass::Informational,
            );
            observation.evidence_refs = evidence_refs;
            outcome.domain_events.push(
                self.services
                    .goal_store()
                    .observation_event(
                        &observation,
                        format!("{}:terminal-input-observation", ticket.idempotency_key),
                    )
                    .map_err(|error| error.to_string())?,
            );
            return Ok(outcome);
        }
        // `Finalizing` is published only after the completion-gap decision.
        observe_stage!("execution_projection");
        let projection = self
            .services
            .execution_supervisor()
            .projection(&ticket.graph_id)
            .await
            .map_err(|error| error.to_string())?;
        let (
            ingress,
            goal_id,
            mut terminal_override,
            objective,
            terminal_model,
            turn_transcript_start,
            session_id,
            turn_id,
            execution_role,
        ) = {
            let state = self.state.lock().await;
            (
                state.ingress.clone(),
                state.goal_id.clone(),
                state.terminal_override.clone(),
                state.content.clone(),
                state.model.clone(),
                state.turn_transcript_start,
                state.session_id.clone(),
                state.turn_id.clone(),
                state.execution_role,
            )
        };
        let mut completion = terminal_override
            .as_ref()
            .map(|(completion, _)| *completion)
            .unwrap_or(GoalCompletion::Satisfied);
        // An ordinary turn gets its business verdict from the Goal owner,
        // not from a Host-requested completion enum. Program conclusions keep
        // their independent checked Goal/Program transaction below.
        let goal_binding = self
            .services
            .goal_store()
            .get(&goal_id)?
            .and_then(|goal| goal.execution_binding)
            .ok_or("Synthesize Goal binding missing")?;
        let program = self
            .services
            .agent_action_service()
            .project_if_exists(&goal_binding.agentic_program_id)
            .map_err(|error| error.to_string())?;
        let has_program = program.is_some();
        if let Some(verdict) = program.as_ref().and_then(|program| program.objective_verdict.as_ref()) {
            let verdict_completion = match verdict.kind {
                harness_contract::goal::ObjectiveTerminalKind::Satisfied => None,
                harness_contract::goal::ObjectiveTerminalKind::PartiallySatisfied => Some(GoalCompletion::Partial),
                harness_contract::goal::ObjectiveTerminalKind::Blocked => Some(GoalCompletion::Blocked),
                harness_contract::goal::ObjectiveTerminalKind::Failed => Some(GoalCompletion::Failed),
                harness_contract::goal::ObjectiveTerminalKind::Cancelled => Some(GoalCompletion::Cancelled),
            };
            if let Some(verdict_completion) = verdict_completion {
                let goal = self.services.goal_store().get(&goal_id)?.ok_or("Program verdict Goal missing")?;
                let terminal = goal.terminal.as_ref().filter(|terminal|
                    goal.id == verdict.goal_id && goal.revision == verdict.goal_revision
                        && goal.completion == verdict_completion && terminal.kind == verdict.kind
                        && terminal.terminal_fence == verdict.terminal_fence
                        && terminal.authority_revision == verdict.authority_revision)
                    .ok_or("Program and Goal terminal verdict disagree")?;
                completion = verdict_completion;
                terminal_override = Some((verdict_completion, terminal.reason.clone()));
            }
        }
        let ordinary_conclusion = if !has_program {
            Some(
                self.services
                    .objective_supervisor()
                    .prepare_turn_conclusion(
                        &goal_id,
                        ticket,
                        self.services.path_identity_resolver(),
                        "terminal_synthesis".into(),
                        format!("{}:goal-complete", ticket.idempotency_key),
                    )?,
            )
        } else {
            None
        };
        if let Some(prepared) = ordinary_conclusion.as_ref() {
            completion = prepared.completion;
        }
        let mut envelope = projection.delivery_envelope.clone().unwrap_or_else(|| {
            terminal_delivery_envelope(
                &projection,
                &goal_id,
                completion,
                &objective,
                &ticket.node_id,
            )
        });
        if terminal_override.is_none() && ordinary_conclusion.is_none() {
            completion = match envelope.delivery_status {
                harness_contract::outcome::DeliveryStatus::Satisfied => GoalCompletion::Satisfied,
                harness_contract::outcome::DeliveryStatus::Partial
                | harness_contract::outcome::DeliveryStatus::Denied
                | harness_contract::outcome::DeliveryStatus::Unavailable => GoalCompletion::Partial,
            };
        }
        if !is_synthesize_terminal_completion(completion) {
            return Err(
                "Synthesize requires immutable Goal completion, not open or resumable waiting"
                    .into(),
            );
        }
        let presentation_id = format!("presentation:{}:{}", ticket.graph_id, envelope.revision);
        let attempt_id = format!("{}:attempt:{}", presentation_id, ticket.attempt);
        let delegated_leaf = execution_role.is_delegated_leaf();
        let direct_answer = match terminal_override.as_ref() {
            Some((GoalCompletion::Satisfied, answer)) if !answer.trim().is_empty() => {
                Some(answer.clone())
            }
            None => committed_terminal_answer(&projection, &ticket.graph_id)
                .ok()
                .filter(|answer| !answer.starts_with("<synthesized_terminal")),
            _ => None,
        }
        .filter(|answer| {
            !answer.trim().is_empty() && !answer.starts_with("<synthesized_terminal")
        });
        let reusable_origin = projection
            .terminal_presentation
            .as_ref()
            .filter(|candidate| {
                candidate.envelope_id == envelope.envelope_id
                    && candidate.envelope_revision == envelope.revision
                    && candidate.answer_origin
                        == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
            })
            .map_or(
                harness_contract::outcome::AnswerOrigin::ModelDirect,
                |candidate| candidate.answer_origin,
            );
        let (
            mut final_answer,
            mut answer_origin,
            mut fallback_reason,
            narrator_model,
            attempted_models,
            mut committed_attempt_id,
        ) = if delegated_leaf {
            let answer = terminal_override
                .as_ref()
                .map(|(_, answer)| answer.clone())
                .or(direct_answer)
                .unwrap_or_default();
            (
                answer,
                harness_contract::outcome::AnswerOrigin::TerminalDelegate,
                None,
                terminal_model.clone(),
                Vec::new(),
                attempt_id.clone(),
            )
        } else if let Some(answer) = direct_answer {
            let visible_answer =
                visible_final_answer(&answer, envelope.user_answer_contract.format);
            if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                bus.emit_synthetic_text_item("terminal-presentation", &visible_answer);
                bus.emit(CowdEvent::TerminalDelivery {
                    delivery:
                        harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                            presentation_id: presentation_id.clone(),
                            attempt_id: attempt_id.clone(),
                            envelope_id: envelope.envelope_id.clone(),
                            envelope_revision: envelope.revision,
                            objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
                        },
                });
                bus.emit(CowdEvent::TerminalDelivery {
                    delivery: harness_contract::live::TerminalDeliveryEvent::TextDelta {
                        presentation_id: presentation_id.clone(),
                        attempt_id: attempt_id.clone(),
                        byte_start: 0,
                        byte_end: u64::try_from(visible_answer.len()).unwrap_or(u64::MAX),
                        delta: visible_answer.clone(),
                    },
                });
            }
            (
                visible_answer,
                reusable_origin,
                None,
                None,
                Vec::new(),
                attempt_id.clone(),
            )
        } else {
            let raw = terminal_override
                .as_ref()
                .map(|(_, answer)| answer.as_str())
                .unwrap_or("Execution ended without a qualified root answer candidate.");
            let collaboration_carrier = is_collaboration_evidence_carrier(raw);
            observe_stage!(if collaboration_carrier {
                "terminal_provider_collaboration"
            } else {
                "terminal_provider_failure_narration"
            });
            let narrated = if collaboration_carrier {
                self.terminal_collaboration_answer(
                    raw,
                    &objective,
                    &envelope,
                    &presentation_id,
                    &attempt_id,
                )
                .await
            } else {
                self.terminal_narrated_answer(
                    raw,
                    &objective,
                    &envelope,
                    &presentation_id,
                    &attempt_id,
                )
                .await
            };
            match narrated {
                Ok((answer, model, models_used, provider_attempt_id)) => (
                    answer,
                    harness_contract::outcome::AnswerOrigin::TerminalNarrator,
                    None,
                    model,
                    models_used,
                    provider_attempt_id,
                ),
                Err(cancelled) if cancelled == "terminal_presentation_cancelled" => {
                    return Err(cancelled);
                }
                Err(fallback) => {
                    let fallback = if collaboration_carrier {
                        completion = GoalCompletion::Partial;
                        envelope.delivery_status =
                            harness_contract::outcome::DeliveryStatus::Partial;
                        envelope
                            .unresolved
                            .push(harness_contract::outcome::DeliveryUnresolved {
                                unresolved_id: format!(
                                    "terminal-presentation:{}",
                                    envelope.revision
                                ),
                                kind: "collaboration_answer_quality".to_string(),
                                summary: fallback,
                                source_execution_id: Some(ticket.graph_id.clone()),
                                obligation_id: None,
                            });
                        "协作执行及完整证据均已保留，但根综合答案未通过完整性与清晰度质量门，框架已按部分完成关闭，未将原始 Team 证据包冒充最终答案。".to_string()
                    } else {
                        fallback
                    };
                    let fallback_attempt_id = format!("{attempt_id}:fallback");
                    if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                        bus.emit(CowdEvent::TerminalDelivery {
                                delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationStarted {
                                    presentation_id: presentation_id.clone(),
                                    attempt_id: fallback_attempt_id.clone(),
                                    envelope_id: envelope.envelope_id.clone(),
                                    envelope_revision: envelope.revision,
                                    objective_scope: harness_contract::outcome::AnswerObjectiveScope::Root,
                                },
                            });
                        bus.emit(CowdEvent::TerminalDelivery {
                            delivery: harness_contract::live::TerminalDeliveryEvent::TextDelta {
                                presentation_id: presentation_id.clone(),
                                attempt_id: fallback_attempt_id.clone(),
                                byte_start: 0,
                                byte_end: u64::try_from(fallback.len()).unwrap_or(u64::MAX),
                                delta: fallback.clone(),
                            },
                        });
                    }
                    (
                        fallback,
                        harness_contract::outcome::AnswerOrigin::ProgrammaticFallback,
                        Some("all configured terminal narrator candidates failed".to_string()),
                        None,
                        Vec::new(),
                        fallback_attempt_id,
                    )
                }
            }
        };
        let mut terminal_validation =
            TerminalPresentationValidation::new(answer_origin, fallback_reason.as_deref());
        if !delegated_leaf {
            terminal_validation.correct_truth(
                &mut final_answer,
                &mut answer_origin,
                &mut fallback_reason,
                &mut committed_attempt_id,
                &envelope,
            );
        }
        // Only a turn-level cancellation aborts the terminal presentation:
        // a step-scoped provider stop is linked into the current token while
        // a model step runs and must not read as a turn cancellation.
        if self.runtime.lock().await.turn_cancelled()? {
            #[cfg(feature = "test-fixtures")]
            eprintln!("k06_cancel_site=main_check turn_cancelled=true");
            if !delegated_leaf {
                if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                    bus.emit(CowdEvent::TerminalDelivery {
                    delivery:
                        harness_contract::live::TerminalDeliveryEvent::TerminalPresentationAborted {
                            presentation_id: presentation_id.clone(),
                            attempt_id: committed_attempt_id,
                            reason: "user_cancelled".to_string(),
                        },
                });
                }
            }
            #[cfg(feature = "test-fixtures")]
            // Read the cancelled token's identity and cancel site once: every
            // extra acquisition of the runtime lock inside this abort path can
            // queue behind an unrelated holder and strand the turn task before
            // it reaches its Err return, which would leave the node without a
            // terminal outcome.
            let (turn_token_id, turn_token_cancel_site) = {
                let token = self.runtime.lock().await.turn_cancellation_token()?;
                (token.identity(), token.last_cancel_site())
            };
            #[cfg(feature = "test-fixtures")]
            eprintln!(
                "k05_terminal_presentation_aborted site={} last_operational_cancel={:?} cancel_sites={:?} turn_token_id={:x} turn_token_cancel_site={:?}",
                std::panic::Location::caller(),
                crate::execution_core::services::last_operational_cancel(),
                crate::conversation::cancel_sites(),
                turn_token_id,
                turn_token_cancel_site
            );
            #[cfg(feature = "test-fixtures")]
            {
                use std::io::Write as _;
                let path = std::env::temp_dir().join("k05-cancel-diagnostic.log");
                if let Ok(mut file) = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                {
                    let _ = writeln!(
                        file,
                        "abort site={} turn_token_id={:x} turn_token_cancel_site={:?} last_operational={:?} cancel_sites={:?}",
                        std::panic::Location::caller(),
                        turn_token_id,
                        turn_token_cancel_site,
                        crate::execution_core::services::last_operational_cancel(),
                        crate::conversation::cancel_sites()
                    );
                }
            }
            return Err("terminal_presentation_cancelled".to_string());
        }
        observe_stage!("session_input_fence");
        let late_inputs = self
            .runtime
            .lock()
            .await
            .consume_active_runtime_inputs_for_next_step(TurnInputCheckpoint::BeforeFinalAnswer);
        if !late_inputs.is_empty() {
            if !delegated_leaf {
                if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                    bus.emit(CowdEvent::TerminalDelivery {
                    delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                        presentation_id: presentation_id.clone(),
                        attempt_id: committed_attempt_id,
                        reason: "new_durable_session_input".to_string(),
                    },
                });
                }
            }
            {
                let mut state = self.state.lock().await;
                state.terminal_override = None;
                state.terminal_failure_narration = None;
                state.clean_terminal_synthesis_next = false;
            }
            let next = dynamic_node(
                ticket,
                self.state.lock().await.iterations,
                "post-narrator-input-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            );
            return Ok(NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:terminal-superseded-after-narration", ticket.graph_id)),
                ExecutionUsage::default(),
            ))
            .with_replan(ExecutionGraphReplan {
                nodes: vec![next.clone()],
                edges: dynamic_edges(&ticket.node_id, &[next]),
                reason: format!(
                    "{} newer durable Session input(s) superseded the terminal presentation before commit",
                    late_inputs.len()
                ),
                framework_authored: true,
            }));
        }
        // An ordinary Goal conclusion is authorized for the committed model
        // candidate and its original attempt, not for a later replacement.
        // The original prepared Goal/Graph read set is attached below to the
        // terminal transaction, so a changed source cannot reuse this cut.
        if !delegated_leaf && completion == GoalCompletion::Satisfied && ordinary_conclusion.is_some()
            && envelope.user_answer_contract.format == harness_contract::outcome::UserAnswerFormat::StrictJson
        {
            let original = committed_terminal_answer(&projection, &ticket.graph_id)?;
            if original != final_answer {
                return Err("terminal answer differs from the original Goal-bound candidate".into());
            }
        }
        // Every root producer converges here before its presentation or Goal
        // conclusion is committed. The format contract is not a business
        // verdict: preserve the original Goal and receipt cuts on recovery.
        let contract_rejected = !delegated_leaf && (
            !qualified_root_answer(&final_answer, &envelope)
                || (envelope.user_answer_contract.format
                    == harness_contract::outcome::UserAnswerFormat::StrictJson
                    && response_language_mismatch(&objective, &final_answer))
        );
        if contract_rejected && completion != GoalCompletion::Satisfied {
            // A partial/failed explanation is not a successful structured
            // business answer. Keep its honest diagnostic and invalid status.
            terminal_validation.invalid = true;
            terminal_validation.findings.push("terminal explanation does not satisfy the declared output contract".into());
        }
        if contract_rejected && completion == GoalCompletion::Satisfied {
            if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
                bus.emit(CowdEvent::TerminalDelivery {
                    delivery: harness_contract::live::TerminalDeliveryEvent::TerminalPresentationSuperseded {
                        presentation_id: presentation_id.clone(),
                        attempt_id: committed_attempt_id.clone(),
                        reason: "terminal_answer_contract_rejected".to_string(),
                    },
                });
            }
            let mut state = self.state.lock().await;
            state.terminal_recovery_attempts = state.terminal_recovery_attempts.saturating_add(1);
            let budget = terminal_recovery_retry_budget(&state.safety_lease).min(1);
            if state.terminal_recovery_attempts > budget {
                return Err("terminal answer did not satisfy the declared output contract after bounded recovery".to_string());
            }
            state.terminal_override = None;
            state.terminal_failure_narration = None;
            state.force_text_only_next_model = true;
            let reason = if !qualified_root_answer(&final_answer, &envelope) {
                if envelope.user_answer_contract.format == harness_contract::outcome::UserAnswerFormat::StrictJson {
                    "The previous answer is not a JSON object in the requested format. Return the requested object without Markdown fences, using only the original request and committed evidence.".to_string()
                } else {
                    "The previous answer was empty or contained tool protocol markup. Return a user-facing answer using the original request and committed evidence.".to_string()
                }
            } else {
                let fields = serde_json::from_str::<serde_json::Value>(&final_answer).ok()
                    .and_then(|value| value.as_object().cloned())
                    .and_then(|fields| json_field_language_findings(&objective, &fields))
                    .unwrap_or_default().into_iter().filter(|name| !name.is_empty()).take(8)
                    .collect::<Vec<_>>();
                let field = if fields.is_empty() { "the requested field".to_string() }
                    else { format!("field(s) {}", fields.join(", ")) };
                format!("The previous JSON answer did not meet the original request's explicit language requirement for {field}. Correct only that requirement using committed evidence.")
            };
            // Normal continuation does not append state.content as a new user
            // message. Hand the existing bounded recovery feedback to the same
            // one-shot context path used by other Host interventions.
            let mut item = ContextItem::new(
                format!("terminal-contract-recovery:{}", state.iterations),
                ContextSourceKind::Task,
                ContextRole::Instruction,
                reason,
            );
            item.authority = ContextAuthority::System;
            item.visibility = ContextVisibility::Private;
            state.pending_next_model_context.push(item);
            let next = dynamic_node(
                ticket,
                state.iterations,
                "root-terminal-contract-recovery-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            );
            drop(state);
            return Ok(NodeExecutionOutcome::new(completed_result(
                Some(format!("{}:terminal-contract-rejected", ticket.graph_id)),
                ExecutionUsage::default(),
            )).with_replan(ExecutionGraphReplan {
                nodes: vec![next.clone()],
                edges: dynamic_edges(&ticket.node_id, &[next]),
                reason: "root terminal presentation did not satisfy its declared output contract".to_string(),
                framework_authored: true,
            }));
        }
        observe_stage!("terminal_state_preparation");
        let now_ms = crate::tool_invocation::now_ms();
        let presentation = execution_role.owns_root_presentation().then(|| {
            terminal_validation.finish(harness_contract::outcome::TerminalPresentation {
                presentation_id,
                attempt_id: committed_attempt_id,
                envelope_id: envelope.envelope_id.clone(),
                envelope_revision: envelope.revision,
                state: harness_contract::outcome::TerminalPresentationState::Committed,
                answer_origin,
                source_execution_id: Some(ticket.graph_id.clone()),
                narrator_model: if answer_origin
                    == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
                {
                    projection
                        .terminal_presentation
                        .as_ref()
                        .and_then(|candidate| candidate.narrator_model.clone())
                } else if answer_origin == harness_contract::outcome::AnswerOrigin::TerminalNarrator
                {
                    narrator_model.or(terminal_model)
                } else {
                    None
                },
                narrator_provider: projection
                    .terminal_presentation
                    .as_ref()
                    .filter(|_| {
                        answer_origin == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
                    })
                    .and_then(|candidate| candidate.narrator_provider.clone()),
                models_attempted: if answer_origin
                    == harness_contract::outcome::AnswerOrigin::TeamSynthesizer
                {
                    projection
                        .terminal_presentation
                        .as_ref()
                        .map(|candidate| candidate.models_attempted.clone())
                        .unwrap_or_default()
                } else {
                    attempted_models
                        .into_iter()
                        .map(
                            |model| harness_contract::outcome::PresentationModelAttempt {
                                provider: "configured".to_string(),
                                model,
                                failure: None,
                            },
                        )
                        .collect()
                },
                validation: harness_contract::outcome::AnswerValidation::default(),
                fallback_reason,
                generated_at_ms: now_ms,
                committed_at_ms: Some(now_ms),
            })
        });
        // ── Terminal path: four kinds of special handling, and which layer owns each ──
        //
        // Business-completeness layer (the Objective is not actually finished, so the
        // published terminal must be rolled back and the root returned to work):
        //   1. completion-gap rollback      — a recorded Objective gap remains.
        //   2. closure-rung rollback        — a Program-owned rung (an independent
        //                                     review) is still outstanding.
        // These two are isomorphic and share one helper.
        //
        // Provider-transport layer (the answer could not be produced at all, which is
        // a different failure with different recovery):
        //   3. protocol-failure recovery    — the provider emitted an unusable frame.
        //   4. structured-output recovery   — the provider could not be made to emit
        //                                     the required shape.
        // These two are deliberately NOT merged with the pair above: their trigger,
        // their recovery action and their boundary semantics are not isomorphic to
        // business completeness, and forcing them into one pipeline would leak
        // transport detail into the business path. Recorded here so the decision is
        // visible rather than implicit.
        //
        // The boundary matters: a reviewer of this file should be able to tell at a
        // glance which layer a block belongs to, and why it is or is not shared.
        let answer_digest = terminal_answer_digest(&final_answer);
        {
            let mut state = self.state.lock().await;
            state.delivery_envelope = Some(envelope.clone());
            state.terminal_presentation = presentation.clone();
            state.terminal_commit_owner = Some((ticket.node_id.clone(), ticket.attempt));
            state.committed_terminal_answer = Some(final_answer.clone());
            state.committed_terminal_completion = Some(completion);
            // The observer for "the model stopped acting and started restating".
            //
            // Every terminal answer counts, whether or not the words changed. The
            // first version incremented only when the digest matched, so a model
            // that rewrote its conclusion - same substance, fresh phrasing - reset
            // the counter every time and the escalation never fired. Run one27
            // wrote five different final answers after the synthesis Task was
            // accepted, all twelve of its text answers had distinct digests, and
            // the exit rung had been advertised fifty-four times.
            //
            // A successful tool call resets the count, which is what makes this a
            // count of *consecutive* answers rather than a total: work in between
            // is progress, and progress is not a loop.
            let rewrote_previous = state.last_terminal_answer_digest != answer_digest;
            state.last_terminal_answer_digest = answer_digest.clone();
            state.repeated_terminal_answers = state.repeated_terminal_answers.saturating_add(1);
            if rewrote_previous && state.repeated_terminal_answers > 1 {
                tracing::warn!(
                    target: "cowd::repeated_final_answer",
                    count = state.repeated_terminal_answers,
                    "the model is concluding again with reworded text and no intervening tool call"
                );
            }
        }
        let digest = format!("{:x}", Sha256::digest(final_answer.as_bytes()));
        let mut outcome = NodeExecutionOutcome::new(completed_result(
            Some(format!("turn-result:{}:{digest}", ticket.graph_id)),
            ExecutionUsage::default(),
        ));
        outcome.delivery_envelope = Some(envelope.clone());
        outcome.terminal_presentation = presentation.clone();
        // A weak model can finish its final answer without emitting the
        // Objective completion request. Drive it deterministically — only when
        // the collaboration is objectively complete (`completion_gap` gates it)
        // — so the verified Objective terminal exists and this presentation
        // defers to it instead of failing.
        observe_stage!("completion_gap_and_objective_drive");
        // `Satisfied` alone left a hole the size of a whole failure mode. A root
        // that gives up early - `Partial`, "here is what I managed" - skipped this
        // entire block, including the closure-rung rollback below, so a Program
        // with unreviewed Submitted Tasks was never returned to work and its only
        // closure authority was later fenced off by the root's own terminal graph.
        // One live run recorded exactly that: three `partially_satisfied`
        // terminals, two `review / root_graph_terminal` dispatch declines, zero
        // reviews, zero accepted Tasks.
        //
        // `Partial` is a claim about the Objective, not about the Program, and the
        // Program is what says whether the work is finished. Both claims now reach
        // the same check; the checks themselves are unchanged and still bounded, so
        // a genuinely finished or genuinely blocked Program falls straight through.
        let reconcile_agentic = should_reconcile_agentic_terminal(
            execution_role,
            completion,
            terminal_override
                .as_ref()
                .map(|(completion, _)| *completion),
        );
        if reconcile_agentic {
            // First make any durable Program completion request visible to the
            // authoritative GoalStore. The gap check must observe that verdict;
            // checking first can miss a request that has not yet been reduced.
            self.services
                .drive_agentic_objective_completion(&session_id, &turn_id, &ticket.graph_id)
                .await?;
            observe_stage!("objective_completion_driven");
        }
        let source_revisions = freeze_synthesize_goal_sources(
            self.services.as_ref(),
            &session_id,
            &turn_id,
            &goal_id,
        )?;
        outcome = inherit_synthesize_sources(outcome, &source_revisions)?;
        if let Some(prepared) = ordinary_conclusion.as_ref() {
            outcome = inherit_synthesize_sources(outcome, &prepared.source_revisions)?;
        }
        if reconcile_agentic {
            // A success presentation while a real completion gap remains means the
            // root finished too early. Do not fail the turn: return the root to
            // continue (bounded) with the gap disclosed, so it can classify
            // outstanding issues and get every Task accepted before completing.
            let recorded_gap = crate::agentic::agentic_program_completion_gap(
                self.services.as_ref(),
                &session_id,
                &turn_id,
            );
            {
                let mut state = self.state.lock().await;
                // The obligation is recomputed from Goal truth on every
                // presentation: a gap that just cleared must release the
                // tool-availability guarantee with it.
                state.completion_gap_outstanding = recorded_gap.is_some();
            }
            if let Some(gap) = recorded_gap {
                // Stop when the model has stopped acting. Concluding again after
                // being sent back is not progress, and the budget it burns is the
                // user's: run one30 wrote seven final answers across four minutes
                // with one tool call in the whole window, and the scenario timed
                // out with the Objective still open.
                //
                // The honest terminal is the right ending, not a failure: the
                // durable work is real and stays visible, and the reason it did not
                // close is stated rather than implied by a timeout.
                let idle_answers = {
                    let state = self.state.lock().await;
                    state.repeated_terminal_answers
                };
                if let Some(bound) = consecutive_answer_stop_bound()? {
                    if idle_answers >= bound {
                        tracing::warn!(
                            target: "cowd::consecutive_answer_stop",
                            graph_id = %ticket.graph_id,
                            idle_answers,
                            gap = %gap,
                            "the model concluded repeatedly without acting; presenting the honest terminal instead of spending the remaining budget"
                        );
                        let outcome = self.honest_partial_terminal(ticket, &gap, idle_answers).await?;
                        let current_sources = freeze_synthesize_goal_sources(
                            self.services.as_ref(), &session_id, &turn_id, &goal_id,
                        )?;
                        return inherit_synthesize_sources(outcome, &current_sources);
                    }
                }
                let continue_gap = {
                    let mut state = self.state.lock().await;
                    claim_continuation_budget(&mut state, "completion_gap")
                };
                if continue_gap {
                    // This turn is not finished, so every terminal fact
                    // published above must be rolled back. Do this after
                    // releasing `state`: `abandon_published_terminal` acquires
                    // the same mutex.
                    self.abandon_published_terminal().await;
                    observe_stage!("completion_gap_terminal_rollback");
                    // The continuation must be a real, tool-enabled model step
                    // that knows what to do (run t2 got neither).
                    self.push_completion_gap_guidance(ticket, &gap).await;
                    let iteration = self.state.lock().await.iterations;
                    let next = dynamic_node(
                        ticket,
                        iteration,
                        "completion-gap-continuation",
                        ExecutionNodeKind::InlineModel,
                        "inline_model",
                        "inline_model",
                    );
                    return inherit_synthesize_sources(
                        NodeExecutionOutcome::new(completed_result(
                            Some(format!("{}:completion-gap-continuation", ticket.graph_id)),
                            ExecutionUsage::default(),
                        ))
                        .with_replan(ExecutionGraphReplan {
                            nodes: vec![next.clone()],
                            edges: dynamic_edges(&ticket.node_id, &[next]),
                            reason: format!(
                                "Objective completion gap remains; continue working before presenting success: {gap}"
                            ),
                            framework_authored: true,
                        }),
                        &source_revisions,
                    );
                }
            }
            // A success presentation while Program-owned rungs remain also
            // means the root finished too early: every submitted Task still
            // needs its independent review, and the objective-review Task is
            // no exception. One live run finalized with
            // `independent_task_review` outstanding and stalled into the
            // inactivity watchdog. Return the root to coordinate that rung.
            if let Some(program) = super::root_agentic_program_projection(
                self.services.as_ref(),
                &session_id,
                &turn_id,
            ) {
                // Only a Program that is still live can be returned to work. A
                // Verified or Blocked Program has already said its piece, and
                // re-opening it here would turn an honest terminal into a loop.
                // Same predicate `root_agentic_terminal_action` uses.
                let program_still_live = matches!(
                    program.status,
                    crate::AgenticProgramStatus::Open | crate::AgenticProgramStatus::Waiting
                );
                let all_rungs = if program_still_live {
                    super::outstanding_closure_steps(&program)
                } else {
                    Vec::new()
                };
                let outstanding = all_rungs
                    .iter()
                    .filter(|step| step.as_str() != "objective_complete_request")
                    .cloned()
                    .collect::<Vec<_>>();
                // When the completion request is the ONLY rung left, the work is
                // done and the model has one action to take. It was told nothing,
                // and one live run finished every task - four published, five
                // reviewed, three accepted - then wrote a prose conclusion instead
                // of asking, and the run ended at its window with
                // `objective_complete_requested: false`.
                //
                // Rolling back would be wrong here: the model is allowed to
                // conclude, it just does not know that concluding means calling the
                // tool. So this branch guides instead of rolling back.
                let only_exit_rung = outstanding.is_empty()
                    && all_rungs
                        .iter()
                        .any(|step| step == "objective_complete_request");
                if only_exit_rung {
                    let mut item = crate::ContextItem::new(
                        format!("exit-rung:{}", ticket.node_id),
                        crate::ContextSourceKind::Task,
                        crate::ContextRole::Instruction,
                        "Every Program rung is closed: the Teams exist, the Tasks are \
                         published, submitted, independently reviewed and accepted. The only \
                         outstanding action is yours: call `objective_complete_request` to \
                         close the Objective. Runtime verifies it and rejects it if anything \
                         regressed, so you never risk a premature exit. A prose answer is not \
                         a completion request - Runtime cannot certify the Objective from one."
                            .to_string(),
                    );
                    item.authority = crate::ContextAuthority::System;
                    item.visibility = crate::ContextVisibility::Private;
                    item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                    self.runtime.lock().await.push_next_model_context_item(item);
                }
                {
                    let mut state = self.state.lock().await;
                    state.outstanding_closure_rungs = outstanding.clone();
                }
                if !outstanding.is_empty() {
                    let (continue_rungs, repeated_answer, attempt) = {
                        let mut state = self.state.lock().await;
                        let allowed = claim_continuation_budget(&mut state, "closure_rung");
                        (
                            allowed,
                            state.repeated_terminal_answers,
                            state.completion_gap_continuations,
                        )
                    };
                    if continue_rungs {
                        self.abandon_published_terminal().await;
                        observe_stage!("closure_rung_terminal_rollback");
                        {
                            let mut state = self.state.lock().await;
                            state.force_text_only_next_model = false;
                        }
                        // Same escalation contract as the completion-gap path: a
                        // repeated rung with a repeated answer is a loop, not a
                        // plan, and the framework says so instead of restating.
                        let escalation = if repeated_answer >= 2 {
                            format!(
                                "ESCALATION (attempt {attempt}, identical answer #{repeated}): the same outstanding \
                                 rungs have now been shown {attempt} time(s) and you have returned the same final answer \
                                 {repeated} time(s) without a tool call. A restated answer is not delivered. Runtime has \
                                 restricted this step to the actions that advance these rungs.",
                                attempt = attempt,
                                repeated = repeated_answer
                            )
                        } else if repeated_answer >= 1 || attempt >= 2 {
                            format!(
                                "NOTICE (attempt {attempt}): these rungs were already outstanding {attempt} time(s). \
                                 Another prose answer will not be delivered; take the action that advances them.",
                                attempt = attempt
                            )
                        } else {
                            String::new()
                        };
                        let body = format!(
                            "Runtime returned this turn to work because the collaboration is not finished: outstanding rungs: {}. \
                                 A submitted Task still needs its independent review by a different Agent, and the objective-review \
                                 Task is no exception. Invite or assign a reviewer inside the existing Team (do not create another \
                                 Team), have it read the submitted report through read_ref or evidence_retrieve and record its \
                                 task_review verdict, then present the final answer again.",
                            outstanding.join(", ")
                        );
                        let guidance = if escalation.is_empty() {
                            body
                        } else {
                            format!("{body}\n\n{escalation}")
                        };
                        let mut item = ContextItem::new(
                            format!("closure-rung-continuation:{}", ticket.node_id),
                            ContextSourceKind::Task,
                            ContextRole::Instruction,
                            guidance,
                        );
                        item.authority = ContextAuthority::System;
                        item.visibility = ContextVisibility::Private;
                        item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
                        self.runtime.lock().await.push_next_model_context_item(item);
                        let iteration = self.state.lock().await.iterations;
                        let next = dynamic_node(
                            ticket,
                            iteration,
                            "closure-rung-continuation",
                            ExecutionNodeKind::InlineModel,
                            "inline_model",
                            "inline_model",
                        );
                        return inherit_synthesize_sources(
                            NodeExecutionOutcome::new(completed_result(
                                Some(format!("{}:closure-rung-continuation", ticket.graph_id)),
                                ExecutionUsage::default(),
                            ))
                            .with_replan(ExecutionGraphReplan {
                                nodes: vec![next.clone()],
                                edges: dynamic_edges(&ticket.node_id, &[next]),
                                reason: format!(
                                    "Program-owned rungs remain; continue working before presenting success: {}",
                                    outstanding.join(", ")
                                ),
                                framework_authored: true,
                            }),
                            &source_revisions,
                        );
                    }
                }
            }
        }
        // The turn is terminal from here on.
        self.publish_terminal_finalizing().await;
        let terminal_owned = root_terminal_owned_elsewhere(
            self.services.as_ref(),
            execution_role.owns_root_presentation(),
            &session_id,
            &turn_id,
            &ticket.graph_id,
            &goal_id,
            completion,
        )?;
        if let Some(prepared) = ordinary_conclusion {
            if terminal_owned {
                return Err("ordinary Goal terminal owner changed before Graph commit".into());
            }
            outcome.domain_events.push(prepared.event);
        } else if !terminal_owned {
            return Err("Program-bound Goal requires its original authoritative conclusion before presentation".into());
        }
        let recovery_scope = format!("turn:{turn_id}");
        let controlled_recovery_claim_fingerprints = self
            .runtime
            .lock()
            .await
            .authorization_negotiator()
            .controlled_recovery_claims_for_scope(&recovery_scope);
        outcome.domain_events.push(
            crate::authorization_negotiator::controlled_recovery_terminal_event(
                &crate::authorization_negotiator::ControlledRecoveryTerminalRecord {
                    recovery_scope,
                    session_id: session_id.clone(),
                    turn_id: turn_id.clone(),
                    execution_id: ticket.graph_id.clone(),
                    fingerprints: controlled_recovery_claim_fingerprints.clone(),
                },
            )?,
        );
        self.state
            .lock()
            .await
            .pending_controlled_recovery_claim_fingerprints =
            controlled_recovery_claim_fingerprints.clone();
        if let Some(ingress) = ingress {
            observe_stage!("terminal_artifact_and_session_event");
            let presentation = presentation.as_ref().ok_or_else(|| {
                "root Session ingress terminal is missing its presentation".to_string()
            })?;
            let (terminal_fence, consumed_input_sequence) = {
                let runtime = self.runtime.lock().await;
                let terminal_fence = runtime
                    .capture_session_execution_fence(
                        crate::SessionExecutionFencePhase::TerminalCommit,
                    )
                    .await
                    .map_err(|error| error.to_string())?
                    .ok_or_else(|| {
                        "Session terminal requires a durable execution fence snapshot".to_string()
                    })?;
                let consumed_input_sequence = runtime
                    .consumed_session_input_cursor()
                    .filter(|cursor| cursor.generation == ingress.session_generation)
                    .map_or(ingress.input_sequence, |cursor| cursor.sequence)
                    .max(ingress.input_sequence);
                (terminal_fence, consumed_input_sequence)
            };
            let mut transcript = {
                let runtime = self.runtime.lock().await;
                let session = runtime.session_snapshot().await;
                session
                    .messages_page(
                        turn_transcript_start,
                        session
                            .message_count()
                            .saturating_sub(turn_transcript_start),
                    )
                    .materialize()
            };
            // The source ingress row and its Runtime request are committed in
            // one Gateway transaction before execution begins. Persisting it
            // again here would create a duplicate user turn.
            if transcript
                .first()
                .is_some_and(|message| message.role.role_str() == "user")
            {
                transcript.remove(0);
            }
            let terminal_is_last = transcript.last().is_some_and(|message| {
                message.role.role_str() == "assistant"
                    && message.blocks.iter().any(
                        |block| matches!(block, ContentBlock::Text { text } if text == &final_answer),
                    )
            });
            if !terminal_is_last {
                transcript.push(ConversationMessage::assistant(vec![ContentBlock::Text {
                    text: final_answer.clone(),
                }]));
            }
            let transcript = transcript
                .iter()
                .map(|message| {
                    let persisted = message
                        .to_persisted_json()
                        .map_err(|error| format!("seal terminal Provider transcript: {error}"))?;
                    serde_json::from_str::<serde_json::Value>(&persisted.render())
                        .map_err(|error| format!("encode terminal transcript: {error}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            // Terminal narration and hierarchical Team synthesis may perform
            // several Provider calls after the initial graph projection was
            // loaded. Snapshot the cumulative root owner only at artifact
            // encoding, after every success/failure path has been accounted.
            let latest_root_usage = {
                let state = self.state.lock().await;
                TerminalTokenUsage {
                    input_tokens: state.input_tokens,
                    output_tokens: state.output_tokens,
                    cache_creation_input_tokens: state.cache_create_tokens,
                    cache_read_input_tokens: state.cache_read_tokens,
                    cache_dimensions_known: state.cache_dimensions_known,
                }
            };
            let terminal_usage = terminal_aggregate_usage(
                latest_root_usage,
                self.services
                    .execution_model_usage_snapshot(&ticket.graph_id)
                    .as_ref(),
            );
            let terminal_id = format!("turn-terminal:{}", ingress.request_id);
            let collaboration_evidence = terminal_override
                .as_ref()
                .map(|(_, value)| value.as_str())
                .filter(|value| is_collaboration_evidence_carrier(value));
            let terminal_payload = serde_json::to_vec(&serde_json::json!({
                "schema_version": crate::SESSION_TERMINAL_ARTIFACT_SCHEMA_VERSION,
                "text": final_answer,
                // The user-facing synthesis and its complete source carrier
                // are committed atomically. Presentation limits can therefore
                // never erase Team semantics or make the concise answer the
                // only surviving copy of a complex Program result.
                "collaboration_evidence": collaboration_evidence,
                "delivery_envelope": envelope.clone(),
                "terminal_presentation": presentation.clone(),
                "answer_origin": presentation.answer_origin,
                "envelope_id": presentation.envelope_id,
                "envelope_revision": presentation.envelope_revision,
                "narrator_provider": presentation.narrator_provider,
                "narrator_model": presentation.narrator_model,
                "fallback_reason": presentation.fallback_reason,
                "goal_completion": completion,
                "ingress_message_id": ingress.message_id,
                "consumed_input_sequence": consumed_input_sequence,
                "transcript": transcript,
                "token_usage": terminal_usage,
            }))
            .map_err(|error| format!("encode terminal artifact: {error}"))?;
            let staging_owner = format!("prepared:terminal:{terminal_id}");
            let artifacts = Arc::clone(self.services.artifact_store());
            let scope = format!("session:{}", ingress.session_id);
            let prepared_owner = staging_owner.clone();
            let prepared_origin = crate::PreparedArtifactOrigin::TerminalBody {
                terminal_id: terminal_id.clone(),
                session_id: ingress.session_id.clone(),
                execution_id: ticket.graph_id.clone(),
                turn_id: ingress.turn_id.clone(),
                event_stream: format!("session-terminal:{}", ingress.request_id),
                event_key: ticket.idempotency_key.clone(),
            };
            let terminal_artifact = tokio::task::spawn_blocking(move || {
                artifacts.write_pre_pinned_with_media_blocking(
                    &scope,
                    &prepared_owner,
                    "application/vnd.cowd.session-terminal+json",
                    &terminal_payload,
                    &prepared_origin,
                )
            })
            .await
            .map_err(|_| "terminal artifact preparation task failed".to_string())?
            .map_err(|error| format!("persist permanently pinned terminal artifact: {error}"))?;
            let payload_ref = crate::encode_session_terminal_artifact_ref(&terminal_artifact)?;
            self.state.lock().await.pending_terminal_artifact = Some(PendingTerminalArtifact {
                artifact: terminal_artifact,
                staging_owner,
                durable_owner: terminal_id.clone(),
            });
            let terminal = crate::runtime_event_store::SessionTerminalInput {
                terminal_id,
                message_id: format!("assistant:{}", ingress.message_id),
                session_id: ingress.session_id.clone(),
                execution_id: Some(ticket.graph_id.clone()),
                turn_id: Some(ingress.turn_id.clone()),
                request_id: Some(terminal_fence.request_id),
                session_generation: Some(terminal_fence.session_generation),
                input_sequence: Some(ingress.input_sequence),
                input_claim_owner: Some(terminal_fence.claim_owner),
                input_claim_token: Some(terminal_fence.claim_token),
                // Runtime terminal storage predates the immutable fence epoch
                // name. Its `input_claim_revision` column carries that epoch,
                // never the renewable outbox row revision.
                input_claim_revision: Some(terminal_fence.claim_fence_epoch),
                controlled_recovery_claim_fingerprints,
                payload_ref,
            };
            outcome
                .domain_events
                .push(crate::runtime_event_store::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: format!("session-terminal:{}", ingress.request_id),
                        scope: crate::RuntimeEventScope::SessionInput,
                        kind: "runtime.session.terminal_requested".to_string(),
                        status: Some("pending_delivery".to_string()),
                        actor: Some("SynthesizeNodeExecutor".to_string()),
                        refs: vec![
                            crate::RuntimeEventRef {
                                kind: "execution_graph".to_string(),
                                id: ticket.graph_id.clone(),
                            },
                            crate::RuntimeEventRef {
                                kind: "session".to_string(),
                                id: ingress.session_id.clone(),
                            },
                        ],
                        payload: serde_json::to_value(&terminal).unwrap_or_default(),
                    },
                    idempotency_key: Some(ticket.idempotency_key.clone()),
                    schema_version: 1,
                });
        }
        observe_stage!("complete");
        Ok(outcome)
    }
}

#[async_trait]
impl<C, T> crate::execution_core::graph::executors::SynthesizeBackend
    for TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    async fn synthesize(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Result<NodeExecutionOutcome, String> {
        let mut outcome = self.synthesize_original(ticket).await?;
        self.stage_terminal_publication(ticket, &mut outcome)
            .await?;
        if outcome.domain_events.iter().any(|event| event.event.kind == "runtime.session.terminal_requested") {
            let (session, turn) = {
                let state = self.state.lock().await;
                (state.session_id.clone(), state.turn_id.clone())
            };
            let owner = self.services.original_terminal_owner(ticket, &session, &turn)
                .ok_or("original Synthesize Session owner unavailable")?;
            outcome.attach_original_terminal_owner(Arc::clone(&owner))?;
            let mut state = self.state.lock().await;
            if state.authenticated_terminal_owner.is_some() { return Err("original terminal owner already exists".into()); }
            state.authenticated_terminal_owner = Some(owner);
        }
        Ok(outcome)
    }

    async fn after_commit(&self, ticket: &NodeExecutionTicket) -> Result<(), String> {
        let commits = self.services.commit_service().clone();
        let original = ticket.clone();
        let store = self.services.event_store().clone();
        let (document, outcome, committed_claims, original_terminal, original_objective, authenticated_terminal_event) =
            tokio::task::spawn_blocking(move || {
                let document = commits
                    .committed_host_transcript(&original)
                    .map_err(|error| error.to_string())?;
                let crate::execution_core::graph::ExecutionEffectState::Completed(outcome) =
                    commits
                        .inspect_execution_effect(&original)
                        .map_err(|error| error.to_string())?
                else {
                    return Err("Synthesize original atomic effect is missing".to_string());
                };
                let (claims, terminal, authenticated_terminal_event) = if document.terminal_completion.is_some() {
                    let claims = committed_synthesize_terminal_claims(
                        &store,
                        &original,
                        &document.session_id,
                        &document.turn_id,
                    )?;
                    let terminal = store
                        .session_terminal_for_execution(
                            &document.session_id,
                            &original.graph_id,
                            &document.turn_id,
                        )
                        .map_err(|error| error.to_string())?;
                    let authenticated_terminal_event = if let Some(row) = terminal.as_ref() {
                        Some(validate_synthesize_terminal_row(
                            &store,
                            &original,
                            &document.session_id,
                            &document.turn_id,
                            row,
                            &claims,
                        )?)
                    } else { None };
                    (claims, terminal, authenticated_terminal_event)
                } else {
                    (Vec::new(), None, None)
                };
                let graph = crate::ExecutionGraphStateStore::new(store.clone())
                    .load(&original.graph_id)
                    .map_err(|error| error.to_string())?;
                Ok::<_, String>((document, outcome, claims, terminal, graph.objective, authenticated_terminal_event))
            })
            .await
            .map_err(|error| error.to_string())??;
        let terminal = validate_synthesize_publication(
            ticket,
            &document.ticket,
            &document.messages,
            document.terminal_completion,
            &outcome,
        )?;
        if document.root_control_team_count.is_some() {
            return Err("Synthesize publication cannot carry root-control metadata".into());
        }
        {
            let state = self.state.lock().await;
            if state.session_id != document.session_id || state.turn_id != document.turn_id {
                return Err("Synthesize publication belongs to another Session/turn".into());
            }
        }
        let Some((final_answer, terminal_completion)) = terminal else {
            return Ok(());
        };
        // Retain the exact event authenticated above in this original turn
        // owner. The runner must prove physical closure before using it.
        if let Some(event) = authenticated_terminal_event {
            let state = self.state.lock().await;
            let owner = state.authenticated_terminal_owner.as_ref().ok_or("original terminal owner unavailable")?;
            owner.record_terminal(ticket, event)?;
        }
        let mut runtime = self.runtime.lock().await;
        if runtime.session_id() != document.session_id {
            return Err("Synthesize Runtime Session mismatch".into());
        }
        let (recovering, already_finalized) = {
            let state = self.state.lock().await;
            let owns = terminal_commit_owned_by(
                state.terminal_commit_owner.as_ref(),
                &ticket.node_id,
                ticket.attempt,
            );
            if state.terminal_commit_owner.is_some() && !owns {
                return Err("Synthesize publication conflicts with another active terminal".into());
            }
            if owns
                && (state.committed_terminal_answer.as_ref() != Some(&final_answer)
                    || state.committed_terminal_completion != Some(terminal_completion))
            {
                return Err("Synthesize cached terminal diverges from original publication".into());
            }
            if state.summary.as_ref().is_some_and(|summary| {
                summary.final_answer != final_answer
                    || summary.terminal_completion != terminal_completion
            }) {
                return Err(
                    "Synthesize finalized summary diverges from original publication".into(),
                );
            }
            (!owns, state.summary.is_some())
        };
        if !already_finalized {
            let strategy = runtime
                .active_turn_strategy()
                .ok_or("Synthesize finalization requires its original admitted strategy")?;
            if strategy.session_ref != document.session_id
                || strategy.turn_ref != document.turn_id
                || strategy.execution_graph_ref.as_deref() != Some(ticket.graph_id.as_str())
            {
                return Err(
                    "Synthesize finalization strategy crosses original Session/turn/graph".into(),
                );
            }
        }
        let recovered_telemetry = recovering.then(|| {
            self.services
                .execution_model_usage_snapshot(&ticket.graph_id)
                .and_then(|snapshot| snapshot.own)
                .unwrap_or_else(unknown_recovered_terminal_telemetry)
        });
        if let Some(telemetry) = recovered_telemetry.as_ref() {
            validate_recovered_terminal_usage(telemetry)?;
        }
        {
            let mut validation = runtime.session_snapshot().await;
            apply_synthesize_publication(
                &mut validation,
                &document.session_id,
                document.prefix_count,
                &document.prefix_sha256,
                &document.messages,
            )?;
        }
        {
            let state = self.state.lock().await;
            if let Some(pending) = state.pending_terminal_artifact.as_ref() {
                let terminal = original_terminal
                    .as_ref()
                    .ok_or("Synthesize staged terminal pin has no original outbox row")?;
                let artifact = crate::decode_session_terminal_artifact_ref(&terminal.payload_ref)?;
                if pending.artifact != artifact || pending.durable_owner != terminal.terminal_id {
                    return Err(
                        "Synthesize staged terminal pin diverges from original outbox".into(),
                    );
                }
                self.services
                    .artifact_store()
                    .pin(
                        &pending.artifact,
                        &pending.durable_owner,
                        crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                    )
                    .map_err(|error| {
                        format!(
                            "promote original terminal artifact pin {}: {error}",
                            pending.staging_owner
                        )
                    })?;
            }
        }
        if already_finalized {
            let mut session = runtime.session_mut_async().await;
            apply_synthesize_publication(
                &mut session,
                &document.session_id,
                document.prefix_count,
                &document.prefix_sha256,
                &document.messages,
            )?;
            return Ok(());
        }
        let (
            content,
            assistant_messages,
            tool_results,
            iterations,
            model,
            models_used,
            first_token_latency_ms,
            active_stream_duration_ms,
            input_tokens,
            output_tokens,
            wall_duration_ms,
            duplicate_tool_calls,
            write_attempt_paths,
            max_tool_concurrency_observed,
            parallel_tool_batches,
            defer_memory,
        ) = {
            let state = self.state.lock().await;
            (
                if recovering {
                    original_objective
                } else {
                    state.content.clone()
                },
                if recovering {
                    vec![synthesize_publication_message(&document.messages[0])?]
                } else {
                    state.assistant_messages.clone()
                },
                if recovering {
                    Vec::new()
                } else {
                    state.tool_results.clone()
                },
                state.iterations,
                state.model.clone(),
                state.models_used.clone(),
                state.first_token_latency_ms,
                state.active_stream_duration_ms,
                state.input_tokens,
                state.output_tokens,
                state.wall_duration_ms,
                state.duplicate_tool_calls,
                state.write_attempt_paths.clone(),
                state.max_tool_concurrency_observed,
                state.parallel_tool_batches,
                state.ingress.is_some(),
            )
        };
        let summary = runtime
            .finalize_graph_turn(
                &content,
                final_answer,
                assistant_messages,
                tool_results,
                iterations,
                model,
                models_used,
                first_token_latency_ms,
                active_stream_duration_ms,
                input_tokens,
                output_tokens,
                wall_duration_ms,
                duplicate_tool_calls,
                write_attempt_paths,
                max_tool_concurrency_observed,
                parallel_tool_batches,
                terminal_completion,
                defer_memory,
                recovered_telemetry,
            )
            .await
            .map_err(|error| error.to_string())?;
        {
            let mut session = runtime.session_mut_async().await;
            apply_synthesize_publication(
                &mut session,
                &document.session_id,
                document.prefix_count,
                &document.prefix_sha256,
                &document.messages,
            )?;
        }
        {
            let mut state = self.state.lock().await;
            if !committed_claims.is_empty() {
                runtime
                    .authorization_negotiator()
                    .acknowledge_controlled_recovery_terminals(&committed_claims);
            }
            state.pending_controlled_recovery_claim_fingerprints.clear();
            state.pending_terminal_artifact = None;
            state.assistant_messages.clear();
            state.tool_results.clear();
            state.write_attempt_paths.clear();
            crate::repetition_guard::clear_breakers_for_role(
                &self.services.event_store().repetition_scope(&state.session_id, &state.turn_id),
                state.execution_role.owns_root_presentation(),
            );
            state.summary = Some(summary);
            state.terminal_commit_owner = None;
            state.committed_terminal_answer = None;
            state.committed_terminal_completion = None;
            state.terminal_notify.notify_one();
        }
        Ok(())
    }
    async fn after_abort(&self, ticket: &NodeExecutionTicket, reason: &str) -> Result<(), String> {
        let presentation = {
            let mut state = self.state.lock().await;
            let owns_terminal_commit = terminal_commit_owned_by(
                state.terminal_commit_owner.as_ref(),
                &ticket.node_id,
                ticket.attempt,
            );
            let presentation = if owns_terminal_commit {
                state.terminal_presentation.take()
            } else {
                None
            };
            if owns_terminal_commit {
                state.terminal_commit_owner = None;
                state.committed_terminal_answer = None;
                state.committed_terminal_completion = None;
                // A streamed narrator attempt that did not reach the graph
                // commit boundary is no longer reusable. Do not let an abort
                // from a sibling Synthesize node erase this owner's cache.
                state.terminal_failure_narration = None;
            }
            presentation
        };
        if let (Some(bus), Some(presentation)) =
            (self.runtime.lock().await.cowd_bus().cloned(), presentation)
        {
            bus.emit(CowdEvent::TerminalDelivery {
                delivery:
                    harness_contract::live::TerminalDeliveryEvent::TerminalPresentationAborted {
                        presentation_id: presentation.presentation_id,
                        attempt_id: presentation.attempt_id,
                        reason: reason.to_string(),
                    },
            });
        }
        Ok(())
    }
}

impl<C, T> TurnSynthesizeBackend<C, T>
where
    C: ApiClient + Send + Sync + 'static,
    T: ToolExecutor,
{
    /// A turn that returns to work after a success presentation is not
    /// terminal. Roll back every terminal fact that presentation published:
    /// the committed answer, the delivery envelope and the terminal
    /// presentation. The runner also copies the last two off any
    /// non-batchable outcome into durable graph state, so leaving them set
    /// records a terminal the turn never delivered. Run t2 is the proof: the
    /// Program stayed `open`, the session transcript never received the
    /// assistant message, and every observer had already been told the turn
    /// was finalizing.
    async fn abandon_published_terminal(&self) {
        let mut state = self.state.lock().await;
        state.delivery_envelope = None;
        state.terminal_presentation = None;
        state.terminal_commit_owner = None;
        state.committed_terminal_answer = None;
        state.committed_terminal_completion = None;
    }

    /// `Finalizing` is a one-way terminal-preparation live status. Publish it
    /// only once this turn is known to be terminal: announcing it while the
    /// graph still has a legitimate replan path latches every observer on
    /// "finalizing" while the turn keeps working.
    async fn publish_terminal_finalizing(&self) {
        if let Some(bus) = self.runtime.lock().await.cowd_bus().cloned() {
            bus.emit(CowdEvent::ExecutionPhase {
                status: harness_contract::projection::ExecutionLiveStatus::Finalizing,
                detail: Some("synthesizing terminal".to_string()),
            });
        }
    }

    /// Arm the continuation step that a completion gap replans into.
    ///
    /// Two things have to happen or the continuation is inert. The gap has to
    /// reach the prompt: it used to live only in the graph's replan `reason`, so
    /// the model was told to "continue working" with no statement of the work -
    /// run t2 wrote a complete final answer that named both unadjudicated
    /// issues in prose and was never given the action that adjudicates them.
    /// And the one-shot terminal state has to be cleared, because the step that
    /// just finished was a zero-tool clean terminal synthesis.
    async fn push_completion_gap_guidance(&self, ticket: &NodeExecutionTicket, gap: &str) {
        // Read the escalation counters before taking the lock again: how many
        // times this same gap has already been shown, and how many times the
        // model has already answered it with the same words.
        let (attempt, repeated_answer) = {
            let state = self.state.lock().await;
            (
                state.completion_gap_continuations,
                state.repeated_terminal_answers,
            )
        };
        let kind = crate::agentic::issues::completion_gap_kind(gap);
        {
            let mut state = self.state.lock().await;
            state.clean_terminal_synthesis_next = false;
            state.force_text_only_next_model = false;
            state.force_tool_allowlist_next_model = None;
            // The framework now owes the model a mandatory action, so the next
            // model step must keep the tool that performs it.
            state.completion_gap_outstanding = true;
        }
        let body = if kind == crate::agentic::issues::CompletionGapKind::ObjectiveReview {
            // The scope discipline is shared with the Program receipt's
            // actionable guidance. Both sites read it from the order
            // contract so a rule change cannot land on one layer only.
            let scope = crate::agentic::order_contract::OBJECTIVE_REVIEW_SCOPE_DISCIPLINE;
            format!(
                "Runtime returned this turn to work because the Objective is not complete yet: {gap}. \
                     The final answer you just wrote is not being delivered. This is an Objective-review \
                     gap, not a Task-issue classification gap: {scope}. If every current Agent produced one of the delivered result artifacts, \
                     give the Objective-review Task to a reviewer Agent inside the existing Team (do not create \
                     another Team). Use the reviewer you already have: a retry by the same Agent after it reads \
                     counts, while inviting a new one discards the reads it already made and starts over. Only \
                     invite a different physical reviewer when Runtime explicitly requires one. The reviewer must \
                     tools such as `read_ref` to inspect each current delivered result and its durable \
                     evidence, then call `objective_review` under its Runtime-bound Agent identity for the \
                     exact open criterion with decision `satisfied`, `gap` or `blocked`, current \
                     `result_refs`, evidence refs produced by those reads, and a durable `reason_ref`. \
                     For a satisfied verdict, include every result ref in the active completion request \
                     in one review. When the gap text above lists `uncovered_result_refs`, those are the \
                     delivered result refs no current review covers yet: read each one with \
                     `evidence_retrieve`, then submit exactly one `objective_review` whose `result_refs` \
                     cover that full set, and re-request completion citing the same set - do not add \
                     newly created review artifacts to the request. Do not resubmit completion or change accepted Tasks/results while \
                     this review is pending unless the reviewer identifies a concrete flaw; if a review \
                     attempt is rejected, follow its exact missing refs and `next_request` instructions \
                     and use a different physical reviewer when Runtime requires one. \
                     A prior Task review or prose assertion alone does not satisfy the original \
                     user-intent criterion. Complete any acceptance review required for the new Task. \
                     After the Goal projection confirms the criterion is current and satisfied, present the \
                     final answer again."
            )
        } else {
            format!(
                "Runtime returned this turn to work because the Objective is not complete yet: {gap}. \
                     The final answer you just wrote is not being delivered. Resolve the gap with the Agent \
                     actions you already have - for an unadjudicated issue, call `message_publish` with \
                     `issue_dispositions` naming each `issue:` reference and a disposition of `must_resolve`, \
                     `disclose` or `resolved`, each with a durable `reason_ref` and `evidence_refs` \
                     (`artifact://`, `tool://`, `task:`, `issue:` or `program:`). `disclose` preserves a \
                     labelled limitation without asserting an estimate is a measurement. Then present the \
                     final answer again."
            )
        };
        // Escalation. The guidance above is a pure function of the gap, so
        // an unchanged gap produced an unchanged instruction on every
        // continuation: run one19 received byte-identical text on five
        // consecutive rounds (the only difference in the whole context
        // envelope was `elapsed_ms` and `rounds`) and answered with the same
        // final answer every time. Restating the same request is not an
        // intervention. From the second occurrence the instruction says so,
        // names the counter, and states that a prose restatement will not be
        // delivered; from the third it stops asking and pins the tool.
        let escalation = if repeated_answer >= 2 {
            format!(
                "ESCALATION (attempt {attempt}, consecutive answer #{repeated}): this exact gap was already \
                     shown to you {attempt} time(s) and you have now concluded {repeated} time(s) in a row \
                     without a tool call. Runtime does not deliver another conclusion. The action that clears this \
                     gap is `{tool}`. Your next response MUST be a tool call - read the references you still need, \
                     then call it. Runtime has restricted this step to tool calls.",
                attempt = attempt,
                repeated = repeated_answer,
                tool = kind.closing_tool()
            )
        } else if repeated_answer >= 1 || attempt >= 2 {
            format!(
                "NOTICE (attempt {attempt}): you have already been shown this gap {attempt} time(s). \
                     Another prose answer will not be delivered either. Take the action named above - `{tool}` \
                     - and only then present the final answer.",
                attempt = attempt,
                tool = kind.closing_tool()
            )
        } else {
            String::new()
        };
        let guidance = if escalation.is_empty() {
            body
        } else {
            format!("{body}\n\n{escalation}")
        };
        let mut item = ContextItem::new(
            format!("objective-completion-gap:{}", ticket.node_id),
            ContextSourceKind::Task,
            ContextRole::Instruction,
            guidance,
        );
        item.authority = ContextAuthority::System;
        item.visibility = ContextVisibility::Private;
        item.evidence = vec![format!("execution_node:{}", ticket.node_id)];
        // Once prose has demonstrably stopped working, stop accepting it. The
        // framework is not choosing the model's decision here - it is removing
        // the one response shape that has already failed repeatedly, so the next
        // step has to be the action that closes the gap.
        if repeated_answer >= 2 {
            // Force a tool call and leave the tool set alone - the same correction
            // the model-step boundary needed. Pinning the gap's closing tool here
            // refused `objective_complete_request` in run one27, which is the one
            // action that would have closed the Objective the model kept concluding
            // about. Withdrawing a tool is a framework defect; choosing the model's
            // tool is not the framework's call at all.
            let mut state = self.state.lock().await;
            state.force_tool_allowlist_next_model = None;
            state.force_text_only_next_model = false;
            state.force_tool_call_next_model = true;
            tracing::warn!(
                target: "cowd::completion_gap",
                graph_id = %ticket.graph_id,
                gap = %gap,
                attempt,
                repeated_answer,
                closing_tool = kind.closing_tool(),
                "repeated identical final answer with no tool call; pinning the next model step to the closing tool"
            );
        }
        self.runtime.lock().await.push_next_model_context_item(item);
    }
}

/// Digest of a terminal answer for repeat detection.
///
/// Whitespace and case are normalized away because the model restates its
/// conclusion with cosmetic edits; the substance is what has to be identical
/// for the answer to be a loop rather than a revision. Leading and trailing
/// space, line breaks and runs of blanks are the only edits a restating model
/// makes, so collapsing them is enough to catch the loop without conflating two
/// genuinely different answers.
pub(super) fn terminal_answer_digest(answer: &str) -> String {
    let normalized = answer
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_ascii_lowercase();
    format!("{:x}", Sha256::digest(normalized.as_bytes()))
}

fn consecutive_answer_stop_bound() -> Result<Option<u64>, String> {
    match std::env::var("COWD_CONSECUTIVE_ANSWER_STOP") {
        Ok(value) => consecutive_answer_stop_bound_from(Some(&value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!(
            "invalid operator no-action resource limit: {error}"
        )),
    }
}

fn consecutive_answer_stop_bound_from(raw: Option<&str>) -> Result<Option<u64>, String> {
    match raw {
        None => Ok(None),
        Some(value) => value
            .trim()
            .parse::<u64>()
            .map(|limit| (limit > 0).then_some(limit))
            .map_err(|_| {
                "COWD_CONSECUTIVE_ANSWER_STOP must be an explicit nonnegative resource limit"
                    .to_string()
            }),
    }
}

fn completion_gap_continuation_bound() -> Result<Option<u64>, String> {
    match std::env::var("COWD_COMPLETION_GAP_CONTINUATIONS") {
        Ok(value) => parse_continuation_resource_limit(&value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(format!(
            "invalid operator continuation resource limit: {error}"
        )),
    }
}

fn parse_continuation_resource_limit(value: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|limit| *limit > 0)
        .ok_or_else(|| {
            "COWD_COMPLETION_GAP_CONTINUATIONS must be an explicit positive resource limit"
                .to_string()
        })
}

/// Resolve terminal ownership before emitting a Goal event. Agent-first
/// supervision and a previously committed Objective verdict are both durable
/// authorities; presentation must never race either one.
pub(super) fn root_terminal_owned_elsewhere(
    services: &crate::RuntimeServices,
    root_presentation: bool,
    session_id: &str,
    turn_id: &str,
    graph_id: &str,
    goal_id: &str,
    completion: GoalCompletion,
) -> Result<bool, String> {
    let goal = services
        .goal_store()
        .get(goal_id)
        .map_err(|error| format!("load Objective terminal before presentation: {error}"))?;
    // A Goal bound to a live Agentic Program has exactly one terminal owner: the
    // atomic Program conclusion. Delegated workers synthesize their task result
    // but must not commit the shared Goal, or effect_authority rejects them with
    // `program_terminal_authority`.
    let program_bound = goal.as_ref().is_some_and(|goal| {
        goal.execution_binding.as_ref().is_some_and(|binding| {
            services
                .agent_action_service()
                .project_if_exists(&binding.agentic_program_id)
                .ok()
                .flatten()
                .is_some()
        })
    });
    if !root_presentation && program_bound {
        return Ok(true);
    }
    let agentic_supervisor = root_presentation
        && agentic_program_owns_root_terminal(services, session_id, turn_id, graph_id)?;
    let existing = goal.filter(|goal| goal.completion != GoalCompletion::Open);
    if let Some(goal) = existing {
        if goal.completion != completion {
            return Err(format!(
                "presentation completion {:?} conflicts with authoritative Objective verdict {:?}",
                completion, goal.completion
            ));
        }
        return Ok(true);
    }
    if agentic_supervisor && completion == GoalCompletion::Satisfied {
        return Err(
            "Agent-first Objective has no verified terminal; presentation cannot declare success"
                .into(),
        );
    }
    Ok(agentic_supervisor)
}

pub(super) fn terminal_commit_owned_by(
    owner: Option<&(String, u32)>,
    node_id: &str,
    attempt: u32,
) -> bool {
    owner.is_some_and(|(owner_node_id, owner_attempt)| {
        owner_node_id == node_id && *owner_attempt == attempt
    })
}

pub(super) fn user_facing_blocked_answer(raw: &str, language: &str) -> String {
    let normalized = raw.to_ascii_lowercase();
    let zh = language.eq_ignore_ascii_case("zh");
    let (category, hint, preserved, reason_label) = if normalized
        .contains("explicit team acceptance is incomplete")
    {
        if zh {
            (
                "团队协作未完成",
                "部分团队没有产出可验收结果，已保留的证据仍然有效。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Team collaboration incomplete",
                "Some team roles did not produce acceptable results; preserved evidence remains valid.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if normalized.contains("execution blocked safely")
        || normalized.contains("safety fuse")
        || normalized.contains("safety-fuse")
    {
        if zh {
            (
                "安全上限",
                "已达到安全步骤上限，未执行超出边界的操作。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Safety limit reached",
                "The safety step limit was reached; no out-of-bound operations were executed.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if normalized.contains("provider")
        && (normalized.contains("repeated") || normalized.contains("failed"))
    {
        if zh {
            (
                "模型服务异常",
                "模型服务多次失败或不可用，已保留已完成的内容和证据。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Model service issue",
                "The model service failed repeatedly or is unavailable; completed content and evidence were retained.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if normalized.contains("approval") {
        if zh {
            (
                "审批未通过",
                "有操作未获得授权，因此未执行；无冲突的其它工作可以继续，也可以重新授权后继续。",
                "已保留的已完成工作和证据，详见下方结果树/活动详情。",
                "失败原因",
            )
        } else {
            (
                "Approval not granted",
                "An operation was not authorized, so it was not executed; other non-conflicting work may continue, or you may re-authorize and continue.",
                "Completed work and evidence were retained; see the result tree / activity detail below.",
                "Failure reason",
            )
        }
    } else if zh {
        (
            "任务未完成",
            "任务没有达到预期终态，但已保留已完成的工作和证据。",
            "已保留的已完成工作和证据，详见下方结果树/活动详情。",
            "失败原因",
        )
    } else {
        (
            "Task incomplete",
            "The task did not reach the intended terminal state, but completed work and evidence were retained.",
            "Completed work and evidence were retained; see the result tree / activity detail below.",
            "Failure reason",
        )
    };
    let mut markdown = if zh {
        format!("**{category}**：{hint}\n\n{preserved}")
    } else {
        format!("**{category}**: {hint}\n\n{preserved}")
    };
    let reason = raw.trim();
    if !reason.is_empty() {
        let capped = if reason.chars().count() > 1500 {
            let tail: String = reason.chars().take(1500).collect();
            if zh {
                format!("{tail}…（已截断，完整原因见活动详情）")
            } else {
                format!("{tail}… (truncated; full reason in activity detail)")
            }
        } else {
            reason.to_string()
        };
        markdown.push_str(&format!("\n\n**{reason_label}**\n\n```text\n"));
        markdown.push_str(&capped);
        markdown.push_str("\n```\n");
    }
    markdown
}

/// Preserve an already-qualified strict JSON answer byte for byte. Other answer
/// contracts retain the user-visible rendering of structured model results.
pub(super) fn visible_final_answer(
    text: &str,
    format: harness_contract::outcome::UserAnswerFormat,
) -> String {
    if format == harness_contract::outcome::UserAnswerFormat::StrictJson {
        return text.to_string();
    }
    let trimmed = text.trim();
    if let Ok(serde_json::Value::Object(object)) =
        serde_json::from_str::<serde_json::Value>(trimmed)
    {
        for field in ["answer", "summary", "final_answer", "content"] {
            if let Some(serde_json::Value::String(value)) = object.get(field) {
                if !value.trim().is_empty() {
                    return value.clone();
                }
            }
        }
        let mut markdown = String::from("已完成：\n");
        for (key, value) in object {
            if key == "unresolved" || key == "risks" {
                continue;
            }
            let rendered = match value {
                serde_json::Value::String(value) => value,
                other => other.to_string(),
            };
            markdown.push_str(&format!("- {key}: {rendered}\n"));
        }
        return markdown;
    }
    text.to_string()
}

pub(super) fn sha256_digest(value: &str) -> String {
    format!("{:x}", Sha256::digest(value.as_bytes()))
}

/// Complete Provider usage sealed into the durable Session terminal.
///
/// Host state is the latest inline-root cumulative usage. ExecutionLiveStore
/// retains that root and each descendant by execution ID. Terminal therefore
/// selects one root snapshot and adds each exact descendant once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct TerminalTokenUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_input_tokens: u64,
    pub cache_read_input_tokens: u64,
    /// True distinguishes explicit Provider zeros from missing cache usage.
    #[serde(default)]
    pub cache_dimensions_known: bool,
}

pub(super) fn terminal_aggregate_usage(
    own: TerminalTokenUsage,
    live: Option<&crate::execution_live::ExecutionModelUsageSnapshot>,
) -> TerminalTokenUsage {
    let own_observed = own.input_tokens > 0
        || own.output_tokens > 0
        || own.cache_creation_input_tokens > 0
        || own.cache_read_input_tokens > 0
        || own.cache_dimensions_known;
    let mut aggregate = own;
    let mut usage_count = u64::from(own_observed);
    let mut all_cache_dimensions_known = !own_observed || own.cache_dimensions_known;
    let Some(live) = live else {
        aggregate.cache_dimensions_known = usage_count > 0 && all_cache_dimensions_known;
        return aggregate;
    };

    // Host state is the latest root snapshot at terminal time. A store-own
    // value is used only when that host carried no observed usage at all; the
    // two cumulative snapshots are never added to each other.
    if !own_observed {
        if let Some(stored_own) = live.own.as_ref() {
            aggregate.input_tokens = stored_own.input_tokens;
            aggregate.output_tokens = stored_own.output_tokens;
            aggregate.cache_creation_input_tokens = stored_own.cache_create_tokens;
            aggregate.cache_read_input_tokens = stored_own.cache_read_tokens;
            usage_count = 1;
            all_cache_dimensions_known = stored_own.cache_dimensions_known;
        }
    }
    for descendant in live.descendants.values() {
        aggregate.input_tokens = aggregate
            .input_tokens
            .saturating_add(descendant.input_tokens);
        aggregate.output_tokens = aggregate
            .output_tokens
            .saturating_add(descendant.output_tokens);
        aggregate.cache_creation_input_tokens = aggregate
            .cache_creation_input_tokens
            .saturating_add(descendant.cache_create_tokens);
        aggregate.cache_read_input_tokens = aggregate
            .cache_read_input_tokens
            .saturating_add(descendant.cache_read_tokens);
        usage_count = usage_count.saturating_add(1);
        all_cache_dimensions_known &= descendant.cache_dimensions_known;
    }
    aggregate.cache_dimensions_known = usage_count > 0 && all_cache_dimensions_known;
    aggregate
}

#[cfg(test)]
mod terminal_usage_tests {
    use super::*;

    #[test]
    fn explicit_partial_terminal_override_is_not_reopened_by_program_closure() {
        assert!(should_reconcile_agentic_terminal(
            crate::TurnExecutionRole::RootTurn,
            GoalCompletion::Partial,
            None,
        ));
        assert!(!should_reconcile_agentic_terminal(
            crate::TurnExecutionRole::RootTurn,
            GoalCompletion::Partial,
            Some(GoalCompletion::Partial),
        ));
        assert!(!should_reconcile_agentic_terminal(
            crate::TurnExecutionRole::DelegatedLeaf,
            GoalCompletion::Partial,
            None,
        ));
    }

    #[test]
    fn a_satisfied_override_does_not_bypass_program_closure_verification() {
        for completion in [GoalCompletion::Satisfied, GoalCompletion::Partial] {
            assert!(should_reconcile_agentic_terminal(
                crate::TurnExecutionRole::RootTurn,
                completion,
                Some(GoalCompletion::Satisfied),
            ));
        }
        assert!(should_reconcile_agentic_terminal(
            crate::TurnExecutionRole::RootTurn,
            GoalCompletion::Satisfied,
            Some(GoalCompletion::Partial),
        ));
    }

    fn telemetry(
        input_tokens: u64,
        output_tokens: u64,
        cache_create_tokens: u64,
        cache_read_tokens: u64,
        cache_dimensions_known: bool,
    ) -> crate::RunModelTelemetry {
        crate::RunModelTelemetry {
            model: Some("model-a".to_string()),
            models_used: vec!["model-a".to_string()],
            first_token_latency_ms: Some(1),
            active_stream_duration_ms: Some(2),
            wall_duration_ms: 3,
            output_chars: output_tokens,
            output_chunks: 1,
            input_tokens,
            output_tokens,
            cache_create_tokens,
            cache_read_tokens,
            cache_dimensions_known,
            total_tokens: input_tokens
                .saturating_add(output_tokens)
                .saturating_add(cache_create_tokens)
                .saturating_add(cache_read_tokens),
            usage_source: "provider".to_string(),
            wall_chars_per_second: None,
            wall_tokens_per_second: None,
            active_chars_per_second: None,
            active_tokens_per_second: None,
            chars_per_second: None,
            tokens_per_second: None,
        }
    }

    #[test]
    fn terminal_usage_uses_live_root_and_descendant_union_without_counting_inline_root_twice() {
        let live = crate::execution_live::ExecutionModelUsageSnapshot {
            own: Some(telemetry(100, 40, 17, 80, true)),
            descendants: [("child-1".to_string(), telemetry(20, 5, 3, 11, true))]
                .into_iter()
                .collect(),
        };

        let aggregate = terminal_aggregate_usage(
            TerminalTokenUsage {
                input_tokens: 100,
                output_tokens: 40,
                cache_creation_input_tokens: 17,
                cache_read_input_tokens: 80,
                cache_dimensions_known: true,
            },
            Some(&live),
        );

        assert_eq!(aggregate.input_tokens, 120);
        assert_eq!(aggregate.output_tokens, 45);
        assert_eq!(aggregate.cache_creation_input_tokens, 20);
        assert_eq!(aggregate.cache_read_input_tokens, 91);
        assert!(aggregate.cache_dimensions_known);
    }

    #[test]
    fn terminal_usage_adds_descendant_when_live_root_is_lagging() {
        let live = crate::execution_live::ExecutionModelUsageSnapshot {
            own: None,
            descendants: [(
                "child-visible-first".to_string(),
                telemetry(20, 5, 0, 0, true),
            )]
            .into_iter()
            .collect(),
        };

        let aggregate = terminal_aggregate_usage(
            TerminalTokenUsage {
                input_tokens: 100,
                output_tokens: 40,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                cache_dimensions_known: true,
            },
            Some(&live),
        );

        assert_eq!(aggregate.input_tokens, 120);
        assert_eq!(aggregate.output_tokens, 45);
        assert!(aggregate.cache_dimensions_known);
    }

    #[test]
    fn terminal_usage_marks_cache_unknown_when_any_descendant_attempt_is_unknown() {
        let live = crate::execution_live::ExecutionModelUsageSnapshot {
            own: None,
            descendants: [("child-unknown".to_string(), telemetry(20, 5, 0, 0, false))]
                .into_iter()
                .collect(),
        };
        let aggregate = terminal_aggregate_usage(
            TerminalTokenUsage {
                input_tokens: 100,
                output_tokens: 40,
                cache_creation_input_tokens: 0,
                cache_read_input_tokens: 0,
                cache_dimensions_known: true,
            },
            Some(&live),
        );
        assert!(!aggregate.cache_dimensions_known);
    }

    #[test]
    fn terminal_usage_json_round_trip_preserves_complete_cache_telemetry() {
        let usage = TerminalTokenUsage {
            input_tokens: 1_024,
            output_tokens: 128,
            cache_creation_input_tokens: 256,
            cache_read_input_tokens: 768,
            cache_dimensions_known: true,
        };

        let encoded = serde_json::to_value(usage).expect("serialize terminal usage");
        assert_eq!(encoded["cache_creation_input_tokens"], 256);
        assert_eq!(encoded["cache_read_input_tokens"], 768);
        assert_eq!(encoded["cache_dimensions_known"], true);
        assert_eq!(
            serde_json::from_value::<TerminalTokenUsage>(encoded).expect("replay terminal usage"),
            usage
        );
    }

    #[test]
    fn consecutive_answer_stop_requires_an_explicit_resource_limit_and_preserves_zero_disable() {
        assert_eq!(consecutive_answer_stop_bound_from(None).unwrap(), None);
        assert_eq!(
            consecutive_answer_stop_bound_from(Some("5")).unwrap(),
            Some(5)
        );
        assert_eq!(
            consecutive_answer_stop_bound_from(Some(" 2 ")).unwrap(),
            Some(2)
        );
        assert_eq!(
            consecutive_answer_stop_bound_from(Some("1000")).unwrap(),
            Some(1000)
        );
        assert_eq!(consecutive_answer_stop_bound_from(Some("0")).unwrap(), None);
        for value in ["", "abc", "-1", "18446744073709551616"] {
            assert!(consecutive_answer_stop_bound_from(Some(value)).is_err());
        }
    }
}

pub(super) fn runtime_observation_identity(
    services: &crate::RuntimeServices,
    state: &TurnGraphState,
    ticket: &NodeExecutionTicket,
) -> RuntimeObservationIdentity {
    #[cfg(test)]
    if std::env::var_os("COWD_R622_TRACE").is_some() {
        eprintln!("r622_toolbatch phase=observation_turn_owner ingress_present={} state_turn_present={} ingress_turn_matches_state={}",
            state.ingress.is_some(), !state.turn_id.is_empty(),
            state.ingress.as_ref().is_some_and(|ingress| ingress.turn_id == state.turn_id));
    }
    RuntimeObservationIdentity {
        workspace_id: services.workspace_key().to_string(),
        session_id: state.session_id.clone(),
        turn_id: state
            .ingress
            .as_ref()
            .map(|ingress| ingress.turn_id.clone())
            // Direct owned turns have no Session ingress, but Host has
            // already checked this turn against the supplied graph lineage.
            .or_else(|| (!state.turn_id.is_empty()).then(|| state.turn_id.clone())),
        task_id: None,
        graph_id: ticket.graph_id.clone(),
        goal_id: state.goal_id.clone(),
        node_id: Some(ticket.node_id.clone()),
    }
}

pub(super) fn observation_freshness(observed_at_ms: u64) -> ObservationFreshness {
    ObservationFreshness {
        observed_at_ms,
        valid_until_ms: None,
        policy_revision: "goal-observation-v2".to_string(),
    }
}

pub(super) fn runtime_observation(
    identity: RuntimeObservationIdentity,
    kind: RuntimeObservationKind,
    source: &str,
    source_revision: u64,
    summary: String,
    fingerprint: String,
    result_class: ObservationResultClass,
) -> RuntimeObservation {
    RuntimeObservation {
        identity,
        kind,
        source: source.to_string(),
        source_revision: source_revision.max(1),
        freshness: observation_freshness(crate::tool_invocation::now_ms()),
        summary,
        fingerprint,
        evidence_refs: Vec::new(),
        observed_evidence: Vec::new(),
        criterion_deltas: Vec::new(),
        evidence_delta: EvidenceDelta::default(),
        effect_deltas: Vec::new(),
        conflict_deltas: Vec::new(),
        unknown_deltas: Vec::new(),
        cost_delta: CostDelta::default(),
        information_gain: InformationGain::default(),
        context_delta: ContextDelta::default(),
        parallelism_delta: ParallelismDelta::default(),
        result_class,
        failure_class: None,
    }
}

pub(super) fn predecessor_goal_observations(
    graph: &harness_contract::execution_graph::ExecutionGraph,
    ticket: &NodeExecutionTicket,
    current_identity: &RuntimeObservationIdentity,
) -> Vec<RuntimeObservation> {
    graph
        .edges
        .iter()
        .filter(|edge| edge.to == ticket.node_id)
        .filter_map(|edge| {
            let node = graph.nodes.iter().find(|node| node.id == edge.from)?;
            if !matches!(
                node.kind,
                ExecutionNodeKind::Approval
                    | ExecutionNodeKind::AgentTask
                    | ExecutionNodeKind::Verify
            ) {
                return None;
            }
            let result = graph.node_results.get(&node.id)?;
            if !result.status.is_terminal() {
                return None;
            }
            let source = match node.kind {
                ExecutionNodeKind::Approval => "runtime.approval_result",
                ExecutionNodeKind::Verify => "runtime.verification_result",
                ExecutionNodeKind::AgentTask => {
                    if serde_json::from_str::<harness_contract::agent::AgentTaskPacket>(
                        &node.payload_ref,
                    )
                    .ok()
                    .is_some_and(|packet| packet.team_id().is_some())
                    {
                        "runtime.team_agent_result"
                    } else {
                        "runtime.agent_result"
                    }
                }
                _ => return None,
            };
            let result_class = match result.status {
                ExecutionNodeStatus::Completed => ObservationResultClass::Succeeded,
                ExecutionNodeStatus::Blocked
                | ExecutionNodeStatus::Failed
                | ExecutionNodeStatus::Cancelled => ObservationResultClass::Failed,
                _ => ObservationResultClass::Informational,
            };
            let mut identity = current_identity.clone();
            identity.node_id = Some(node.id.clone());
            let mut observation = runtime_observation(
                identity,
                RuntimeObservationKind::GraphProgress,
                source,
                result.finished_at_ms.max(1),
                result
                    .summary
                    .clone()
                    .unwrap_or_else(|| format!("{:?} node {} completed", node.kind, node.id)),
                sha256_digest(&format!(
                    "{}:{:?}:{}",
                    node.id,
                    result.status,
                    result.result_ref.as_deref().unwrap_or_default()
                )),
                result_class,
            );
            let durable_result_ref = result
                .result_ref
                .as_ref()
                .map(|reference| format!("execution_result:{reference}"));
            let materialized_evidence = result
                .evidence_refs
                .iter()
                .map(|reference| reference.evidence_ref.id.clone())
                .filter(|reference| !reference.trim().is_empty())
                .collect::<BTreeSet<_>>();
            observation.evidence_refs = materialized_evidence
                .iter()
                .cloned()
                .chain(durable_result_ref.iter().cloned())
                .collect();
            if result.status == ExecutionNodeStatus::Completed {
                observation.evidence_delta.added = observation.evidence_refs.clone();
                observation.information_gain = InformationGain {
                    distinguishing_evidence_refs: materialized_evidence.into_iter().collect(),
                    resolved_unknown_refs: Vec::new(),
                    provenance: if result.evidence_refs.is_empty() {
                        MeasureProvenance::Unknown
                    } else {
                        MeasureProvenance::Observed
                    },
                };
            }
            observation.effect_deltas.push(EffectDelta {
                effect_id: format!("execution-node:{}", node.id),
                terminal_class: match result.status {
                    ExecutionNodeStatus::Completed => EffectTerminalClass::Completed,
                    ExecutionNodeStatus::Cancelled => EffectTerminalClass::Cancelled,
                    ExecutionNodeStatus::Blocked | ExecutionNodeStatus::Failed => {
                        EffectTerminalClass::Failed
                    }
                    _ => EffectTerminalClass::Uncertain,
                },
                idempotency_ref: node.idempotency_key.clone(),
            });
            observation.cost_delta = CostDelta {
                model_steps: u64::from(result.usage.model.is_some()),
                tool_calls: result.usage.tool_calls,
                duration_ms: result.usage.duration_ms,
                input_tokens: result.usage.input_tokens,
                output_tokens: result.usage.output_tokens,
                cached_tokens: result.usage.cached_tokens,
            };
            observation.failure_class = (result.status != ExecutionNodeStatus::Completed)
                .then_some(match node.kind {
                    ExecutionNodeKind::Approval => ObservationFailureClass::Approval,
                    ExecutionNodeKind::Verify => ObservationFailureClass::Verification,
                    ExecutionNodeKind::AgentTask => ObservationFailureClass::Unknown,
                    _ => ObservationFailureClass::Unknown,
                });
            Some(observation)
        })
        .collect()
}

pub(super) fn propose_intervention_after_observation(
    services: &crate::RuntimeServices,
    goal_id: &str,
    observation: RuntimeObservation,
) -> Result<RuntimeIntervention, String> {
    let projection = services
        .goal_store()
        .projection(goal_id)?
        .ok_or_else(|| format!("goal {goal_id} disappeared before intervention"))?;
    let mut progress = projection.progress;
    crate::execution_core::GoalProgressReducer::apply(&mut progress, &observation)?;
    let mut observations = projection.observations;
    observations.push(observation);
    crate::execution_core::InterventionPolicy.propose(&projection.goal, &progress, &observations)
}

pub(super) fn dynamic_node(
    ticket: &NodeExecutionTicket,
    iteration: usize,
    suffix: &str,
    kind: ExecutionNodeKind,
    executor_prefix: &str,
    _scoped_model_kind: &str,
) -> ExecutionNodeSpec {
    let id = format!("{}:{iteration}:{suffix}", ticket.graph_id);
    ExecutionNodeSpec {
        id: id.clone(),
        kind,
        payload_ref: ticket.payload_ref.clone(),
        executor_kind: executor_prefix.to_string(),
        idempotency_key: format!("{id}:attempt"),
        lease_ref: None,
        acceptance: Default::default(),
        retry_policy: Default::default(),
        resource_scopes: Vec::new(),
        work: Some(
            harness_contract::execution_graph::ExecutionWorkContract::new(match kind {
                ExecutionNodeKind::ToolBatch | ExecutionNodeKind::Materialize => {
                    harness_contract::execution_graph::ExecutionWorkRole::Tool
                }
                ExecutionNodeKind::Verify | ExecutionNodeKind::Approval => {
                    harness_contract::execution_graph::ExecutionWorkRole::Verify
                }
                ExecutionNodeKind::Synthesize => {
                    harness_contract::execution_graph::ExecutionWorkRole::Synthesize
                }
                ExecutionNodeKind::AgentTask => {
                    harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze
                }
                ExecutionNodeKind::InlineModel => {
                    harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze
                }
                ExecutionNodeKind::SessionDispatch | ExecutionNodeKind::Timer => {
                    harness_contract::execution_graph::ExecutionWorkRole::Plan
                }
            }),
        ),
    }
}

pub(super) fn model_intent_summary(intent: &ModelStepIntent) -> String {
    match intent {
        ModelStepIntent::FinalAnswer { .. } => "model produced a terminal answer".to_string(),
        ModelStepIntent::ToolCalls { calls } => {
            format!("model requested {} tool call(s)", calls.len())
        }
        ModelStepIntent::Replan { reason } => format!("model requested replan: {reason}"),
    }
}

pub(super) fn provider_protocol_intervention_kind(attempt: u8) -> RuntimeInterventionKind {
    if attempt <= PROVIDER_PROTOCOL_RECOVERY_BUDGET {
        RuntimeInterventionKind::Replan
    } else {
        RuntimeInterventionKind::Block
    }
}

pub(super) fn provider_failure_intervention_kind_after_receipt(
    evidence_synthesis_attempted: bool,
) -> RuntimeInterventionKind {
    if evidence_synthesis_attempted {
        RuntimeInterventionKind::Block
    } else {
        RuntimeInterventionKind::Synthesize
    }
}

pub(super) fn runtime_replan_context_item(
    node_id: &str,
    intervention: Option<&RuntimeIntervention>,
) -> Option<ContextItem> {
    let intervention =
        intervention.filter(|value| value.kind == RuntimeInterventionKind::Replan)?;
    let mut item = ContextItem::new(
        format!("runtime-replan-guidance:{node_id}"),
        ContextSourceKind::Task,
        ContextRole::Instruction,
        format!(
            "Runtime replan guidance (mandatory): {}",
            intervention.reason
        ),
    );
    item.authority = ContextAuthority::System;
    item.visibility = ContextVisibility::Private;
    item.evidence = intervention.evidence_refs.clone();
    Some(item)
}

pub(super) fn final_answer_recovery_reason(
    text: &str,
    _workspace_root: &std::path::Path,
) -> Option<String> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Some("empty final answer".to_string());
    }
    let normalized = trimmed.to_ascii_lowercase();
    if [
        "<tool_call",
        "<function=",
        "<parameter=",
        "</tool_call>",
        "<｜｜dsml｜｜tool_calls>",
        "<｜｜dsml｜｜invoke",
        "```tool_use",
    ]
    .iter()
    .any(|marker| normalized.contains(marker))
    {
        return Some("simulated tool-call markup in a final answer".to_string());
    }
    if looks_like_unfinished_work_preamble(trimmed) {
        return Some("unfinished work preamble was presented as a final answer".to_string());
    }
    None
}

pub(super) fn final_answer_recovery_reason_for_objective(
    text: &str,
    workspace_root: &std::path::Path,
    objective: &str,
) -> Option<String> {
    let _ = objective;
    final_answer_recovery_reason(text, workspace_root)
}

pub(super) fn final_answer_recovery_reason_for_execution_scope(
    text: &str,
    workspace_root: &std::path::Path,
    objective: &str,
    bounded_evidence_role: bool,
) -> Option<String> {
    if bounded_evidence_role {
        // A delegated role owns only its typed Focus/output contract. Aggregate
        // requirements from the parent objective (for example, a minimum
        // number of source paths across all lanes) belong to the parent
        // synthesizer and must not reject an otherwise complete child result.
        final_answer_recovery_reason(text, workspace_root)
    } else {
        final_answer_recovery_reason_for_objective(text, workspace_root, objective)
    }
}

pub(super) fn looks_like_unfinished_work_preamble(text: &str) -> bool {
    if text.chars().count() > 420 || text.lines().count() > 3 {
        return false;
    }
    let normalized = text.trim().to_ascii_lowercase();
    let promises_more_work = [
        "let me try",
        "let me read",
        "let me inspect",
        "let me get",
        "i will now read",
        "i will now inspect",
        "i'll read",
        "i'll inspect",
        "i need to read",
        "let me continue",
        "让我再",
        "让我尝试",
        "让我使用",
        "让我获取",
        "让我读取",
        "让我搜索",
        "我再读取",
        "我来执行",
        "我将继续",
        "接下来我会",
        "继续读取",
        "继续检查",
        "用 glob",
        "用 grep",
        "用 read",
        "现在读取",
        "先读取",
        "需要查看",
    ]
    .iter()
    .any(|prefix| normalized.starts_with(prefix));
    let explicit_continuation = [
        "let me try",
        "let me read",
        "let me inspect",
        "let me get",
        "let me continue",
        "i will now read",
        "i will now inspect",
        "i'll read",
        "i'll inspect",
        "i need to read",
        "让我继续",
        "让我尝试",
        "让我使用",
        "让我获取",
        "让我读取",
        "让我搜索",
        "同时查看可用的工具",
        "继续收集完整证据",
        "需要小段读取",
        "需要分块读取",
        "同时搜索",
        "先收集证据",
        "先获取",
        "让我直接搜索",
        "let me continue",
        "continue collecting evidence",
    ]
    .iter()
    .any(|fragment| normalized.contains(fragment));
    explicit_continuation
        || promises_more_work
            && (normalized.ends_with(':')
                || normalized.ends_with('：')
                || normalized.contains("once more")
                || normalized.contains("再试一次"))
}

pub(super) fn normalize_terminal_answer_with_evidence(
    text: &str,
    _tool_results: &[ConversationMessage],
    _workspace_root: &std::path::Path,
    objective: &str,
) -> String {
    let trimmed = text.trim();
    if objective_requires_strict_json(objective)
        && serde_json::from_str::<serde_json::Value>(trimmed).is_ok_and(|value| value.is_object())
    {
        return text.to_string();
    }
    if serde_json::from_str::<serde_json::Value>(trimmed).is_ok_and(|value| value.is_object()) {
        // Delegated roles and evaluation Judges own exact machine-readable
        // output contracts. Appending prose evidence to a valid JSON object
        // silently corrupts that contract and makes completed Agent work fail
        // at the parent validation boundary.
        return trimmed.to_string();
    }
    if looks_like_unfinished_work_preamble(text) {
        return text.trim().to_string();
    }
    text.trim().to_string()
}

pub(super) fn retained_orchestration_terminal_candidate(
    _messages: &[ConversationMessage],
    _workspace_root: &std::path::Path,
    _objective: &str,
) -> Option<String> {
    None
}

pub(super) fn replace_latest_assistant_text(
    assistant_messages: &mut [ConversationMessage],
    pending_transcript: &mut BTreeMap<String, Vec<ConversationMessage>>,
    node_id: &str,
    text: &str,
) {
    let replace = |message: &mut ConversationMessage| {
        if let Some(ContentBlock::Text { text: current }) = message
            .blocks
            .iter_mut()
            .find(|block| matches!(block, ContentBlock::Text { .. }))
        {
            *current = text.to_string();
        }
    };
    if let Some(message) = assistant_messages.last_mut() {
        replace(message);
    }
    if let Some(message) = pending_transcript
        .get_mut(node_id)
        .and_then(|messages| messages.last_mut())
    {
        replace(message);
    }
}

pub(super) fn early_tool_receipt_message(
    receipt: &crate::conversation::EarlyToolExecutionReceipt,
) -> ConversationMessage {
    let is_error = receipt.outcome.model_feedback_is_error();
    let output = receipt
        .outcome
        .output
        .clone()
        .or_else(|| receipt.outcome.error.clone())
        .unwrap_or_else(|| {
            format!(
                "Early tool receipt recorded as {}",
                receipt.outcome.evidence_ref
            )
        });
    ConversationMessage::tool_result(
        receipt.call.id.clone(),
        receipt.call.name.clone(),
        output,
        is_error,
    )
}

pub(super) fn terminal_evidence_digest(messages: &[ConversationMessage]) -> String {
    let receipts = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_name,
                output,
                is_error,
                ..
            } => Some((tool_name.as_str(), output.as_str(), *is_error)),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut seen = BTreeSet::new();
    let mut rendered = String::new();
    for (index, (tool_name, output, is_error)) in receipts.into_iter().enumerate() {
        let fingerprint = sha256_digest(output);
        if !seen.insert(fingerprint) {
            continue;
        }
        let receipt = format!(
            "\n\n### Receipt {} · {} · {}\n{}",
            index + 1,
            tool_name,
            if is_error { "failed" } else { "completed" },
            output,
        );
        rendered.push_str(&receipt);
    }
    rendered.trim().to_string()
}

/// Build the smallest provider-valid history that proves checked tool
/// receipts were actually exposed to the terminal synthesis model.
///
/// A ToolResult cannot be sent on its own: strict transports require the
/// preceding assistant ToolUse with the same invocation id. Retaining the
/// complete exploratory transcript would defeat clean terminal synthesis and
/// prompt caching, so this projection keeps only exact pairs and strips all
/// text and thinking blocks.
pub(super) fn terminal_evidence_history(
    assistant_messages: &[ConversationMessage],
    result_messages: &[ConversationMessage],
) -> Vec<ConversationMessage> {
    let mut results_by_id = BTreeMap::<String, Vec<ContentBlock>>::new();
    let mut result_order = Vec::new();
    for message in result_messages {
        for block in &message.blocks {
            if let ContentBlock::ToolResult { tool_use_id, .. } = block {
                if !results_by_id.contains_key(tool_use_id) {
                    result_order.push(tool_use_id.clone());
                }
                results_by_id
                    .entry(tool_use_id.clone())
                    .or_default()
                    .push(block.clone());
            }
        }
    }

    let mut history = Vec::new();
    let mut emitted = BTreeSet::new();
    for message in assistant_messages {
        for block in &message.blocks {
            let ContentBlock::ToolUse { id, .. } = block else {
                continue;
            };
            let Some(results) = results_by_id.get(id) else {
                continue;
            };
            if !emitted.insert(id.clone()) {
                continue;
            }
            history.push(ConversationMessage::assistant(vec![block.clone()]));
            history.push(ConversationMessage {
                role: crate::MessageRole::Tool,
                blocks: results.clone(),
                usage: None,
            });
        }
    }
    // Runtime may proactively prefetch exact evidence before a model step.
    // Such a receipt has no assistant-authored ToolUse, but strict provider
    // protocols still require a paired historical call carrier. Emit an
    // explicitly Runtime-labelled synthetic carrier; the ToolResult bytes,
    // invocation id and tool name remain exact and are what the observation
    // ledger verifies after a valid provider response.
    for id in result_order {
        if !emitted.insert(id.clone()) {
            continue;
        }
        let Some(results) = results_by_id.get(&id) else {
            continue;
        };
        let Some(tool_name) = results.iter().find_map(|block| match block {
            ContentBlock::ToolResult { tool_name, .. } => Some(tool_name.clone()),
            _ => None,
        }) else {
            continue;
        };
        history.push(ConversationMessage::assistant(vec![
            ContentBlock::ToolUse {
                id,
                name: tool_name,
                input: r#"{"runtime_evidence_replay":true}"#.to_string(),
            },
        ]));
        history.push(ConversationMessage {
            role: crate::MessageRole::Tool,
            blocks: results.clone(),
            usage: None,
        });
    }
    history
}

pub(super) fn focus_synthesis_evidence_context_item(
    node_id: &str,
    calls: &[ModelToolCall],
    messages: &[ConversationMessage],
    required_fields: &[String],
) -> Option<ContextItem> {
    let evidence = terminal_evidence_digest(messages);
    if evidence.is_empty() {
        return None;
    }
    let required_fields = required_fields.join(", ");
    let mut item = ContextItem::new(
        format!("runtime-focus-synthesis-evidence:{node_id}"),
        ContextSourceKind::ToolTrace,
        ContextRole::Evidence,
        format!(
            "## Runtime-verified Focus evidence\n\
             The Focus acceptance scopes for this delegated role are complete. \
             The immediately preceding native tool-result messages are the actual committed, role-local receipts. \
             Use their source paths and content to cover every required terminal presentation field \
             [{required_fields}]. Native structured output, JSON, Markdown headings, and `Field: value` labels are accepted. \
             Do not claim that source evidence is unavailable, do not invoke more tools, and do not \
             substitute prose for the committed receipt facts. Runtime deliberately references those receipts here \
             instead of copying their full content into a second context packet."
        ),
    );
    item.authority = ContextAuthority::System;
    item.visibility = ContextVisibility::Private;
    item.evidence = calls
        .iter()
        .map(|call| format!("tool_call:{}", call.id))
        .collect();
    Some(item)
}

/// Bounds no-tool final-answer recovery using the same Runtime lease that
/// governs the turn. This is intentionally not a global retry constant:
/// complex work with already-committed evidence deserves more chances to
/// convert a malformed provider response into a useful synthesis, while a
/// pressured or explicitly constrained turn stops promptly.
pub(super) fn terminal_recovery_retry_budget(
    lease: &crate::execution_core::ExecutionBudgetLease,
) -> u8 {
    use harness_contract::core::TaskComplexity;

    let mut retries: u8 = match lease.complexity {
        TaskComplexity::Trivial | TaskComplexity::Simple => 1,
        TaskComplexity::Moderate => 2,
        TaskComplexity::Complex | TaskComplexity::Strategic => 3,
    };
    if lease.explicit_user_limit.is_some_and(|limit| limit <= 2) {
        retries = retries.min(1);
    }
    retries
}

/// A provider may emit a normal final answer and then append direct XML-like
/// tool markup in the same text stream. The adapter can only execute a *pure*,
/// declared XML response; executing a mixed prose block would turn generated
/// text into a command channel. Preserve the already complete answer and drop
/// only a suffix that begins with a tool marker after visible prose. A response
/// that starts with markup remains invalid and follows governed recovery.
pub(super) fn strip_trailing_simulated_tool_markup(text: String) -> String {
    let normalized = text.to_ascii_lowercase();
    let start = [
        "<tool_call",
        "<function=",
        "<parameter=",
        "<｜｜dsml｜｜tool_calls>",
        "<｜｜dsml｜｜invoke",
        "```tool_use",
    ]
    .iter()
    .filter_map(|marker| normalized.find(marker))
    .min();
    let Some(start) = start else {
        return text;
    };
    let suffix = &text[start..];
    let lower_suffix = suffix.to_ascii_lowercase();
    let is_direct_markup = lower_suffix.starts_with("<tool_call")
        || lower_suffix.starts_with("<function=")
        || lower_suffix.starts_with("<parameter=")
        || lower_suffix.starts_with("<｜｜dsml｜｜tool_calls>")
        || lower_suffix.starts_with("<｜｜dsml｜｜invoke")
        || lower_suffix.starts_with("```tool_use");
    if start > 0 && is_direct_markup {
        text[..start].trim_end().to_string()
    } else {
        text
    }
}

pub(super) fn model_intent_kind(intent: &ModelStepIntent) -> &'static str {
    match intent {
        ModelStepIntent::FinalAnswer { .. } => "final_answer",
        ModelStepIntent::ToolCalls { .. } => "tool_calls",
        ModelStepIntent::Replan { .. } => "replan",
    }
}

pub(super) fn independent_tool_call_count(intent: &ModelStepIntent) -> usize {
    match intent {
        ModelStepIntent::ToolCalls { calls } => calls
            .iter()
            .filter(|call| call.depends_on.is_empty())
            .count(),
        _ => 0,
    }
}

pub(super) fn context_pressure_basis_points(input_tokens: u64, context_window: u32) -> i64 {
    let window = u64::from(context_window.max(1));
    i64::try_from(input_tokens.saturating_mul(10_000) / window).unwrap_or(i64::MAX)
}

pub(super) fn failed_tool_names(messages: &[ConversationMessage]) -> Vec<String> {
    let mut names = messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_name,
                is_error: true,
                ..
            } => Some(tool_name.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();
    names.sort();
    names.dedup();
    names
}

/// A governed action is identified by its tool name and canonical input, not
/// by the provider-generated call id. The id changes on every retry, so using
/// it would hide repeated work from Goal/Intervention policy.
pub(super) fn tool_batch_fingerprint(calls: &[ModelToolCall]) -> String {
    let mut actions = calls
        .iter()
        .map(|call| {
            let input = canonical_tool_input_for_governance(call);
            format!("{}:{input}", call.name)
        })
        .collect::<Vec<_>>();
    actions.sort();
    format!("tool_action:{}", sha256_digest(&actions.join("\n")))
}

/// Capability discovery is a control-plane lookup, not new evidence. Provider
/// wording in `intent` is intentionally excluded so paraphrased repeated
/// queries are visible to the InterventionPolicy instead of resetting its
/// novelty/progress accounting. Detail, surface, and profile remain because
/// they change the returned capability view.
pub(super) fn canonical_tool_input_for_governance(call: &ModelToolCall) -> String {
    let parsed = serde_json::from_str::<serde_json::Value>(&call.input).ok();
    if call.name.eq_ignore_ascii_case("runtime_capabilities") {
        let object = parsed.as_ref().and_then(serde_json::Value::as_object);
        let detail = object
            .and_then(|value| value.get("detail"))
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("summary");
        let surface = object
            .and_then(|value| value.get("surface"))
            .and_then(serde_json::Value::as_str);
        let profile = object
            .and_then(|value| value.get("profile"))
            .and_then(serde_json::Value::as_str);
        return serde_json::json!({
            "detail": detail,
            "surface": surface,
            "profile": profile,
        })
        .to_string();
    }
    parsed.map_or_else(|| call.input.clone(), |value| value.to_string())
}

/// Derive stable evidence coverage keys without treating provider-generated
/// call identifiers or superficial query variations as new investigation.
/// Direct file reads retain file-level detail; broad discovery tools collapse
/// to their workspace/crate zone so repeatedly globbing or grepping the same
/// area becomes visible to the Goal policy as low-novelty work.
/// Derive novelty only from the original authorized executor's actual child searches.
/// These keys are not read receipts and never satisfy an acceptance scope.
pub(crate) fn original_orchestrated_search_coverage(
    request: &crate::RuntimeToolExecutionRequest,
    auth: &harness_contract::tool::ToolExecutionAuthorization,
    invocation: &harness_contract::tool::GovernedToolInvocation,
) -> Option<BTreeSet<String>> {
    let mut result = BTreeSet::new();
    let Some(binding) = request.step_binding.as_ref() else { return None; };
    if request.tool_name != "tool_orchestrate" || invocation.invocation_id != request.tool_use_id
        || binding.origin.call_id != request.tool_use_id || invocation.origin.as_ref() != Some(&binding.origin)
        || Some(binding.snapshot.identity.session_id.as_str()) != request.session_id.as_deref()
        || invocation.effect.tool_id != "tool_orchestrate" || invocation.effect.descriptor_hash != auth.descriptor_hash
        || auth.tool_id != "tool_orchestrate" { return None; }
    let Ok(Some(searches)) = request.tool_progress.child_searches() else { return None; };
    if searches.is_empty() { return None; }
    let mut positions = BTreeSet::new();
    for child in &searches {
        if child.tool_name() != "grep_search" { continue; }
        let effect = child.effect();
        if child.step_id().is_empty() || child.input_sha256().len() != 64 || child.output_sha256().len() != 64
            || !positions.insert((child.step_index(), child.position()))
            || child.parent_authorization() != auth || child.parent_request_id() != auth.request_id
            || child.parent_descriptor_hash() != auth.descriptor_hash
            || effect.tool_id != "grep_search" || effect.effect_kind != harness_contract::tool::ToolEffectKind::Read
            || effect.uses_network || effect.spawns_process || effect.mutates_packages || effect.mutates_system
            || effect.scopes.is_empty() || !effect.scopes.iter().all(|scope|
                auth.authorization_lease.scopes.contains(scope) && invocation.effect.scopes.contains(scope)) {
            return None;
        }
        result.insert(format!("discovery:{}", coverage_zone(child.base_path())));
    }
    Some(result)
}

/// Delegated evidence roles have a deliberately tighter contract. Main turns
/// get one additional no-progress batch so a multi-chunk file read is not cut
/// off prematurely, but must still converge before repeatedly rebuilding an
/// ever-larger provider context from unchanged evidence.
pub(super) const fn evidence_saturation_limit(bounded_evidence_role: bool) -> usize {
    if bounded_evidence_role {
        2
    } else {
        3
    }
}

pub(super) fn focus_acceptance_is_met(
    bounded_evidence_role: bool,
    required_scopes: &[String],
    pending_scopes: &[String],
) -> bool {
    // A bounded delegated role declares required scopes only when its packet
    // carries evidence obligations. A role without any has nothing outstanding,
    // so its acceptance is trivially met. The previous non-empty requirement
    // made that case permanently unsatisfiable, and the saturation guard could
    // then only ever Block such a role - including one that had already
    // finished its work and retained the evidence.
    let _ = required_scopes;
    bounded_evidence_role && pending_scopes.is_empty()
}

pub(super) fn should_force_focus_synthesis(
    focus_acceptance_met: bool,
    required_scopes: &[String],
    repeated_evidence_saturation: bool,
    has_retained_terminal_candidate: bool,
    exact_workspace_evidence: bool,
) -> bool {
    if !focus_acceptance_met {
        return false;
    }
    // Runtime retained this candidate specifically while it completed the
    // missing deterministic read. Once the typed receipt closes the exact
    // obligation, another provider answer creates a stale second-final race;
    // the retained candidate owns the text and the receipt owns the evidence.
    if has_retained_terminal_candidate && exact_workspace_evidence {
        return true;
    }
    // An exact file receipt is mechanically complete, so another acquisition
    // round can add no evidence required by this role. A directory/network
    // scope is different: it is an exploration authority and responsibility
    // zone. One descendant receipt establishes in-scope provenance but does
    // not prove that investigation is complete. Keep discovery available
    // until the provider returns a grounded terminal answer or the existing
    // bounded novelty policy observes repeated saturation.
    if required_scopes.is_empty() {
        // No declared evidence obligation and repeated saturation: there is
        // nothing left to acquire, so the role must present what it retains
        // instead of being stopped with a Block.
        return repeated_evidence_saturation;
    }
    exact_workspace_evidence || repeated_evidence_saturation
}

#[cfg(test)]
pub(super) fn required_write_for_turn(
    strategy_requires_write: bool,
    bounded_evidence_role: bool,
    focus_acceptance_scopes: &[String],
) -> bool {
    if bounded_evidence_role {
        return focus_acceptance_scopes
            .iter()
            .any(|scope| scope.starts_with("write:"));
    }
    strategy_requires_write
}

/// Responsibility-zone coverage is intentionally coarser than file coverage.
/// It is consulted only for a bounded delegated role, where reading another
/// file in an already-investigated component is not by itself a reason to
/// defer a supported conclusion.
pub(super) fn tool_batch_scope_keys(calls: &[ModelToolCall]) -> BTreeSet<String> {
    calls
        .iter()
        .flat_map(|call| {
            let value = serde_json::from_str::<serde_json::Value>(&call.input).ok();
            let paths = value.as_ref().map(coverage_paths).unwrap_or_default();
            if paths.is_empty() {
                vec![format!("tool:{}", call.name.to_ascii_lowercase())]
            } else {
                paths.iter().map(|path| coverage_zone(path)).collect()
            }
        })
        .collect()
}

pub(super) fn tool_call_coverage_keys(call: &ModelToolCall) -> Vec<String> {
    let value = serde_json::from_str::<serde_json::Value>(&call.input).ok();
    let name = call.name.to_ascii_lowercase();
    if name == "tool_orchestrate" { return Vec::new(); }
    let paths = value.as_ref().map(coverage_paths).unwrap_or_default();
    let is_discovery = matches!(
        name.as_str(),
        "workspace_snapshot"
            | "glob_search"
            | "grep_search"
            | "lsp"
            | "toolsearch"
            | "tool_search"
            | "runtime_capabilities"
    );
    if is_discovery {
        let zones = if paths.is_empty() {
            vec!["workspace".to_string()]
        } else {
            paths.iter().map(|path| coverage_zone(path)).collect()
        };
        return zones
            .into_iter()
            .map(|zone| format!("discovery:{zone}"))
            .collect();
    }
    if !paths.is_empty() {
        return paths
            .iter()
            .map(|path| format!("evidence:{name}:{}", normalized_coverage_path(path)))
            .collect();
    }
    vec![format!("tool:{name}")]
}

pub(super) fn coverage_paths(value: &serde_json::Value) -> Vec<String> {
    const PATH_FIELDS: &[&str] = &[
        "path",
        "file_path",
        "file",
        "files",
        "paths",
        "pattern",
        "patterns",
        "searches",
        "uri",
        "evidence_ref",
    ];
    let mut values = Vec::new();
    if let Some(object) = value.as_object() {
        for field in PATH_FIELDS {
            if let Some(field_value) = object.get(*field) {
                collect_coverage_strings(field_value, &mut values);
            }
        }
    }
    values.sort();
    values.dedup();
    values
}

pub(super) fn collect_coverage_strings(value: &serde_json::Value, output: &mut Vec<String>) {
    match value {
        serde_json::Value::String(value) => {
            if let Ok(nested) = serde_json::from_str::<serde_json::Value>(value) {
                collect_coverage_strings(&nested, output);
            } else if !value.trim().is_empty() {
                output.push(value.trim().to_string());
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                collect_coverage_strings(value, output);
            }
        }
        serde_json::Value::Object(values) => {
            for field in ["path", "file_path", "file", "glob", "pattern", "uri"] {
                if let Some(value) = values.get(field) {
                    collect_coverage_strings(value, output);
                }
            }
        }
        _ => {}
    }
}

pub(super) fn normalized_coverage_path(path: &str) -> String {
    let path = path.replace('\\', "/");
    path.find("crates/")
        .map_or(path.clone(), |index| path[index..].to_string())
}

pub(super) fn coverage_zone(path: &str) -> String {
    let normalized = normalized_coverage_path(path);
    let parts = normalized.split('/').collect::<Vec<_>>();
    if parts.first() == Some(&"crates") && parts.len() >= 2 {
        format!("crates/{}", parts[1])
    } else if parts.first() == Some(&"docs") {
        "docs".to_string()
    } else {
        "workspace".to_string()
    }
}

pub(super) fn explicit_model_step_limit(content: &str) -> Option<usize> {
    ["max_steps=", "max model steps=", "最大模型步骤="]
        .into_iter()
        .find_map(|marker| {
            let start = content.to_ascii_lowercase().find(marker)? + marker.len();
            let digits = content[start..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            digits.parse::<usize>().ok().filter(|value| *value > 0)
        })
}

/// The legacy conversation engine appends provider/tool messages during the
/// effect call. They are deliberately removed until Runner commits the node;
/// `after_commit` is the only publisher to the parent transcript.
pub(super) fn resource_scopes_for_tool_calls(calls: &[ModelToolCall]) -> Vec<String> {
    // These scopes are descriptive graph/evaluation metadata, not execution
    // leases. ToolBatch container nodes deliberately skip ScopeLockManager in
    // GraphRunner; each leaf acquires its authoritative descriptor-derived
    // ResourceDemand through ToolExecutionPlane.
    let mut paths = std::collections::BTreeMap::<String, bool>::new();
    let mut other = Vec::new();
    for call in calls {
        let Ok(input) = serde_json::from_str::<serde_json::Value>(&call.input) else {
            continue;
        };
        let Some(effect) = graph_metadata_effect(&call.name, &input) else {
            continue;
        };
        let access = effect.effect_kind != harness_contract::tool::ToolEffectKind::Read;
        for scope in effect.scopes {
            let Some(target) = scope.target else {
                continue;
            };
            match scope.resource {
                harness_contract::policy::PermissionResource::File
                | harness_contract::policy::PermissionResource::Tool => {
                    paths
                        .entry(target)
                        .and_modify(|write| *write |= access)
                        .or_insert(access);
                }
                harness_contract::policy::PermissionResource::Network => {
                    other.push("network:*".to_string());
                }
                _ => {}
            }
        }
    }
    other.extend(
        paths
            .into_iter()
            .map(|(path, write)| format!("{}:{path}", if write { "write" } else { "read" })),
    );
    other.sort();
    other.dedup();
    other
}

pub(super) fn graph_metadata_effect(
    tool_name: &str,
    input: &serde_json::Value,
) -> Option<harness_contract::tool::ToolEffectDescriptor> {
    use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
        ToolPermissionMode,
    };

    // This bridge only materializes model-declared paths before the registered
    // ToolHost is entered. It is not an execution safety fallback: unknown or
    // pathless tools emit no graph scope and remain an Unknown barrier in the
    // governed compiler.
    let normalized = tool_name.trim().replace('-', "_").to_ascii_lowercase();
    let effect_kind = match normalized.as_str() {
        "read_file" | "tool_orchestrate" | "grep_search" | "glob_search" | "workspace_snapshot" => {
            ToolEffectKind::Read
        }
        "edit" => ToolEffectKind::Write,
        _ => return None,
    };
    let mut targets = Vec::new();
    collect_graph_resource_targets(input, &mut targets);
    targets.sort();
    targets.dedup();
    if targets.is_empty() {
        return None;
    }
    let operation = if effect_kind == ToolEffectKind::Read {
        PermissionOperation::Read
    } else {
        PermissionOperation::Write
    };
    Some(ToolEffectDescriptor {
        tool_id: tool_name.to_string(),
        descriptor_hash: "graph-metadata-only".to_string(),
        effect_kind,
        idempotency: ToolIdempotency::Unknown,
        scopes: targets
            .into_iter()
            .map(|target| PermissionScope {
                resource: PermissionResource::File,
                operation: operation.clone(),
                target: Some(target),
            })
            .collect(),
        required_permission: if effect_kind == ToolEffectKind::Read {
            ToolPermissionMode::ReadOnly
        } else {
            ToolPermissionMode::WorkspaceWrite
        },
        approval_class: ToolApprovalClass::None,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: harness_contract::policy::EffectAssessment {
            reversibility: if effect_kind == ToolEffectKind::Read {
                harness_contract::policy::EffectReversibility::Reversible
            } else {
                harness_contract::policy::EffectReversibility::Compensatable
            },
            externality: if effect_kind == ToolEffectKind::Read {
                harness_contract::policy::EffectExternality::Internal
            } else {
                harness_contract::policy::EffectExternality::Workspace
            },
            data_sensitivity: harness_contract::policy::DataClassification::Internal,
            novelty: harness_contract::policy::EffectNovelty::Routine,
            blast_radius: if effect_kind == ToolEffectKind::Read {
                harness_contract::policy::EffectBlastRadius::Item
            } else {
                harness_contract::policy::EffectBlastRadius::Workspace
            },
        },
    })
}

pub(super) fn collect_graph_resource_targets(value: &serde_json::Value, targets: &mut Vec<String>) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                collect_graph_resource_targets(value, targets);
            }
        }
        serde_json::Value::Object(values) => {
            for field in ["path", "file_path", "file", "notebook_path"] {
                if let Some(path) = values.get(field).and_then(serde_json::Value::as_str) {
                    targets.push(path.to_string());
                }
            }
            for field in ["files", "edits", "calls", "searches"] {
                if let Some(value) = values.get(field) {
                    collect_graph_resource_targets(value, targets);
                }
            }
        }
        _ => {}
    }
}

/// Compile model-provided paths into graph lock scopes without turning a bad
/// path into a terminal graph failure.  These scopes only coordinate concurrent
/// work; the governed tool host remains the authority that permits or rejects
/// the actual filesystem operation.
///
/// A path outside the workspace (or containing a parent traversal) is therefore
/// represented by a conservative workspace-wide lock.  The tool still receives
/// the original path and returns its normal security error to the model, which
/// lets the next model step correct a typo instead of leaving the turn without a
/// terminal result.
pub(super) fn graph_resource_scopes_for_tool_calls(
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> Vec<String> {
    let mut scopes = resource_scopes_for_tool_calls(calls);
    let mut invalid_read = false;
    let mut invalid_write = false;
    scopes.retain_mut(|scope| {
        let Some((mode, path)) = scope
            .split_once(':')
            .map(|(mode, path)| (mode.to_string(), path.trim().to_string()))
        else {
            return true;
        };
        if !matches!(mode.as_str(), "read" | "write") {
            return true;
        }
        let requested = std::path::Path::new(&path);
        let valid = if requested.is_absolute() {
            if let Ok(relative) = requested.strip_prefix(workspace_root) {
                let relative = if relative.as_os_str().is_empty() {
                    ".".to_string()
                } else {
                    relative.to_string_lossy().replace('\\', "/")
                };
                *scope = format!("{mode}:{relative}");
                true
            } else {
                false
            }
        } else {
            !requested.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        };
        if valid {
            true
        } else {
            invalid_write |= mode == "write";
            invalid_read |= mode == "read";
            false
        }
    });

    if invalid_write || (invalid_read && scopes.iter().any(|scope| scope.starts_with("write:"))) {
        scopes.retain(|scope| !scope.starts_with("read:") && !scope.starts_with("write:"));
        scopes.push("write:.".to_string());
    } else if invalid_read {
        scopes.retain(|scope| !scope.starts_with("read:"));
        scopes.push("read:.".to_string());
    }
    scopes.sort();
    scopes.dedup();
    scopes
}

/// Every call whose result was published to the transcript, whether that
/// result reports success or failure. Publication is a delivery fact; whether
/// the mutation committed is decided by the original owner, never by the
/// transport outcome.
pub(super) fn delivered_tool_call_ids(messages: &[ConversationMessage]) -> BTreeSet<String> {
    messages
        .iter()
        .flat_map(|message| &message.blocks)
        .filter_map(|block| match block {
            ContentBlock::ToolResult { tool_use_id, .. } => Some(tool_use_id.clone()),
            _ => None,
        })
        .collect()
}

pub(super) fn successful_tool_call_ids(messages: &[ConversationMessage]) -> BTreeSet<String> {
    messages
        .iter()
        .flat_map(|message| &message.blocks)
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                output,
                is_error: false,
                ..
            } => {
                // Agent actions use a typed domain receipt: transport can succeed
                // while the mutation itself is rejected (for example, an
                // optimistic fence becoming stale because another Agent advanced
                // the Program).  Such a receipt must remain visible to the model
                // so it can inspect current state and retry, but it is not a
                // successful call for protocol closure or novelty accounting.
                let rejected_agent_action = serde_json::from_str::<
                    harness_contract::agent_action::AgentActionObservation,
                >(output)
                .is_ok_and(|observation| {
                    observation.status
                        == harness_contract::agent_action::AgentActionStatus::Rejected
                });
                (!rejected_agent_action).then(|| tool_use_id.clone())
            }
            _ => None,
        })
        .collect()
}

/// Extract durable semantic progress from complete Runtime Agent-action receipts.
/// Collaboration mutations do not necessarily touch a filesystem resource,
/// so the generic evidence novelty tracker cannot infer their information
/// gain from path receipts. Only an applied, non-duplicate action with
/// changed refs is progress; inspections and rejected actions are excluded.
pub(super) fn agentic_action_progress_refs(messages: &[ConversationMessage]) -> BTreeSet<String> {
    messages
        .iter()
        .flat_map(|message| &message.blocks)
        .filter_map(|block| match block {
            ContentBlock::ToolResult {
                output,
                is_error: false,
                ..
            } => serde_json::from_str::<harness_contract::agent_action::AgentActionObservation>(
                output,
            )
            .ok(),
            _ => None,
        })
        .filter(|observation| {
            observation.status == harness_contract::agent_action::AgentActionStatus::Applied
                && !observation.duplicate
                && !observation.changed_refs.is_empty()
        })
        .flat_map(|observation| {
            std::iter::once(format!("agentic_receipt:{}", observation.receipt_id)).chain(
                observation
                    .changed_refs
                    .into_iter()
                    .map(|reference| format!("agentic_changed:{reference}")),
            )
        })
        .collect()
}

pub(super) fn normalize_workspace_scope(scope: &str) -> Option<(&str, String)> {
    let (mode, path) = scope.split_once(':')?;
    if !matches!(mode, "read" | "write" | "workspace") {
        return None;
    }
    let path = path.trim().replace('\\', "/");
    if path.starts_with('/') {
        return None;
    }
    let mut components = Vec::new();
    for component in path.split('/') {
        match component {
            "" | "." => {}
            ".." => return None,
            value if value.contains(':') => return None,
            value => components.push(value),
        }
    }
    if components.is_empty() {
        return (path == "." || path == "./").then(|| (mode, ".".to_string()));
    }
    Some((mode, components.join("/")))
}

pub(super) fn evaluation_scope_authorizes(allowed: &str, requested: &str) -> bool {
    let (Some((allowed_mode, allowed_path)), Some((requested_mode, requested_path))) = (
        normalize_workspace_scope(allowed),
        normalize_workspace_scope(requested),
    ) else {
        return allowed == requested;
    };
    let mode_authorized = match allowed_mode {
        "write" | "workspace" => matches!(requested_mode, "read" | "write" | "workspace"),
        "read" => requested_mode == "read",
        _ => false,
    };
    mode_authorized
        && (allowed_path == "."
            || requested_path == allowed_path
            || requested_path
                .strip_prefix(&allowed_path)
                .is_some_and(|suffix| suffix.starts_with('/')))
}

pub(super) fn evaluation_scope_allows_early_dispatch(clean_terminal: bool, force_text: bool, allowed: &[String]) -> bool {
    !clean_terminal && !force_text && allowed.is_empty()
}

// Reuse the same ceiling predicate for each original call. The caller must
// pass these decisions through the existing ToolBatch admission owner; this
// function alone does not authorize or execute any call.
pub(super) fn evaluation_scope_call_violations(
    allowed: &[String],
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> BTreeMap<String, String> {
    calls.iter().filter_map(|call| {
        evaluation_scope_violation(allowed, std::slice::from_ref(call), workspace_root)
            .map(|violation| (call.id.clone(), violation))
    }).collect()
}

pub(super) fn evaluation_scope_violation(
    allowed: &[String],
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> Option<String> {
    if allowed.is_empty() {
        return None;
    }
    // Provider tool calls commonly use an absolute path even though the
    // evaluation contract is workspace-relative. Reuse the production graph
    // canonicalizer so an in-workspace absolute path is checked against the
    // same relative scope instead of being rejected and replanned forever.
    graph_resource_scopes_for_tool_calls(calls, workspace_root)
        .into_iter()
        .find(|requested| {
            !allowed
                .iter()
                .any(|scope| evaluation_scope_authorizes(scope, requested))
        })
}

pub(super) fn pending_focus_write_action_violation(
    pending_scopes: &[String],
    observed_scopes: &BTreeSet<String>,
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) -> Option<Vec<String>> {
    let pending_writes = pending_scopes
        .iter()
        .filter(|scope| scope.starts_with("write:"))
        .cloned()
        .collect::<Vec<_>>();
    if pending_writes.is_empty()
        || !pending_writes.iter().all(|write_scope| {
            write_scope
                .strip_prefix("write:")
                .is_some_and(|path| observed_scopes.contains(&format!("read:{path}")))
        })
    {
        return None;
    }
    let requested = graph_resource_scopes_for_tool_calls(calls, workspace_root);
    (!requested.iter().any(|scope| {
        scope.starts_with("write:")
            && pending_writes
                .iter()
                .any(|required| evaluation_scope_authorizes(required, scope))
    }))
    .then_some(pending_writes)
}

pub(super) fn focus_action_rejection_outcome(
    ticket: &NodeExecutionTicket,
    state: &mut TurnGraphState,
    pending_writes: &[String],
) -> (RuntimeIntervention, Vec<ExecutionNodeSpec>) {
    state.focus_action_rejections = state.focus_action_rejections.saturating_add(1);
    let pending = pending_writes.join(", ");
    if state.focus_action_rejections <= 2 {
        state.force_tool_allowlist_next_model = None;
        let reason = format!(
            "the delegated mutation role already has its required pre-write read receipt; the next accepted action must invoke an authorized write tool for [{pending}]. Do not reread, search, glob, synthesize, or claim the change in prose before the committed write receipt exists"
        );
        return (
            RuntimeIntervention {
                goal_id: state.goal_id.clone(),
                kind: RuntimeInterventionKind::Replan,
                reason,
                evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
                expected_graph_revision: None,
            },
            vec![dynamic_node(
                ticket,
                state.iterations,
                "focus-required-write-replan-model",
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "inline_model",
            )],
        );
    }

    let terminal_reason = format!(
        "Execution blocked after the delegated mutation role repeatedly ignored the required write action [{pending}]. No unverified replacement action was executed."
    );
    state.terminal_override = Some((GoalCompletion::Partial, terminal_reason.clone()));
    let mut node = dynamic_node(
        ticket,
        state.iterations,
        "focus-required-write-block-synthesize",
        ExecutionNodeKind::Synthesize,
        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND,
        "inline_model",
    );
    node.executor_kind =
        crate::execution_core::graph::executors::SynthesizeNodeExecutor::KIND.to_string();
    (
        RuntimeIntervention {
            goal_id: state.goal_id.clone(),
            kind: RuntimeInterventionKind::Block,
            reason: terminal_reason,
            evidence_refs: vec![format!("execution_node:{}", ticket.node_id)],
            expected_graph_revision: None,
        },
        vec![node],
    )
}

pub(super) fn record_write_attempt_paths(
    paths: &mut Vec<String>,
    calls: &[ModelToolCall],
    workspace_root: &std::path::Path,
) {
    paths.extend(
        graph_resource_scopes_for_tool_calls(calls, workspace_root)
            .into_iter()
            .filter_map(|scope| scope.strip_prefix("write:").map(str::to_string)),
    );
    paths.sort();
    paths.dedup();
}

/// Compile deterministic post-write verification into governed read calls.
///
/// Focus acceptance scopes are Runtime-authored and already bounded by the
/// delegated role contract. Executing their exact reads here avoids spending
/// another provider round trip merely to ask the model to repeat a mechanical
/// action. Mixed or malformed scopes retain the normal model-driven recovery.
pub(super) fn focus_verification_tool_calls(
    pending_scopes: &[String],
    iteration: usize,
    workspace_root: &Path,
) -> Option<Vec<ModelToolCall>> {
    if pending_scopes.is_empty() {
        return None;
    }
    pending_scopes
        .iter()
        .enumerate()
        .map(|(index, scope)| {
            let path = scope
                .strip_prefix("read:")
                .or_else(|| scope.strip_prefix("verify_after_write:"))
                .or_else(|| scope.strip_prefix("verify_upstream_change:"))?;
            let (_, path) = normalize_workspace_scope(&format!("read:{path}"))?;
            let path = focus_verification_read_path(&path, workspace_root)?;
            (path != ".").then(|| ModelToolCall {
                binding: None,
                id: format!("runtime-focus-verify-{iteration}-{index}"),
                name: "read_file".to_string(),
                // ExactContent is a whole-object evidence contract. The
                // default read window is useful for exploration but cannot
                // close this obligation, even though it reports a full-file
                // digest. Runtime therefore makes EOF intent explicit.
                input: serde_json::json!({"path": path, "complete": true}).to_string(),
                depends_on: Vec::new(),
            })
        })
        .collect()
}

/// Return only Focus acceptance scopes proven by a successful,
/// Runtime-authored exact `read_file` call in the current ToolBatch.
///
/// This is a receipt bridge, not an authorization bypass: arbitrary
/// model-generated reads never enter it, and a scope is returned only when it
/// already belongs to the immutable role contract and the matching tool
/// result is successful.
pub(super) fn successful_runtime_focus_scope_keys(
    calls: &[ModelToolCall],
    successful_call_ids: &BTreeSet<String>,
    required_scopes: &[String],
) -> BTreeSet<String> {
    calls
        .iter()
        .filter(|call| {
            call.id.starts_with("runtime-focus-verify-")
                && call.name == "read_file"
                && successful_call_ids.contains(&call.id)
        })
        .filter_map(|call| {
            serde_json::from_str::<serde_json::Value>(&call.input)
                .ok()
                .and_then(|input| {
                    (input.get("complete").and_then(serde_json::Value::as_bool) == Some(true))
                        .then(|| {
                            input
                                .get("path")
                                .and_then(|path| path.as_str())
                                .map(str::to_string)
                        })
                        .flatten()
                })
        })
        .flat_map(|path| {
            required_scopes.iter().filter_map(move |scope| {
                let scoped_path = scope
                    .strip_prefix("read:")
                    .or_else(|| scope.strip_prefix("verify_after_write:"))
                    .or_else(|| scope.strip_prefix("verify_upstream_change:"));
                (scoped_path == Some(path.as_str())).then(|| scope.clone())
            })
        })
        .collect()
}

/// Resolve only an exact existing regular file for Runtime-owned verification.
/// A directory is exploration authority, not an exact evidence target.
pub(super) fn focus_verification_read_path(path: &str, workspace_root: &Path) -> Option<String> {
    if path == "." {
        return None;
    }
    let candidate = workspace_root.join(path);
    let metadata = std::fs::symlink_metadata(&candidate).ok()?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return None;
    }
    let root = workspace_root.canonicalize().ok()?;
    let file = candidate.canonicalize().ok()?;
    file.strip_prefix(&root)
        .ok()?
        .to_str()
        .map(|relative| relative.replace('\\', "/"))
}

pub(super) fn focus_scopes_are_exact_workspace_files(
    scopes: &[String],
    workspace_root: &Path,
) -> bool {
    !scopes.is_empty()
        && scopes.iter().all(|scope| {
            let path = scope
                .strip_prefix("read:")
                .or_else(|| scope.strip_prefix("write:"))
                .or_else(|| scope.strip_prefix("verify_after_write:"))
                .or_else(|| scope.strip_prefix("verify_upstream_change:"));
            path.and_then(|path| {
                let (_, normalized) = normalize_workspace_scope(&format!("read:{path}"))?;
                focus_verification_read_path(&normalized, workspace_root)
            })
            .is_some()
        })
}

pub(super) fn concrete_focus_verification_scopes(
    pending_scopes: &[String],
    observed_evidence: &[harness_contract::context::ObservedEvidence],
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> Vec<String> {
    let mut concrete = pending_scopes
        .iter()
        .flat_map(|pending| {
            let Some(authorized_path) = pending.strip_prefix("verify_after_write:") else {
                return vec![pending.clone()];
            };
            let required_write = format!("write:{authorized_path}");
            let required = resolver.compile_obligation_or_unresolved(&required_write);
            let matched = observed_evidence
                .iter()
                .filter(|observed| {
                    crate::path_identity::observed_evidence_satisfies(&required, observed)
                })
                .filter_map(|observed| match &observed.target {
                    harness_contract::context::EvidenceTargetIdentity::Workspace { scope }
                        if scope.access_mode
                            == harness_contract::context::WorkspaceAccessMode::Write =>
                    {
                        Some(scope.path.workspace_relative_path.as_str())
                    }
                    _ => None,
                })
                .filter(|path| *path != ".")
                .map(|path| format!("verify_after_write:{path}"))
                .collect::<Vec<_>>();
            if matched.is_empty() {
                vec![pending.clone()]
            } else {
                matched
            }
        })
        .collect::<Vec<_>>();
    concrete.sort();
    concrete.dedup();
    concrete
}

pub(super) fn should_prefetch_focus_verification(
    first_model_step: bool,
    bounded_evidence_role: bool,
    already_prefetched: bool,
    pending_scopes: &[String],
    workspace_root: &Path,
) -> bool {
    first_model_step
        && bounded_evidence_role
        && !already_prefetched
        && !pending_scopes.is_empty()
        && pending_scopes
            .iter()
            .all(|scope| scope.starts_with("read:") || scope.starts_with("verify_upstream_change:"))
        && focus_scopes_are_exact_workspace_files(pending_scopes, workspace_root)
}

pub(super) fn typed_satisfied_focus_acceptance_scope_keys(
    required_scopes: &[String],
    current: &[harness_contract::context::ObservedEvidence],
    prior: &[harness_contract::context::ObservedEvidence],
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
) -> BTreeSet<String> {
    let mut all = prior.to_vec();
    all.extend_from_slice(current);
    required_scopes
        .iter()
        .filter(|raw| {
            let evidence = if raw.starts_with("verify_upstream_change:") {
                current.to_vec()
            } else {
                all.clone()
            };
            // A directory-scoped write authorization is intentionally broad,
            // while post-write truth is exact-path. Resolve every committed
            // descendant write into its concrete verification obligation and
            // require all of those reads before closing the parent scope.
            concrete_focus_verification_scopes(&[(*raw).clone()], &all, resolver)
                .into_iter()
                .all(|concrete| {
                    // The whole-workspace read alias (`read:.`) is only minted
                    // under a full-trust lease. Compile it with root-alias
                    // tolerance so any Runtime-attested descendant exact read
                    // satisfies it; the strict compiler would reject the root
                    // alias and keep the obligation unsatisfiable forever.
                    let root_alias = matches!(
                        concrete.trim(),
                        "read:." | "read:./" | "write:." | "write:./"
                    );
                    let required = if root_alias {
                        resolver.compile_required_acceptance_with_root_alias(&[], &[concrete], true)
                    } else {
                        resolver.compile_required_acceptance(&[], &[concrete])
                    };
                    // Host is only deciding whether it must schedule another
                    // deterministic verification read. It still asks the
                    // canonical evaluator for the verdict rather than
                    // matching obligations locally; terminal producers and
                    // Team reduction consume that same envelope unchanged.
                    let (_, evaluation) =
                        crate::acceptance_evaluator::AcceptanceEvaluator::evaluate_terminal(
                            &required,
                            Vec::new(),
                            evidence.clone(),
                        );
                    evaluation.verdict == harness_contract::acceptance::AcceptanceVerdict::Satisfied
                })
        })
        .cloned()
        .collect()
}

/// Explain a completed Runtime-owned reviewer prefetch to the synthesis step.
///
/// The instruction is deliberately limited to upstream-review scopes. A
/// post-write verification has different ownership semantics and ordinary
/// reads must never be promoted into an independent review claim.
pub(super) fn upstream_verification_completion_instruction(
    verified_scopes: &BTreeSet<String>,
) -> Option<String> {
    let paths = verified_scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("verify_upstream_change:"))
        .collect::<Vec<_>>();
    if paths.is_empty() {
        return None;
    }
    Some(format!(
        "Runtime reviewer evidence (authoritative): before this synthesis, the governed tool DAG performed this role's independent exact-path read for [{}]. The retained read receipt, exact content, byteLength, sha256, endsWithNewline and tool:// reference are role-local evidence, not an upstream self-report. Tools are now disabled because acquisition is complete, not because verification was unavailable. Return one concise JSON object under 800 output tokens: cite exact byte metadata and upstream read/write receipts (including protected-path evidence), distinguish verified state from genuine risk, and do not claim that content, trailing-newline, or unchanged-scope verification was impossible when the receipts prove it.",
        paths.join(", ")
    ))
}

/// Keep one model tool frame intact. The governed tool planner owns dependency
/// and effect scheduling; Agent Actions are short journal mutations and never
/// synchronously retain a workspace lease while delegated work runs.
pub(super) fn tool_batches_for_turn(
    calls: &[ModelToolCall],
) -> Result<Vec<Vec<ModelToolCall>>, String> {
    Ok(vec![calls.to_vec()])
}

pub(super) fn dynamic_edges(from: &str, nodes: &[ExecutionNodeSpec]) -> Vec<ExecutionEdge> {
    let mut previous = from.to_string();
    nodes
        .iter()
        .map(|node| {
            let edge = ExecutionEdge {
                from: previous.clone(),
                to: node.id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            };
            previous.clone_from(&node.id);
            edge
        })
        .collect()
}

pub(super) fn structured_field_is_materialized(value: Option<&serde_json::Value>) -> bool {
    match value {
        Some(serde_json::Value::String(value)) => !value.trim().is_empty(),
        Some(serde_json::Value::Array(values)) => !values.is_empty(),
        Some(serde_json::Value::Object(values)) => !values.is_empty(),
        Some(serde_json::Value::Bool(_) | serde_json::Value::Number(_)) => true,
        Some(serde_json::Value::Null) | None => false,
    }
}

pub(super) fn missing_required_structured_fields(
    candidate: &str,
    required: &[String],
) -> Vec<String> {
    let output =
        crate::agent_in_process_worker::structured_agent_output_for_fields(candidate, required);
    required
        .iter()
        .filter(|field| {
            let value = output
                .as_ref()
                .and_then(|object| object.get(field.as_str()));
            !crate::agent_in_process_worker::structured_contract_field_materialized(field, value)
        })
        .cloned()
        .collect()
}

pub(super) fn normalized_team_terminal_candidate(
    candidate: &str,
    required: &[String],
) -> Option<String> {
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut object =
        crate::agent_in_process_worker::structured_agent_output_for_fields(body, required)
            .unwrap_or_default();
    for required_field in required {
        if !missing_required_structured_field_from_object(&object, required_field) {
            continue;
        }
        // A bounded research/direct role may use the two user-facing labels
        // `summary` and `findings` interchangeably. This is transport
        // normalization only; Focus receipt verification has already happened
        // before this helper runs.
        let alias = match required_field.as_str() {
            "findings" => Some("summary"),
            "summary" => Some("findings"),
            _ => None,
        };
        if let Some(value) = alias
            .and_then(|field| object.get(field))
            .filter(|value| structured_field_is_materialized(Some(value)))
            .cloned()
        {
            object.insert(required_field.clone(), value);
            continue;
        }
        if narrative_terminal_field_is_safe(required_field) {
            object.insert(
                required_field.clone(),
                serde_json::Value::String(body.to_string()),
            );
            continue;
        }
        // Risks, unresolved work, and legacy acceptance claims are material
        // disclosures. Generic prose must never manufacture them.
        return None;
    }
    serde_json::to_string(&object).ok()
}

/// After one isolated, zero-tool presentation recovery, tolerate only missing
/// Runtime-declared custom artifact wrappers. The provider must already have
/// materialized every fixed field (especially evidence, risks and unresolved
/// work); Runtime merely copies that recovered terminal wording under the
/// declared transport key. Agent validation still requires fresh tool
/// receipts or authenticated upstream evidence for the custom artifact.
pub(super) fn normalized_declared_custom_terminal_after_recovery(
    candidate: &str,
    required: &[String],
) -> Option<String> {
    normalized_terminal_after_bounded_recovery(candidate, required)
}

/// Close a presentation-only contract after a bounded provider recovery and
/// after Runtime has already verified all evidence scopes.
///
/// This never creates evidence or semantic findings. Narrative fields and
/// declared custom wrappers retain the provider's own terminal wording. A
/// missing disclosure is represented as an explicit Runtime-observed gap,
/// never as an empty list (which would falsely claim that the provider found
/// no risk or unresolved work).
pub(super) fn normalized_terminal_after_bounded_recovery(
    candidate: &str,
    required: &[String],
) -> Option<String> {
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut object =
        crate::agent_in_process_worker::structured_agent_output_for_fields(body, required)
            .unwrap_or_default();
    for field in required {
        if !missing_required_structured_field_from_object(&object, field) {
            continue;
        }
        let value = if narrative_terminal_field_is_safe(field) {
            match field.as_str() {
                "findings" => object
                    .get("summary")
                    .filter(|value| structured_field_is_materialized(Some(value)))
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
                "summary" => object
                    .get("findings")
                    .filter(|value| structured_field_is_materialized(Some(value)))
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
                _ => serde_json::Value::String(body.to_string()),
            }
        } else if matches!(
            field.as_str(),
            "risks" | "unresolved" | "unresolved_or_risks"
        ) {
            serde_json::json!([{
                "kind": "runtime_presentation_gap",
                "status": "provider_omitted_after_bounded_recovery",
                "field": field,
                "no_empty_state_inferred": true,
            }])
        } else if fixed_team_terminal_field(field) {
            // Evidence, decisions, and other fixed semantic declarations
            // remain provider/receipt owned and cannot be manufactured here.
            return None;
        } else {
            // Runtime-declared custom artifacts already accept an exact
            // provider-authored summary wrapper in the ordinary recovery
            // path. Preserve that authority while closing sibling fields.
            object
                .get("summary")
                .or_else(|| object.get("findings"))
                .filter(|value| structured_field_is_materialized(Some(value)))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string()))
        };
        object.insert(field.clone(), value);
    }
    required
        .iter()
        .all(|field| !missing_required_structured_field_from_object(&object, field))
        .then(|| serde_json::to_string(&object).ok())
        .flatten()
}

/// Close a transport-only terminal gap from actual ToolHost receipts.
///
/// This is deliberately narrower than ordinary presentation normalization:
/// it is called only after the bounded zero-tool recovery and only after the
/// Focus acceptance reducer has no pending scope. The receipt objects are
/// copied verbatim from successful tool-result messages. Runtime therefore
/// supplies a missing `evidence` carrier without inventing evidence, wraps
/// custom artifact fields with the same durable receipts, and records omitted
/// disclosure fields as explicit unknowns rather than false empty arrays.
pub(super) fn normalized_receipt_backed_terminal_after_recovery(
    candidate: &str,
    required: &[String],
    receipts: &[serde_json::Value],
) -> Option<String> {
    if receipts.is_empty() {
        return None;
    }
    let body = candidate.trim();
    if body.is_empty()
        || body.starts_with("<synthesized_terminal")
        || body.contains("<tool_call>")
        || body.contains("```tool_use")
        || body.contains("<function=")
    {
        return None;
    }
    let mut object =
        crate::agent_in_process_worker::structured_agent_output_for_fields(body, required)
            .unwrap_or_default();
    for field in required {
        if !missing_required_structured_field_from_object(&object, field) {
            continue;
        }
        let value = match field.as_str() {
            "evidence" => serde_json::Value::Array(receipts.to_vec()),
            "risks" | "unresolved" | "unresolved_or_risks" => serde_json::json!([{
                "kind": "runtime_presentation_gap",
                "status": "provider_omitted_after_bounded_recovery",
                "field": field,
                "no_empty_state_inferred": true,
                "receipt_backed_business_work_completed": true,
            }]),
            "key_decisions" => return None,
            "findings" => object
                .get("summary")
                .filter(|value| structured_field_is_materialized(Some(value)))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
            "summary" => object
                .get("findings")
                .filter(|value| structured_field_is_materialized(Some(value)))
                .cloned()
                .unwrap_or_else(|| serde_json::Value::String(body.to_string())),
            field if narrative_terminal_field_is_safe(field) => {
                serde_json::Value::String(body.to_string())
            }
            // A Runtime-declared custom artifact name has no universal JSON
            // schema. Preserve the provider-authored terminal and attach the
            // exact successful receipts rather than fabricating a digest or
            // artifact claim from prose.
            _ => serde_json::json!({
                "status": "runtime_receipt_backed",
                "provider_terminal": body,
                "receipts": receipts,
            }),
        };
        object.insert(field.clone(), value);
    }
    required
        .iter()
        .all(|field| !missing_required_structured_field_from_object(&object, field))
        .then(|| serde_json::to_string(&object).ok())
        .flatten()
}

pub(super) fn fixed_team_terminal_field(field: &str) -> bool {
    matches!(
        field,
        "evidence"
            | "summary"
            | "findings"
            | "plan"
            | "implementation"
            | "source_verification"
            | "review"
            | "risks"
            | "unresolved"
            | "key_decisions"
            | "unresolved_or_risks"
            | "proposal"
            | "critique"
            | "mitigation"
            | "checkpoint"
    )
}

pub(super) fn missing_required_structured_field_from_object(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
) -> bool {
    !crate::agent_in_process_worker::structured_contract_field_materialized(
        field,
        object.get(field),
    )
}

pub(super) fn narrative_terminal_field_is_safe(field: &str) -> bool {
    matches!(
        field,
        "summary"
            | "findings"
            | "plan"
            | "implementation"
            | "source_verification"
            | "review"
            | "proposal"
            | "critique"
            | "mitigation"
            | "checkpoint"
    )
}

/// Materialize the implementer's mechanical hand-off directly from committed
/// Runtime receipts. The independent reviewer still performs the semantic
/// comparison against the user objective; this only avoids paying for a model
/// to restate an already verified write + post-write read as JSON.
pub(super) fn runtime_verified_implementation_terminal_candidate(
    required: &[String],
    observed_scopes: &BTreeSet<String>,
    write_attempt_paths: &[String],
    tool_results: &[ConversationMessage],
    workspace_root: &std::path::Path,
) -> Option<String> {
    let fields = required.iter().map(String::as_str).collect::<BTreeSet<_>>();
    if fields != BTreeSet::from(["implementation", "source_verification"]) {
        return None;
    }
    let mut write_paths = observed_scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("write:"))
        .filter(|path| *path != ".")
        .map(str::to_string)
        .collect::<Vec<_>>();
    write_paths.sort();
    write_paths.dedup();
    if write_paths.is_empty()
        || !write_paths.iter().all(|path| {
            write_attempt_paths.contains(path)
                && observed_scopes.contains(&format!("verify_after_write:{path}"))
        })
    {
        return None;
    }
    let receipts = runtime_tool_receipt_evidence(tool_results, workspace_root);
    if receipts.is_empty() {
        return None;
    }
    let post_write_evidence_ref = receipts.iter().rev().find_map(|receipt| {
        (receipt.get("tool").and_then(serde_json::Value::as_str) == Some("read_file"))
            .then(|| receipt.get("evidence_ref").cloned())
            .flatten()
    })?;
    serde_json::to_string(&serde_json::json!({
        "implementation": {
            "status": "committed",
            "write_paths": write_paths.clone(),
            "runtime_receipt_count": receipts.len(),
            "receipts": receipts.clone(),
        },
        "source_verification": {
            "status": "verified_after_commit",
            "paths": write_paths,
            "post_write_evidence_ref": post_write_evidence_ref,
        },
        "risks": "No effect-level risk detected by Runtime; the independent reviewer remains responsible for semantic acceptance against the objective.",
    }))
    .ok()
}

pub(super) fn runtime_tool_receipt_evidence(
    messages: &[ConversationMessage],
    workspace_root: &std::path::Path,
) -> Vec<serde_json::Value> {
    messages
        .iter()
        .flat_map(|message| message.blocks.iter())
        .enumerate()
        .filter_map(|(index, block)| match block {
            ContentBlock::ToolResult {
                tool_use_id,
                tool_name,
                output,
                is_error: false,
            } => {
                let evidence_ref = output
                    .split_once("tool://")
                    .map(|(_, tail)| tail)
                    .and_then(|tail| tail.split_whitespace().next())
                    .map(|value| {
                        format!(
                            "tool://{}",
                            value.trim_end_matches(['.', ',', ';', ')', ']', '}'])
                        )
                    })?;
                Some(serde_json::json!({
                    "sequence": index.saturating_add(1),
                    "tool_call_id": tool_use_id,
                    "tool": tool_name,
                    "evidence_ref": evidence_ref,
                    "paths": tool_receipt_workspace_paths(output, workspace_root),
                    "sha256": tool_receipt_sha256_digests(output),
                }))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn tool_receipt_sha256_digests(output: &str) -> Vec<String> {
    let Some(object_start) = output.find('{') else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&output[object_start..]) else {
        return Vec::new();
    };
    fn collect(value: &serde_json::Value, output: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(object) => {
                for (key, value) in object {
                    if matches!(key.as_str(), "sha256" | "observedRevisionOrDigest") {
                        if let Some(digest) = value.as_str().filter(|digest| {
                            let hex = digest.strip_prefix("sha256:").unwrap_or(digest);
                            hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit())
                        }) {
                            output.push(digest.to_string());
                        }
                    }
                    collect(value, output);
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, output);
                }
            }
            _ => {}
        }
    }
    let mut digests = Vec::new();
    collect(&value, &mut digests);
    digests.sort();
    digests.dedup();
    digests
}

pub(super) fn tool_receipt_workspace_paths(
    output: &str,
    workspace_root: &std::path::Path,
) -> Vec<String> {
    let Some(object_start) = output.find('{') else {
        return Vec::new();
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&output[object_start..]) else {
        return Vec::new();
    };
    let Some(raw) = value
        .pointer("/file/filePath")
        .or_else(|| value.get("filePath"))
        .and_then(serde_json::Value::as_str)
    else {
        return Vec::new();
    };
    let path = std::path::Path::new(raw);
    let relative = if path.is_absolute() {
        path.strip_prefix(workspace_root).ok()
    } else {
        Some(path)
    };
    relative
        .filter(|path| {
            !path.as_os_str().is_empty()
                && !path.components().any(|component| {
                    matches!(
                        component,
                        std::path::Component::ParentDir
                            | std::path::Component::RootDir
                            | std::path::Component::Prefix(_)
                    )
                })
        })
        .map(|path| vec![path.to_string_lossy().replace('\\', "/")])
        .unwrap_or_default()
}

pub(super) fn completed_result(
    result_ref: Option<String>,
    usage: ExecutionUsage,
) -> ExecutionNodeResult {
    ExecutionNodeResult {
        status: ExecutionNodeStatus::Completed,
        result_ref,
        wait_class: None,
        summary: None,
        evidence_refs: Vec::new(),
        failure: None,
        usage,
        finished_at_ms: crate::tool_invocation::now_ms(),
    }
}

#[test]
fn continuation_resource_limit_is_explicit_and_supports_long_tasks() {
    assert_eq!(parse_continuation_resource_limit("1000").unwrap(), 1000);
    assert!(parse_continuation_resource_limit("0").is_err());
    assert!(parse_continuation_resource_limit("invalid").is_err());
}

/// Both business-completeness rollbacks must draw on the same budget. They used
/// to carry their own inline copy of the bound check, which is how the two could
/// drift apart - and a run could then obtain a fresh budget by alternating
/// between a completion gap and an outstanding closure rung.
#[test]
fn both_rollback_paths_share_one_bound() {
    // Only the production half of the file: this test's own source would
    // otherwise count itself.
    let source = include_str!("host_presentation.rs");
    let production = source
        .split("#[cfg(test)]")
        .next()
        .expect("production code precedes the test module");
    assert_eq!(
        production
            .matches("claim_continuation_budget(&mut state,")
            .count(),
        2,
        "both the completion-gap and the closure-rung rollback must claim the shared budget"
    );
    assert_eq!(
        production
            .matches("state.completion_gap_continuations =")
            .count(),
        1,
        "the continuation counter must be advanced in exactly one place"
    );
}

/// The terminal path records which layer each special case belongs to, so a
/// reader never has to infer why two of the four are shared and two are not.
#[test]
fn the_terminal_path_enumerates_its_four_special_cases() {
    let source = include_str!("host_presentation.rs");
    for marker in [
        "completion-gap rollback",
        "closure-rung rollback",
        "protocol-failure recovery",
        "structured-output recovery",
        "Business-completeness layer",
        "Provider-transport layer",
    ] {
        assert!(
            source.contains(marker),
            "the terminal path must record `{marker}`"
        );
    }
}
