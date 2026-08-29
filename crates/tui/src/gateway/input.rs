use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum TerminalInputDirective {
    SkipPostInput,
    RunPostInput,
    Exit,
}

struct TerminalInputContext<'a> {
    gateway_client: &'a GatewayApiClient,
    tui_tx: &'a CowdEventSender,
    state: &'a mut TuiState,
    gateway_lease_owner: &'a Option<String>,
    session_authorities: &'a SessionAuthorityRegistry,
    message_submission_generation: &'a mut u64,
    session_switch_generation: &'a mut u64,
    session_switch_inflight_target: &'a mut Option<String>,
    session_switch_tx: &'a tokio::sync::mpsc::UnboundedSender<SessionSwitchResult>,
    observer_id: &'a str,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn handle_terminal_event(
    event: Event,
    gateway_client: &GatewayApiClient,
    tui_tx: &CowdEventSender,
    state: &mut TuiState,
    gateway_lease_owner: &Option<String>,
    session_authorities: &SessionAuthorityRegistry,
    message_submission_generation: &mut u64,
    session_switch_generation: &mut u64,
    session_switch_inflight_target: &mut Option<String>,
    session_switch_tx: &tokio::sync::mpsc::UnboundedSender<SessionSwitchResult>,
    observer_id: &str,
) -> bool {
    let mut ctx = TerminalInputContext {
        gateway_client,
        tui_tx,
        state,
        gateway_lease_owner,
        session_authorities,
        message_submission_generation,
        session_switch_generation,
        session_switch_inflight_target,
        session_switch_tx,
        observer_id,
    };
    if matches!(event, Event::Resize(_, _)) {
        ctx.state.app.request_redraw();
    }
    match event {
        Event::Mouse(mouse)
            if matches!(
                mouse.kind,
                crossterm::event::MouseEventKind::ScrollDown
                    | crossterm::event::MouseEventKind::ScrollUp
            ) =>
        {
            let down = matches!(mouse.kind, crossterm::event::MouseEventKind::ScrollDown);
            ctx.state
                .handle_mouse_scroll_at(down, mouse.column, mouse.row);
            ctx.state.app.request_redraw();
            false
        }
        Event::Paste(text) => {
            ctx.state.process_paste(&text);
            false
        }
        Event::Key(key) if key.kind == KeyEventKind::Press => handle_terminal_key(&mut ctx, key),
        _ => false,
    }
}

fn handle_terminal_key(
    ctx: &mut TerminalInputContext<'_>,
    key: crossterm::event::KeyEvent,
) -> bool {
    let active_session_id = ctx.state.app.shell.session_id.clone();
    let authority_generation = ctx
        .session_authorities
        .current(&active_session_id)
        .unwrap_or_default();
    ctx.state.app.request_redraw();
    if ctx.state.app.shell.picker_active {
        ctx.state.open_session_picker_dialog();
    }
    let directive = match ctx.state.process_raw_key(key) {
        ProcessedKey::Submit(text) => {
            handle_submission(ctx, text, &active_session_id, authority_generation)
        }
        ProcessedKey::Exit => TerminalInputDirective::Exit,
        ProcessedKey::Cancel => {
            if ctx.gateway_lease_owner.is_some() {
                dispatch_gateway_cancel(
                    ctx.gateway_client,
                    ctx.tui_tx,
                    &active_session_id,
                    ctx.state.app.execution.current_execution_id.as_deref(),
                    ctx.state.app.execution.current_turn_id.as_deref(),
                    authority_generation,
                );
            } else {
                ctx.state.app.add_system_notice(
                    SystemNoticeKind::Error,
                    "This session is attached read-only; no cancellation was sent.",
                );
            }
            TerminalInputDirective::RunPostInput
        }
        ProcessedKey::Nothing => TerminalInputDirective::RunPostInput,
    };
    match directive {
        TerminalInputDirective::Exit => true,
        TerminalInputDirective::SkipPostInput => false,
        TerminalInputDirective::RunPostInput => {
            handle_post_input(ctx);
            false
        }
    }
}

fn handle_submission(
    ctx: &mut TerminalInputContext<'_>,
    text: String,
    active_session_id: &str,
    active_authority_generation: u64,
) -> TerminalInputDirective {
    let state = &mut *ctx.state;
    let gateway_client = ctx.gateway_client;
    let tui_tx = ctx.tui_tx;
    let gateway_lease_owner = ctx.gateway_lease_owner;
    if text.is_empty() {
        return TerminalInputDirective::SkipPostInput;
    }
    if matches!(text.as_str(), "/exit" | "/quit") {
        return TerminalInputDirective::Exit;
    }
    if let Some(path) = attach_path_from_command(&text) {
        if gateway_lease_owner.is_none() {
            state.app.shell.input.set_text(&text);
            state.app.add_system_notice(
                                                SystemNoticeKind::Error,
                                                "This session is attached read-only. Resource upload was not sent and the command was restored.",
                                            );
            return TerminalInputDirective::SkipPostInput;
        }
        state.app.add_system_notice(
            SystemNoticeKind::Info,
            &format!("Uploading {} in the background", path.display()),
        );
        dispatch_gateway_resource_upload(
            &gateway_client,
            &tui_tx,
            &active_session_id,
            active_authority_generation,
            path.to_path_buf(),
        );
        return TerminalInputDirective::SkipPostInput;
    }
    if let Some(command) = execution_command_from_input(&text) {
        if gateway_lease_owner.is_none() {
            state.app.shell.input.set_text(&text);
            state.app.add_system_notice(
                                                SystemNoticeKind::Error,
                                                "This session is attached read-only. The execution command was not sent and was restored.",
                                            );
            return TerminalInputDirective::SkipPostInput;
        }
        dispatch_execution_projection_command(
            &gateway_client,
            &tui_tx,
            &state,
            &active_session_id,
            active_authority_generation,
            command,
        );
        return TerminalInputDirective::SkipPostInput;
    }
    if text.starts_with('/') {
        return handle_slash_submission(ctx, text, active_session_id, active_authority_generation);
    }
    if gateway_lease_owner.is_none() {
        state.app.shell.input.set_text(&text);
        state.app.add_system_notice(
                                            SystemNoticeKind::Error,
                                            "This session is read-only because its writer lease was not acquired. The draft was restored; switch sessions or retry after the conflicting writer releases its lease.",
                                        );
        return TerminalInputDirective::SkipPostInput;
    }
    let client_message_id = format!("tui:{}", uuid::Uuid::new_v4());
    let execution_was_active = state.app.turn_is_active();
    *ctx.message_submission_generation = ctx.message_submission_generation.wrapping_add(1);
    let submission_generation = *ctx.message_submission_generation;
    state.app.begin_message_admission(
        &text,
        client_message_id.clone(),
        submission_generation,
        !execution_was_active,
    );
    let resource_ids = state
        .app
        .workbench
        .pending_resources
        .iter()
        .map(|resource| resource.id.clone())
        .collect::<Vec<_>>();
    dispatch_gateway_message(
        &gateway_client,
        &tui_tx,
        &active_session_id,
        active_authority_generation,
        text,
        resource_ids,
        client_message_id,
        submission_generation,
        !execution_was_active,
    );
    TerminalInputDirective::SkipPostInput
}

fn handle_slash_submission(
    ctx: &mut TerminalInputContext<'_>,
    text: String,
    active_session_id: &str,
    active_authority_generation: u64,
) -> TerminalInputDirective {
    let state = &mut *ctx.state;
    let gateway_client = ctx.gateway_client;
    let tui_tx = ctx.tui_tx;
    let gateway_lease_owner = ctx.gateway_lease_owner;
    if text.trim() == "/history older" {
        if state.app.turn_is_active() {
            state.app.add_system_notice(
                                                    SystemNoticeKind::Warning,
                                                    "History window navigation is paused during an active turn so live entries cannot be evicted.",
                                                );
        } else if state.app.history.history_loading_older {
            state.app.add_system_notice(
                SystemNoticeKind::Info,
                "An older history page is already loading in the background.",
            );
        } else if !state.app.history.history_has_older {
            state.app.add_system_notice(
                SystemNoticeKind::Info,
                "This history window is already at the oldest durable message.",
            );
        } else {
            state.app.history.history_loading_older = true;
            dispatch_older_history_page(
                &gateway_client,
                &tui_tx,
                &active_session_id,
                active_authority_generation,
                state.app.history.history_oldest_offset,
            );
        }
        return TerminalInputDirective::SkipPostInput;
    }
    if text.trim() == "/history newer" {
        if state.app.turn_is_active() {
            state.app.add_system_notice(
                SystemNoticeKind::Warning,
                "History window navigation is paused during an active turn.",
            );
        } else if state.app.history.history_loading_newer {
            state.app.add_system_notice(
                SystemNoticeKind::Info,
                "A newer history page is already loading.",
            );
        } else if state.app.history.history_window_end_offset
            >= state.app.history.history_total_messages
        {
            state.app.add_system_notice(
                SystemNoticeKind::Info,
                "This window already contains the latest durable history.",
            );
        } else {
            state.app.history.history_loading_newer = true;
            dispatch_newer_history_page(
                &gateway_client,
                &tui_tx,
                &active_session_id,
                active_authority_generation,
                state.app.history.history_window_end_offset,
                state.app.history.history_total_messages,
                false,
            );
        }
        return TerminalInputDirective::SkipPostInput;
    }
    if text.trim() == "/history latest" {
        if state.app.turn_is_active() {
            state.app.add_system_notice(
                SystemNoticeKind::Warning,
                "History window navigation is paused during an active turn.",
            );
        } else {
            state.app.history.history_loading_newer = true;
            dispatch_newer_history_page(
                &gateway_client,
                &tui_tx,
                &active_session_id,
                active_authority_generation,
                state.app.history.history_window_end_offset,
                state.app.history.history_total_messages,
                true,
            );
        }
        return TerminalInputDirective::SkipPostInput;
    }
    if let Some(input_id) = queue_cancel_command(&text) {
        if gateway_lease_owner.is_none() {
            state.app.shell.input.set_text(&text);
            state.app.add_system_notice(
                SystemNoticeKind::Error,
                "This session is attached read-only. Queued input was not changed.",
            );
            return TerminalInputDirective::SkipPostInput;
        }
        dispatch_pending_input_cancel(
            &gateway_client,
            &tui_tx,
            &active_session_id,
            active_authority_generation,
            input_id,
        );
        return TerminalInputDirective::SkipPostInput;
    }
    if let Some(input_id) = queue_edit_command(&text) {
        if gateway_lease_owner.is_none() {
            state.app.shell.input.set_text(&text);
            state.app.add_system_notice(
                SystemNoticeKind::Error,
                "This session is attached read-only. Queued input was not changed.",
            );
            return TerminalInputDirective::SkipPostInput;
        }
        if let Some(input) = state
            .app
            .workbench
            .pending_inputs
            .iter()
            .find(|input| input.input_id == input_id)
        {
            state.app.shell.input.set_text(&input.content_preview);
            dispatch_pending_input_cancel(
                &gateway_client,
                &tui_tx,
                &active_session_id,
                active_authority_generation,
                input_id,
            );
            state.app.add_system_notice(
                                                    SystemNoticeKind::Info,
                                                    "Queued follow-up restored to composer; edit it and submit to replace the canonical input.",
                                                );
        } else {
            state.app.add_system_notice(
                SystemNoticeKind::Warning,
                "Queued input was not found locally; refresh or use its full input id.",
            );
        }
        return TerminalInputDirective::SkipPostInput;
    }
    if gateway_lease_owner.is_none() && !read_only_slash_command(&text) {
        state.app.shell.input.set_text(&text);
        state.app.add_system_notice(
                                                SystemNoticeKind::Error,
                                                "This session is attached read-only. Only read-only inspection commands are available; the command was restored.",
                                            );
        return TerminalInputDirective::SkipPostInput;
    }
    dispatch_gateway_slash(
        &gateway_client,
        &tui_tx,
        state,
        &active_session_id,
        active_authority_generation,
        &text,
    );
    TerminalInputDirective::SkipPostInput
}

fn handle_post_input(ctx: &mut TerminalInputContext<'_>) {
    if let Some(target_session_id) = take_pending_session_switch(&mut ctx.state) {
        if target_session_id == ctx.state.app.shell.session_id {
            ctx.state
                .session
                .session_sidebar
                .set_current_session(&target_session_id);
            return;
        }
        if let Some(inflight) = ctx.session_switch_inflight_target.as_deref() {
            ctx.state.app.add_system_notice(
                                        SystemNoticeKind::Info,
                                        &format!(
                                            "Session switch to {inflight} is still being prepared; finish or fail that atomic switch before selecting another session"
                                        ),
                                    );
            return;
        }
        *ctx.session_switch_generation = ctx.session_switch_generation.wrapping_add(1).max(1);
        *ctx.session_switch_inflight_target = Some(target_session_id.clone());
        ctx.state.app.add_system_notice(
                                    SystemNoticeKind::Info,
                                    &format!("Preparing session switch to {target_session_id}; the current view remains interactive"),
                                );
        dispatch_gateway_session_switch(
            ctx.gateway_client.clone(),
            ctx.session_switch_tx.clone(),
            *ctx.session_switch_generation,
            target_session_id,
            ctx.observer_id.to_string(),
        );
    }
    consume_pending_session_sidebar_actions(&mut ctx.state);
    consume_pending_session_export(&mut ctx.state);
}
