use super::*;
#[derive(Debug, Clone, Copy)]
struct TuiFrameAreas {
    system: ratatui::layout::Rect,
    search: Option<ratatui::layout::Rect>,
    body: ratatui::layout::Rect,
    input: ratatui::layout::Rect,
    status: ratatui::layout::Rect,
}

impl TuiFrameAreas {
    fn build(area: ratatui::layout::Rect, input_h: u16, search_active: bool) -> Self {
        let top_h = 1u16;
        let bottom_status_h = 1u16;
        let system = ratatui::layout::Rect::new(area.x, area.y, area.width, top_h);
        let status = ratatui::layout::Rect::new(
            area.x,
            area.y
                .saturating_add(area.height.saturating_sub(bottom_status_h)),
            area.width,
            bottom_status_h,
        );
        let input_y = status.y.saturating_sub(input_h);
        let input = ratatui::layout::Rect::new(area.x, input_y, area.width, input_h);
        let available_body_h = input_y.saturating_sub(system.y.saturating_add(system.height));
        let search_h = if search_active && available_body_h > 1 {
            1
        } else {
            0
        };
        let search = (search_h > 0).then(|| {
            ratatui::layout::Rect::new(
                area.x,
                system.y.saturating_add(system.height),
                area.width,
                search_h,
            )
        });
        let body_y = system
            .y
            .saturating_add(system.height)
            .saturating_add(search_h);
        let body_h = input_y.saturating_sub(body_y);
        let body = ratatui::layout::Rect::new(area.x, body_y, area.width, body_h);

        Self {
            system,
            search,
            body,
            input,
            status,
        }
    }
}
impl TuiState {
    pub fn render(&mut self, frame: &mut Frame) {
        let render_started = std::time::Instant::now();
        let area = frame.area();
        self.shell.last_terminal_width = area.width;
        let skin = self.app.workbench.skin.clone();

        // Animation tick: advance all active animations
        self.shell.animation_engine.tick();

        // Toast tick: advance auto-dismiss timers
        self.overlay.toast_manager.tick();

        // Context suggestions tick: drain L4 events, expire stale suggestions
        self.overlay.context_suggestions.tick();

        // Sync chat view from App state before rendering
        self.shell.chat_view.sync_from_app(&self.app);

        // Sync agents overlay from App state
        self.overlay.agents_overlay.sync_from_app(&self.app);
        self.overlay.agents_overlay.tick();

        // Sync thinking panel from App state
        self.overlay.thinking_panel.sync_from_app(&self.app);
        self.overlay.thinking_panel.tick();

        if self.shell.layout_state.sidebar_visible {
            if let Some(topic) = self.workbench.active_topic_panel {
                match topic {
                    SidebarTopicPanel::Diff => self.overlay.diff_viewer.sync_from_app(&self.app),
                    SidebarTopicPanel::Memory => {
                        self.workbench.memory_panel.sync_from_app(&self.app);
                    }
                    SidebarTopicPanel::Skills => {
                        self.workbench.skills_panel.sync_from_app(&self.app)
                    }
                    SidebarTopicPanel::Config => {}
                    SidebarTopicPanel::Reality => {
                        self.workbench.reality_panel.sync_from_app(&self.app)
                    }
                }
            } else {
                match self.workbench.sidebar_active_tab {
                    TAB_RUNTIME => self
                        .workbench
                        .runtime_activity_panel
                        .sync_from_app(&self.app),
                    TAB_TOOLS => {}
                    TAB_CHANGES => {
                        let timeline = self.app.timeline_clone_vec();
                        self.workbench
                            .file_changes_panel
                            .sync_from_timeline(&timeline);
                    }
                    TAB_GOALS => self.workbench.goal_workbench_panel.sync_from_app(&self.app),
                    TAB_APPROVALS => self
                        .workbench
                        .approval_cockpit_panel
                        .sync_from_app(&self.app),
                    TAB_TODO => {
                        let timeline = self.app.timeline_clone_vec();
                        self.workbench.todo_panel.sync_from_timeline(&timeline);
                    }
                    TAB_FILES => {
                        if !self.app.workbench.file_entries.is_empty() {
                            self.workbench
                                .file_tree
                                .rebuild(&self.app.workbench.file_entries);
                            crate::performance::observe_count("tui_layout_cache_rebuild_count", 1);
                        }
                    }
                    TAB_SESSIONS => {
                        self.session
                            .session_sidebar
                            .refresh_if_changed(self.app.shell.picker_sessions.clone());
                        self.session
                            .session_sidebar
                            .set_current_session(&self.app.shell.session_id);
                    }
                    TAB_SURFACES => self.workbench.surface_panel.sync_from_app(&self.app),
                    TAB_APPS => {}
                    TAB_GATEWAY => self.workbench.gateway_panel.sync_from_app(&self.app),
                    _ => {}
                }
            }
        }

        self.overlay.performance_dashboard.tick();
        self.overlay.performance_dashboard.sync_from_app(&self.app);

        // BUG 1 FIX: No bidirectional sync — app.shell.input is the single source of truth.
        // Prompt is used only for autocomplete suggestions (rendered as overlay dropdown).

        // Sync status bar from App state
        self.workbench.system_status_bar.sync_from_app(&self.app);
        self.shell.status_bar.sync_from_app(&self.app);
        self.shell.status_bar.tick();
        let show_activity_panel =
            self.workbench.activity_panel_visible && !self.shell.layout_state.sidebar_visible;
        if show_activity_panel {
            self.workbench.activity_panel.sync_from_app(&self.app);
        }

        let max_input = (area.height / 2).max(3);
        let input_h =
            self.shell
                .composer
                .desired_height(&self.app.shell.input, area.width, max_input);
        let frame_areas = TuiFrameAreas::build(area, input_h, self.app.timeline.search_active);
        self.shell.composer_content_width = frame_areas.input.width.saturating_sub(2).max(1);

        // ── Main content: one RenderContext for chat, sidebar, status, input ──
        let mut main_ctx: RenderContext = RenderContext::new(frame, &skin);
        let toast_anchor_area: ratatui::layout::Rect;

        {
            let _guard = self.shell.render_profiler.guard("system_status_bar");
            let _ = error_recovery::catch_render_panic(
                "system_status_bar",
                AssertUnwindSafe(|| {
                    self.workbench
                        .system_status_bar
                        .render(&mut main_ctx, frame_areas.system);
                }),
            );
        }

        if let Some(search_area) = frame_areas.search {
            let search_text = if self.app.timeline.search_query.is_empty() {
                "/ ".to_string()
            } else {
                format!("/ {}", self.app.timeline.search_query)
            };
            let search_line = ratatui::text::Line::from(vec![
                ratatui::text::Span::styled(
                    search_text,
                    ratatui::style::Style::default()
                        .fg(ratatui::style::Color::Yellow)
                        .add_modifier(ratatui::style::Modifier::BOLD),
                ),
                ratatui::text::Span::styled(
                    "  Esc:cancel Enter:search",
                    ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray),
                ),
            ]);
            main_ctx
                .frame_mut()
                .render_widget(ratatui::widgets::Paragraph::new(search_line), search_area);
        }

        // 1. Render chat view + sidebar using the layout tree
        {
            main_ctx
                .frame_mut()
                .render_widget(ratatui::widgets::Clear, frame_areas.body);
            self.shell.layout_tree.resize(frame_areas.body);
            let mut chat_area = self
                .shell
                .layout_tree
                .area_of("chat")
                .unwrap_or(frame_areas.body);
            let topic_fullscreen = self.shell.layout_state.sidebar_visible
                && self.workbench.active_topic_panel.is_some()
                && frame_areas.body.width < 100;
            let app_fullscreen = self.shell.layout_state.sidebar_visible
                && self.workbench.active_topic_panel.is_none()
                && self.workbench.sidebar_active_tab == TAB_APPS;
            if self.shell.layout_state.sidebar_visible
                && self.workbench.active_topic_panel.is_some()
                && frame_areas.body.width >= 100
            {
                let max_topic_w = frame_areas.body.width.saturating_sub(40);
                let desired_topic_width = u32::from(frame_areas.body.width) * 55 / 100;
                let topic_w = crate::components::base::terminal_len(
                    usize::try_from(desired_topic_width).unwrap_or(usize::MAX),
                )
                .clamp(48, max_topic_w);
                chat_area.width = frame_areas.body.width.saturating_sub(topic_w).max(40);
            }
            if topic_fullscreen || app_fullscreen {
                chat_area.width = 0;
                toast_anchor_area = ratatui::layout::Rect::new(
                    frame_areas.body.x,
                    frame_areas.body.y,
                    frame_areas.body.width.min(56),
                    frame_areas.body.height,
                );
            } else if self.shell.layout_state.sidebar_visible {
                toast_anchor_area = chat_area;
            } else {
                toast_anchor_area = frame_areas.body;
            }
            let activity_area = if show_activity_panel && chat_area.width >= 72 {
                let desired = (chat_area.width / 3).clamp(30, 48);
                let width = desired.min(chat_area.width.saturating_sub(40));
                if width >= 24 {
                    chat_area.width = chat_area.width.saturating_sub(width);
                    Some(ratatui::layout::Rect::new(
                        chat_area.x.saturating_add(chat_area.width),
                        chat_area.y,
                        width,
                        chat_area.height,
                    ))
                } else {
                    None
                }
            } else {
                None
            };
            let sidebar_area = if topic_fullscreen || app_fullscreen {
                frame_areas.body
            } else {
                let sidebar_x = chat_area.x.saturating_add(chat_area.width);
                let sidebar_w = frame_areas
                    .body
                    .x
                    .saturating_add(frame_areas.body.width)
                    .saturating_sub(sidebar_x);
                ratatui::layout::Rect::new(
                    sidebar_x,
                    frame_areas.body.y,
                    sidebar_w,
                    frame_areas.body.height,
                )
            };
            self.shell.last_hit_areas = TuiHitAreas {
                chat: chat_area,
                activity: activity_area,
                sidebar: (self.shell.layout_state.sidebar_visible && sidebar_area.width > 0)
                    .then_some(sidebar_area),
                topic: None,
                input: frame_areas.input,
            };

            self.shell.chat_view.scroll_state.offset = self.app.timeline.scroll_offset;
            self.shell.chat_view.scroll_state.auto_scroll = self.app.timeline.auto_scroll;

            // Render chat view (already synced above)
            if chat_area.width > 0 && chat_area.height > 0 {
                let _guard = self.shell.render_profiler.guard("chat_view");
                self.shell.chat_view.render(&mut main_ctx, chat_area);
            }
            self.shell.chat_view.sync_to_app(&mut self.app);

            if let Some(activity_area) = activity_area {
                let _ = error_recovery::catch_render_panic(
                    "activity_panel",
                    AssertUnwindSafe(|| {
                        self.workbench
                            .activity_panel
                            .render(&mut main_ctx, activity_area);
                    }),
                );
            }

            if self.shell.layout_state.sidebar_visible && sidebar_area.width > 0 {
                main_ctx
                    .frame_mut()
                    .render_widget(ratatui::widgets::Clear, sidebar_area);
                // Render sidebar: tab bar + active panel
                let tab_height = 1u16;
                let tab_area = ratatui::layout::Rect::new(
                    sidebar_area.x,
                    sidebar_area.y,
                    sidebar_area.width,
                    tab_height,
                );
                if let Some(topic) = self.workbench.active_topic_panel {
                    let title = ratatui::text::Line::from(vec![
                        ratatui::text::Span::styled(
                            topic.label(),
                            ratatui::style::Style::default()
                                .fg(ratatui::style::Color::Cyan)
                                .add_modifier(ratatui::style::Modifier::BOLD),
                        ),
                        ratatui::text::Span::styled(
                            "  topic panel · Esc close · j/k scroll",
                            ratatui::style::Style::default().fg(ratatui::style::Color::DarkGray),
                        ),
                    ]);
                    main_ctx
                        .frame_mut()
                        .render_widget(ratatui::widgets::Paragraph::new(title), tab_area);
                } else {
                    let tab_labels = sidebar_tab_labels(sidebar_area.width);
                    let tabs = ratatui::widgets::Tabs::new(tab_labels)
                        .select(self.workbench.sidebar_active_tab);
                    main_ctx.frame_mut().render_widget(tabs, tab_area);
                }

                let panel_area = ratatui::layout::Rect::new(
                    sidebar_area.x,
                    sidebar_area.y.saturating_add(tab_height),
                    sidebar_area.width,
                    sidebar_area.height.saturating_sub(tab_height),
                );
                if self.workbench.active_topic_panel.is_some() {
                    self.shell.last_hit_areas.topic = Some(panel_area);
                }
                if self.workbench.active_topic_panel == Some(SidebarTopicPanel::Diff) {
                    // Collect diff text only when the diff panel is visible.
                    let diffs: Vec<String> = self
                        .app
                        .timeline_clone_vec()
                        .iter()
                        .filter_map(|e| {
                            if let crate::app::TimelineEntry::ToolCall { name, output, .. } = e {
                                if (name == "edit_file"
                                    || name == "patch_file"
                                    || name == "apply_diff")
                                    && !output.is_empty()
                                {
                                    Some(output.clone())
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect();
                    if !diffs.is_empty() {
                        let combined = diffs.join(
                            "
---
",
                        );
                        self.overlay.diff_viewer.load(&combined);
                    }
                }
                if let Some(topic) = self.workbench.active_topic_panel {
                    match topic {
                        SidebarTopicPanel::Diff => {
                            let _guard = self.shell.render_profiler.guard("diff_viewer");
                            let _ = error_recovery::catch_render_panic(
                                "diff_viewer",
                                AssertUnwindSafe(|| {
                                    self.overlay.diff_viewer.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        SidebarTopicPanel::Memory => {
                            let _guard = self.shell.render_profiler.guard("memory_panel");
                            let _ = error_recovery::catch_render_panic(
                                "memory_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .memory_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        SidebarTopicPanel::Skills => {
                            let _ = error_recovery::catch_render_panic(
                                "skills_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .skills_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        SidebarTopicPanel::Config => {
                            let _ = error_recovery::catch_render_panic(
                                "config_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .config_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        SidebarTopicPanel::Reality => {
                            let _ = error_recovery::catch_render_panic(
                                "reality_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .reality_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                    }
                } else {
                    match self.workbench.sidebar_active_tab {
                        TAB_RUNTIME => {
                            let _ = error_recovery::catch_render_panic(
                                "runtime_activity_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .runtime_activity_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_TOOLS => {
                            let _ = error_recovery::catch_render_panic(
                                "tool_ops_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .tool_ops_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_CHANGES => {
                            let _ = error_recovery::catch_render_panic(
                                "file_changes_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .file_changes_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_GOALS => {
                            let _ = error_recovery::catch_render_panic(
                                "goal_workbench_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .goal_workbench_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_APPROVALS => {
                            let _ = error_recovery::catch_render_panic(
                                "approval_cockpit_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .approval_cockpit_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_TODO => {
                            let _ = error_recovery::catch_render_panic(
                                "todo_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench.todo_panel.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_FILES => {
                            let _guard = self.shell.render_profiler.guard("file_tree");
                            let _ = error_recovery::catch_render_panic(
                                "file_tree",
                                AssertUnwindSafe(|| {
                                    self.workbench.file_tree.render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_SESSIONS => {
                            let _guard = self.shell.render_profiler.guard("session_sidebar");
                            let _ = error_recovery::catch_render_panic(
                                "session_sidebar",
                                AssertUnwindSafe(|| {
                                    self.session
                                        .session_sidebar
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_SURFACES => {
                            let _ = error_recovery::catch_render_panic(
                                "surface_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .surface_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        TAB_APPS => {
                            let _ = error_recovery::catch_render_panic(
                                "app_surface_host",
                                AssertUnwindSafe(|| {
                                    self.session.app_surface_host.render(
                                        main_ctx.frame_mut(),
                                        panel_area,
                                        self.shell.focus_target == FocusTarget::Sidebar,
                                    );
                                }),
                            );
                        }
                        TAB_GATEWAY => {
                            let _ = error_recovery::catch_render_panic(
                                "gateway_panel",
                                AssertUnwindSafe(|| {
                                    self.workbench
                                        .gateway_panel
                                        .render(&mut main_ctx, panel_area);
                                }),
                            );
                        }
                        _ => {}
                    }
                }
            }
        }

        // 2. Render status bar at bottom (reuses main_ctx)
        {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("status_bar");
                match error_recovery::catch_render_panic(
                    "status_bar",
                    AssertUnwindSafe(|| {
                        self.shell
                            .status_bar
                            .render(&mut main_ctx, frame_areas.status);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 2.5. Render the bottom composer from its canonical model. Layout is
        // derived from the current frame and cannot mutate authored bytes.
        {
            self.shell.composer.mode_label =
                if self.app.gateway.gateway_lease_mode.as_deref() == Some("read-only") {
                    "Read-only session".to_string()
                } else {
                    self.app.execution.turn_interaction.label()
                };
            let pending_resources = self.app.workbench.pending_resources.len();
            let queued_follow_ups = self.app.queued_follow_up_count();
            let queued_preview = self
                .app
                .queued_follow_up_preview()
                .map(|input| format!("{} · {}", input.decision, input.content_preview));
            let degraded = {
                let _guard = self.shell.render_profiler.guard("composer");
                match error_recovery::catch_render_panic(
                    "composer",
                    AssertUnwindSafe(|| {
                        self.shell.composer.render(
                            &mut main_ctx,
                            frame_areas.input,
                            &self.app.shell.input,
                            &mut self.shell.prompt,
                            &mut self.overlay.context_suggestions,
                            pending_resources,
                            queued_follow_ups,
                            queued_preview.as_deref(),
                        );
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // ── Overlays: one RenderContext for all conditional overlays ──
        let mut overlay_ctx: RenderContext = RenderContext::new(frame, &skin);

        // 4. Render agents overlay when visible
        if self.overlay.agents_overlay.visible {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("agents_overlay");
                match error_recovery::catch_render_panic(
                    "agents_overlay",
                    AssertUnwindSafe(|| {
                        self.overlay.agents_overlay.render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 5. Render agent team panel when visible
        if self.workbench.agent_team_panel.visible {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("agent_team_panel");
                match error_recovery::catch_render_panic(
                    "agent_team_panel",
                    AssertUnwindSafe(|| {
                        self.workbench
                            .agent_team_panel
                            .render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 5.1. Render performance dashboard when visible
        if self.overlay.performance_dashboard.visible {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("performance_dashboard");
                match error_recovery::catch_render_panic(
                    "performance_dashboard",
                    AssertUnwindSafe(|| {
                        // Render in a centered rectangle (70% width, 60% height)
                        let dash_w = (area.width as f32 * 0.7) as u16;
                        let dash_h = (area.height as f32 * 0.55) as u16;
                        let dash_x = (area.width.saturating_sub(dash_w)) / 2;
                        let dash_y = (area.height.saturating_sub(dash_h)) / 2;
                        let dash_area = ratatui::layout::Rect::new(dash_x, dash_y, dash_w, dash_h);
                        self.overlay
                            .performance_dashboard
                            .render(&mut overlay_ctx, dash_area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 5.5 Keep L4 memory cached, but do not auto-render it as a startup
        // overlay. The full memory/L4 surfaces are opened explicitly from the
        // sidebar/topic panels so they cannot cover the first screen.
        self.workbench.l4_memory_view.sync_from_app(&self.app);

        // 6. Render toast notifications at top-right
        if !self.overlay.toast_manager.is_empty() {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("toast_manager");
                match error_recovery::catch_render_panic(
                    "toast_manager",
                    AssertUnwindSafe(|| {
                        self.overlay
                            .toast_manager
                            .render(&mut overlay_ctx, toast_anchor_area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 6. Render dialog stack on top (backdrop + centered dialog)
        if !self.overlay.dialog_manager.is_empty() {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("dialog_manager");
                match error_recovery::catch_render_panic(
                    "dialog_manager",
                    AssertUnwindSafe(|| {
                        self.overlay.dialog_manager.render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 7. Render command palette when open
        if self.overlay.command_palette.is_open() {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("command_palette");
                match error_recovery::catch_render_panic(
                    "command_palette",
                    AssertUnwindSafe(|| {
                        self.overlay.command_palette.render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 8. Render question form when active
        if let Some(ref mut qf) = self.overlay.question_form {
            if qf.is_active() {
                let degraded = {
                    let _guard = self.shell.render_profiler.guard("question_form");
                    match error_recovery::catch_render_panic(
                        "question_form",
                        AssertUnwindSafe(|| {
                            qf.render(&mut overlay_ctx, area);
                        }),
                    ) {
                        RenderResult::Ok => None,
                        RenderResult::Degraded(msg) => Some(msg),
                    }
                };
                if let Some(msg) = degraded {
                    self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
                }
            }
        }

        // 9. Render export dialog when active
        if self.overlay.export_dialog_active {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("export_dialog");
                match error_recovery::catch_render_panic(
                    "export_dialog",
                    AssertUnwindSafe(|| {
                        self.overlay.export_dialog.render(&mut overlay_ctx, area);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // 10. Render Ctrl+O message menu
        self.render_message_menu(frame, area, &skin);

        // 11. Render startup loading overlay (highest z-index, below dialogs)
        if self.shell.startup_phase != StartupPhase::Done {
            self.render_startup_overlay(frame, frame_areas.body);
        }

        // 12. Render which-key overlay when Space leader is active
        if self.shell.keybind_engine.which_key_visible {
            let degraded = {
                let _guard = self.shell.render_profiler.guard("which_key");
                match error_recovery::catch_render_panic(
                    "which_key",
                    AssertUnwindSafe(|| {
                        WhichKey::draw(frame, area, &self.shell.keybind_engine);
                    }),
                ) {
                    RenderResult::Ok => None,
                    RenderResult::Degraded(msg) => Some(msg),
                }
            };
            if let Some(msg) = degraded {
                self.app.add_system_notice(SystemNoticeKind::Warning, &msg);
            }
        }

        // Update last drawn version for render skip optimization
        self.app.timeline.last_drawn_version = self.app.timeline.msg_version;
        self.app.timeline.last_drawn_render_version = self.app.timeline.render_version;
        self.app.timeline.lines_dirty = false;
        crate::performance::observe_duration("tui_render_ms", render_started.elapsed());
        crate::performance::observe_input_frame();
    }
}

impl TuiState {
    /// Render the Ctrl+O per-message action menu when pending.
    fn render_message_menu(
        &mut self,
        frame: &mut Frame,
        area: ratatui::layout::Rect,
        _skin: &crate::skin::SkinConfig,
    ) {
        if !self.shell.chat_view.pending_message_menu {
            return;
        }

        use ratatui::style::{Color, Modifier, Style};
        use ratatui::text::{Line, Span};
        use ratatui::widgets::{Block, Borders, Clear, Paragraph};

        let menu_items = [
            ("c", "Copy", "Copy focused entry to clipboard"),
            ("e", "Expand/Collapse", "Toggle expand/collapse"),
            ("r", "Revert to here", "Revert session to this point"),
        ];
        let n = menu_items.len();

        let w = 42u16;
        let h = crate::components::base::terminal_len(n)
            .saturating_add(4)
            .min(area.height.saturating_sub(2));
        let x = (area.width.saturating_sub(w)) / 2;
        let y = (area.height.saturating_sub(h)) / 2;
        let menu_rect = ratatui::layout::Rect::new(x, y, w, h);

        frame.render_widget(Clear, menu_rect);

        let mut lines: Vec<Line> = Vec::new();
        lines.push(Line::from(Span::styled(
            " Message Actions ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::raw(""));

        for (key, label, _desc) in &menu_items {
            lines.push(Line::from(vec![
                Span::styled(
                    format!("  [{key}] "),
                    Style::default()
                        .fg(Color::Yellow)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(*label, Style::default().fg(Color::White)),
            ]));
        }
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled(
            "  Esc to dismiss",
            Style::default().fg(Color::DarkGray),
        )));

        let block = Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));
        let paragraph = Paragraph::new(lines).block(block);
        frame.render_widget(paragraph, menu_rect);
    }

    /// Render the startup loading overlay at the bottom of the screen.
    fn render_startup_overlay(&self, frame: &mut Frame, area: ratatui::layout::Rect) {
        use ratatui::layout::Alignment;
        use ratatui::style::Style;
        use ratatui::text::Span;
        use ratatui::widgets::Paragraph;

        let text = match self.shell.startup_phase {
            StartupPhase::Loading => " ⟳ Loading plugins... ",
            StartupPhase::Finishing => " ⟳ Finishing startup... ",
            _ => return,
        };

        let fg = self.shell.theme_engine.theme.palette.fg;
        let bg = self.shell.theme_engine.theme.palette.muted;

        let overlay_y = area.y.saturating_add(area.height.saturating_sub(1));
        let overlay_rect = ratatui::layout::Rect::new(area.x, overlay_y, area.width, 1);

        let paragraph = Paragraph::new(Span::styled(text, Style::default().fg(fg).bg(bg)))
            .alignment(Alignment::Center);

        frame.render_widget(paragraph, overlay_rect);
    }
}
