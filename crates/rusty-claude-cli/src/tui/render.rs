use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use super::app::{App, Panel};

pub fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();
    let chunks = Layout::default().direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1), Constraint::Length(3)]).split(area);

    draw_status_bar(frame, chunks[0], app);
    match app.current_panel {
        Panel::Chat | Panel::Gateway | Panel::Delegate => draw_messages(frame, chunks[1], app),
        Panel::Files => draw_file_browser(frame, chunks[1], app),
        Panel::Memory => draw_memory_panel(frame, chunks[1], app),
        Panel::Skills => draw_skills_panel(frame, chunks[1], app),
    }
    draw_input(frame, chunks[2], app);
    if app.picker_active { draw_session_picker(frame, area, app); }
    if app.approval.is_some() { draw_approval_modal(frame, area, app); }
    if app.current_panel == Panel::Gateway { draw_gateway_panel(frame, area, app); }
    if app.current_panel == Panel::Delegate { draw_delegate_panel(frame, area, app); }
}

fn draw_approval_modal(frame: &mut Frame, area: Rect, app: &App) {
    let req = app.approval.as_ref().unwrap();
    let w = 60u16.min(area.width - 4);
    let h = 6u16;
    let x = (area.width - w) / 2;
    let y = (area.height - h) / 2;
    let pa = Rect::new(x, y, w, h);
    frame.render_widget(Clear, pa);

    let text = format!(
        "Tool: {}\nInput: {}\n\n[Y] Approve  [N] Deny",
        req.tool_name,
        req.input_preview.chars().take(40).collect::<String>()
    );
    let block = Block::default().borders(Borders::ALL).title("Approval Required").fg(Color::Yellow);
    frame.render_widget(Paragraph::new(text).block(block), pa);
}

fn draw_session_picker(frame: &mut Frame, area: Rect, app: &App) {
    let n = app.picker_sessions.len();
    if n == 0 { return; }
    let w = 70u16.min(area.width - 4);
    let h = (n as u16 + 3).min(area.height - 2);
    let x = (area.width.saturating_sub(w)) / 2;
    let y = (area.height.saturating_sub(h)) / 2;
    let pa = Rect::new(x, y, w, h);
    frame.render_widget(Clear, pa);

    let items: Vec<ListItem> = app.picker_sessions.iter().enumerate().map(|(i, s)| {
        let ts = chrono::DateTime::from_timestamp((s.updated_at_ms / 1000) as i64, 0)
            .map(|d| d.format("%m-%d %H:%M").to_string()).unwrap_or_default();
        let label = format!("{}  {} msgs  {}  {}",
            if i == app.picker_idx { "▶" } else { " " }, s.message_count, ts, &s.id[..8.min(s.id.len())]);
        if i == app.picker_idx { ListItem::from(label).style(Style::default().fg(Color::Black).bg(Color::Cyan)) }
        else { ListItem::from(label) }
    }).collect();

    let list = List::new(items).block(Block::default().borders(Borders::ALL)
        .title(" Select session (↑↓ jk Enter Esc) ").fg(Color::Yellow));
    frame.render_widget(list, pa);
}

fn draw_status_bar(frame: &mut Frame, area: Rect, app: &App) {
    let _sid = if app.session_id.len() > 8 { &app.session_id[..8] } else { &app.session_id };
    let _st = if app.is_loading { format!("{} Thinking", app.spinner_char()) } else { "✓ Ready".into() };
    let t = app.theme;
    let panel_label = match app.current_panel {
        Panel::Chat => "Chat",
        Panel::Gateway => "Gateway",
        Panel::Files => "Files",
        Panel::Memory => "Memory",
        Panel::Skills => "Skills",
        Panel::Delegate => "Delegates",
    };
    let mut spans = vec![
        Span::styled("Cowd", Style::default().fg(t.accent()).bold()),
        Span::styled(format!(" │ {panel_label} │ {}", app.model), Style::default().fg(t.fg())),
    ];
    if app.token_count > 0 {
        spans.push(Span::styled(format!(" │ {}tk", app.token_count), Style::default().fg(Color::DarkGray)));
    }
    if let Some(cost) = app.cost_estimate {
        spans.push(Span::styled(format!(" │ ${:.4}", cost), Style::default().fg(Color::DarkGray)));
    }
    if app.compaction_count > 0 {
        spans.push(Span::styled(format!(" │ compactedx{}", app.compaction_count), Style::default().fg(Color::DarkGray)));
    }
    if app.cache_hits > 0 {
        spans.push(Span::styled(format!(" │ cache:{}", app.cache_hits), Style::default().fg(Color::Green)));
    }
    // M8: Token context profiler — compact utilization indicator
    if app.token_count > 0 {
        let pct = (app.token_count as f64 / 200_000.0 * 10.0).min(10.0) as usize;
        let bar: String = (0..10).map(|i| if i < pct { '█' } else { '░' }).collect();
        spans.push(Span::styled(format!(" [{bar}]"), Style::default().fg(Color::DarkGray)));
    }
    // 01: profiler distribution summary
    spans.push(Span::styled(format!(" │ {}", "events"), Style::default().fg(Color::DarkGray)));
    let text = Line::from(spans);
    frame.render_widget(Paragraph::new(text).style(Style::default().bg(t.bg())), area);
}

fn draw_messages(frame: &mut Frame, area: Rect, app: &mut App) {
    let mut lines: Vec<Line> = Vec::new();
    for msg in &app.messages {
        let (c, p) = match msg.role.as_str() { "user" => (Color::Green, "> "), "system" => (Color::DarkGray, "  "), _ => (Color::White, "") };
        for line in msg.content.lines() {
            lines.push(Line::from(vec![Span::styled(p, Style::default().fg(c).bold()), Span::styled(line, Style::default().fg(c))]));
        }
        lines.push(Line::raw(""));
    }
    for card in &app.tool_cards {
        let st = if card.done { format!("✔ exit:{}", card.exit_code.unwrap_or(0)) } else { "running...".into() };
        lines.push(Line::from(vec![
            Span::styled("┌─ ", Style::default().fg(Color::Yellow)),
            Span::styled(&card.name, Style::default().fg(Color::Yellow).bold()),
            Span::styled(format!(" {st}"), Style::default().fg(if card.done { Color::Green } else { Color::Yellow })),
        ]));
        if card.expanded && !card.output.is_empty() {
            for line in card.output.lines().take(20) {
                lines.push(Line::from(Span::styled(format!("│ {line}"), Style::default().fg(Color::DarkGray))));
            }
        }
        lines.push(Line::from(Span::styled("└─", Style::default().fg(Color::Yellow))));
        lines.push(Line::raw(""));
    }
    if app.is_loading {
        lines.push(Line::from(vec![Span::styled(format!("{} Processing...", app.spinner_char()), Style::default().fg(Color::Blue))]));
    }
    if app.messages.is_empty() && app.tool_cards.is_empty() {
        lines.push(Line::from(Span::styled("Type to start. /help /resume /exit", Style::default().fg(Color::DarkGray))));
    }

    let content_height = lines.len() as u16;
    let viewport_height = area.height;

    if app.auto_scroll && content_height > viewport_height {
        app.scroll_offset = content_height.saturating_sub(viewport_height);
    }

    let scroll_offset = app.scroll_offset.min(content_height.saturating_sub(1));

    let inner_area = Rect {
        x: area.x,
        y: area.y,
        width: area.width.saturating_sub(1),
        height: area.height,
    };

    let scrollbar_area = Rect {
        x: area.right().saturating_sub(1),
        y: area.y,
        width: 1,
        height: area.height,
    };

    frame.render_widget(Clear, area);

    let paragraph = Paragraph::new(Text::from(lines))
        .wrap(Wrap { trim: false })
        .scroll((scroll_offset, 0));
    frame.render_widget(paragraph, inner_area);

    if content_height > viewport_height {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"))
            .track_symbol(Some("│"))
            .thumb_symbol("█");
        let scroll_state = ScrollbarState::new(content_height as usize)
            .position(scroll_offset as usize)
            .viewport_content_length(viewport_height as usize);
        frame.render_stateful_widget(scrollbar, scrollbar_area, &mut scroll_state.clone());
    }
}

fn draw_input(frame: &mut Frame, area: Rect, app: &App) { frame.render_widget(&app.input, area); }

fn draw_gateway_panel(frame: &mut Frame, area: Rect, app: &App) {
    let w = 65u16.min(area.width - 4);
    let h = 20u16.min(area.height - 2);
    let x = (area.width.saturating_sub(w)) / 2;
    let y = (area.height.saturating_sub(h)) / 2;
    let pa = Rect::new(x, y, w, h);
    frame.render_widget(Clear, pa);

    let mut lines = vec![
        Line::from(Span::styled("Gateway Sessions", Style::default().fg(Color::Cyan).bold())),
        Line::raw(""),
    ];
    if app.gateway_sessions.is_empty() {
        lines.push(Line::from(Span::styled("  No gateway sessions. Configure platforms in config.yaml", Style::default().fg(Color::DarkGray))));
    } else {
        for s in &app.gateway_sessions {
            lines.push(Line::from(vec![
                Span::styled(format!("  [{}] ", s.platform), Style::default().fg(Color::Yellow)),
                Span::styled(&s.title, Style::default().fg(Color::White)),
                Span::styled(format!(" ({} msgs)", s.message_count), Style::default().fg(Color::DarkGray)),
            ]));
        }
    }
    let block = Block::default().borders(Borders::ALL).title(" Gateway (Tab to switch) ").fg(Color::Cyan);
    frame.render_widget(Paragraph::new(Text::from(lines)).block(block), pa);
}

fn draw_file_browser(frame: &mut Frame, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();
    if app.file_entries.is_empty() {
        lines.push(Line::from(Span::styled("No files loaded. Press 'r' to refresh.", Style::default().fg(Color::DarkGray))));
    } else {
        for f in &app.file_entries {
            let icon = if f.is_dir { "📁" } else { "📄" };
            let size = if f.size > 1024 { format!("{}KB", f.size/1024) } else { format!("{}B", f.size) };
            lines.push(Line::from(vec![
                Span::styled(format!("{icon} "), Style::default()),
                Span::styled(&f.name, Style::default().fg(Color::White)),
                Span::styled(format!(" ({size})"), Style::default().fg(Color::DarkGray)),
            ]));
        }
    }
    frame.render_widget(Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }), area);
}

fn draw_delegate_panel(frame: &mut Frame, area: Rect, app: &App) {
    let w = 65u16.min(area.width - 4);
    let h = 18u16.min(area.height - 2);
    let x = (area.width.saturating_sub(w)) / 2;
    let y = (area.height.saturating_sub(h)) / 2;
    let pa = Rect::new(x, y, w, h);
    frame.render_widget(Clear, pa);

    let mut lines = vec![
        Line::from(Span::styled("Delegate Tasks", Style::default().fg(Color::Magenta).bold())),
        Line::raw(""),
    ];
    if app.delegate_tasks.is_empty() {
        lines.push(Line::from(Span::styled("  No active delegates", Style::default().fg(Color::DarkGray))));
    } else {
        for t in &app.delegate_tasks {
            let status_icon = match t.status.as_str() {
                "running" => "⏳",
                "done" => "✅",
                "error" => "❌",
                _ => "⏺",
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{status_icon} "), Style::default()),
                Span::styled(&t.description, Style::default().fg(Color::White)),
                Span::styled(format!(" [{}]", t.status), Style::default().fg(Color::DarkGray)),
            ]));
        }
    }
    let block = Block::default().borders(Borders::ALL).title(" Delegate Dashboard (Tab to switch) ").fg(Color::Magenta);
    frame.render_widget(Paragraph::new(Text::from(lines)).block(block), pa);
}

fn draw_memory_panel(frame: &mut Frame, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();
    if app.memory_entries.is_empty() {
        lines.push(Line::from(Span::styled("No memory entries loaded.", Style::default().fg(Color::DarkGray))));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled("Memory system: 3-layer (L0 Identity / L1 Essential / L3 Deep Recall)", Style::default().fg(Color::DarkGray))));
        lines.push(Line::from(Span::styled("Auto-extraction: background async, zero token cost", Style::default().fg(Color::DarkGray))));
    } else {
        for entry in &app.memory_entries {
            let icon = match entry.priority.as_str() {
                "high" => "🔴",
                "medium" => "🟡",
                _ => "⚪",
            };
            lines.push(Line::from(vec![
                Span::styled(format!("{icon} [{}] ", entry.layer), Style::default().fg(Color::Cyan)),
                Span::styled(&entry.content, Style::default().fg(Color::White)),
            ]));
        }
    }
    frame.render_widget(Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }), area);
}

fn draw_skills_panel(frame: &mut Frame, area: Rect, app: &App) {
    let mut lines: Vec<Line> = Vec::new();
    if app.skill_list.is_empty() {
        lines.push(Line::from(Span::styled("No skills loaded.", Style::default().fg(Color::DarkGray))));
        lines.push(Line::raw(""));
        lines.push(Line::from(Span::styled("Skills: installable agent capabilities with safety scanning", Style::default().fg(Color::DarkGray))));
    } else {
        for skill in &app.skill_list {
            let icon = if skill.installed { "✅" } else { "⬜" };
            lines.push(Line::from(vec![
                Span::styled(format!("{icon} ", ), Style::default()),
                Span::styled(&skill.name, Style::default().fg(Color::Yellow).bold()),
                Span::styled(format!(" — {}", skill.description), Style::default().fg(Color::White)),
            ]));
        }
    }
    frame.render_widget(Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }), area);
}
