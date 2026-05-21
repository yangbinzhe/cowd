use ratatui::{
    layout::{Constraint, Direction, Layout},
    Frame,
};
use super::app::{App, Panel};
use super::widgets::{self, chat, status_bar};

pub fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.area();

    let input_lines = app.input.lines().len().max(1) as u16;
    let max_input = (area.height / 2).max(3);
    let input_h = (input_lines + 2).min(max_input).max(3);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(input_h),
        ])
        .split(area);

    status_bar::draw(frame, chunks[0], app);

    match app.current_panel {
        Panel::Chat | Panel::Gateway | Panel::Delegate => {
            chat::draw(frame, chunks[1], app);
        }
        Panel::Files => draw_file_browser(frame, chunks[1], app),
        Panel::Memory => draw_memory_panel(frame, chunks[1], app),
        Panel::Skills => draw_skills_panel(frame, chunks[1], app),
    }

    draw_input(frame, chunks[2], app);

    let full = frame.area();
    if app.picker_active { draw_session_picker(frame, full, app); }
    if app.approval.is_some() { draw_approval_modal(frame, full, app); }
    if app.current_panel == Panel::Gateway { draw_gateway_panel(frame, full, app); }
    if app.current_panel == Panel::Delegate { draw_delegate_panel(frame, full, app); }
}

fn draw_input(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    frame.render_widget(&app.input, area);
}

use ratatui::{
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
};

fn draw_approval_modal(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let req = app.approval.as_ref().unwrap();
    let w = 60u16.min(area.width - 4);
    let h = 6u16;
    let x = (area.width - w) / 2;
    let y = (area.height - h) / 2;
    let pa = ratatui::layout::Rect::new(x, y, w, h);
    frame.render_widget(Clear, pa);
    let text = format!(
        "Tool: {}\nInput: {}\n\n[Y] Approve  [N] Deny",
        req.tool_name,
        req.input_preview.chars().take(40).collect::<String>()
    );
    let block = Block::default().borders(Borders::ALL).title("Approval Required").fg(Color::Yellow);
    frame.render_widget(Paragraph::new(text).block(block), pa);
}

fn draw_session_picker(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let n = app.picker_sessions.len();
    if n == 0 { return; }
    let w = 70u16.min(area.width - 4);
    let h = (n as u16 + 3).min(area.height - 2);
    let x = (area.width.saturating_sub(w)) / 2;
    let y = (area.height.saturating_sub(h)) / 2;
    let pa = ratatui::layout::Rect::new(x, y, w, h);
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

fn draw_gateway_panel(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let w = 65u16.min(area.width - 4);
    let h = 20u16.min(area.height - 2);
    let x = (area.width.saturating_sub(w)) / 2;
    let y = (area.height.saturating_sub(h)) / 2;
    let pa = ratatui::layout::Rect::new(x, y, w, h);
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

fn draw_file_browser(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
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

fn draw_delegate_panel(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
    let w = 65u16.min(area.width - 4);
    let h = 18u16.min(area.height - 2);
    let x = (area.width.saturating_sub(w)) / 2;
    let y = (area.height.saturating_sub(h)) / 2;
    let pa = ratatui::layout::Rect::new(x, y, w, h);
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

fn draw_memory_panel(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
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

fn draw_skills_panel(frame: &mut Frame, area: ratatui::layout::Rect, app: &App) {
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
