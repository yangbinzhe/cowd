use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap},
    Frame,
};
use super::app::App;

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    let chunks = Layout::default().direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1), Constraint::Length(3)]).split(area);

    draw_status_bar(frame, chunks[0], app);
    draw_messages(frame, chunks[1], app);
    draw_input(frame, chunks[2], app);
    if app.picker_active { draw_session_picker(frame, area, app); }
    if app.approval.is_some() { draw_approval_modal(frame, area, app); }
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
    let sid = if app.session_id.len() > 8 { &app.session_id[..8] } else { &app.session_id };
    let st = if app.is_loading { format!("{} Thinking", app.spinner_char()) } else { "✓ Ready".into() };
    let t = app.theme;
    let text = Line::from(vec![
        Span::styled("Cowd", Style::default().fg(t.accent()).bold()),
        Span::styled(format!(" │ sess:{sid} │ {} │ {st}", app.model), Style::default().fg(t.fg())),
    ]);
    frame.render_widget(Paragraph::new(text).style(Style::default().bg(t.bg())), area);
}

fn draw_messages(frame: &mut Frame, area: Rect, app: &App) {
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
    frame.render_widget(Paragraph::new(Text::from(lines)).wrap(Wrap { trim: false }), area);
}

fn draw_input(frame: &mut Frame, area: Rect, app: &App) { frame.render_widget(&app.input, area); }
