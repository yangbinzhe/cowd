use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use super::super::app::App;

pub fn draw(f: &mut Frame, area: Rect, app: &mut App) {
    let mut lines: Vec<Line> = Vec::new();

    for msg in &app.messages {
        let (color, prefix) = match msg.role.as_str() {
            "user" => (app.theme.user_color(), "> "),
            "system" => (Color::DarkGray, "  "),
            _ => (app.theme.fg(), ""),
        };
        for line in msg.content.lines() {
            lines.push(Line::from(vec![
                Span::styled(prefix, Style::default().fg(color).bold()),
                Span::styled(line, Style::default().fg(color)),
            ]));
        }
        lines.push(Line::raw(""));
    }

    for card in &app.tool_cards {
        let status_icon = if card.done {
            if card.exit_code == Some(0) { "✅" } else { "❌" }
        } else {
            "⏳"
        };
        let status_text = if card.done {
            format!("{status_icon} exit:{}", card.exit_code.unwrap_or(0))
        } else {
            format!("{status_icon} running...")
        };
        lines.push(Line::from(vec![
            Span::styled("┌─ ", Style::default().fg(Color::Yellow)),
            Span::styled(&card.name, Style::default().fg(Color::Yellow).bold()),
            Span::styled(
                format!(" {status_text}"),
                Style::default().fg(if card.done { Color::Green } else { Color::Yellow }),
            ),
        ]));
        if card.expanded && !card.output.is_empty() {
            for line in card.output.lines().take(20) {
                lines.push(Line::from(
                    Span::styled(format!("│ {line}"), Style::default().fg(Color::DarkGray)),
                ));
            }
        }
        lines.push(Line::from(Span::styled("└─", Style::default().fg(Color::Yellow))));
        lines.push(Line::raw(""));
    }

    if app.turn_active || !app.streaming_thinking.is_empty() {
        let thinking_label = if app.thinking_complete { "Thinking" } else { "Thinking..." };
        let preview: String = app.streaming_thinking.lines().take(3).map(|l| format!("  {}", l)).collect::<Vec<_>>().join("\n");
        if !preview.is_empty() {
            lines.push(Line::from(Span::styled(
                format!("▶ {thinking_label}"),
                Style::default().fg(Color::DarkGray),
            )));
            for line in preview.lines() {
                lines.push(Line::from(Span::styled(
                    line.to_string(),
                    Style::default().fg(Color::DarkGray),
                )));
            }
            lines.push(Line::raw(""));
        }
        let spinner = app.spinner_char();
        lines.push(Line::from(vec![
            Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(Color::Blue),
            ),
        ]));
    }

    if app.messages.is_empty() && app.tool_cards.is_empty() {
        lines.push(Line::from(Span::styled(
            "Type to start. /help /resume /exit",
            Style::default().fg(Color::DarkGray),
        )));
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

    f.render_widget(Clear, area);
    let paragraph = Paragraph::new(Text::from(lines))
        .wrap(Wrap { trim: false })
        .scroll((scroll_offset, 0));
    f.render_widget(paragraph, inner_area);

    if content_height > viewport_height {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"))
            .track_symbol(Some("│"))
            .thumb_symbol("█");
        let mut scroll_state = ScrollbarState::new(content_height as usize)
            .position(scroll_offset as usize)
            .viewport_content_length(viewport_height as usize);
        f.render_stateful_widget(scrollbar, scrollbar_area, &mut scroll_state);
    }
}
