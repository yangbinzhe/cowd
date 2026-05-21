use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span},
    widgets::Paragraph,
    Frame,
};
use super::super::app::{App, Panel};

fn fmt_tokens(n: u64) -> String {
    if n >= 1_000_000 { format!("{:.1}M", n as f64 / 1_000_000.0) }
    else if n >= 10_000 { format!("{:.1}k", n as f64 / 1_000.0) }
    else if n >= 1_000 { format!("{}k", n / 1_000) }
    else { n.to_string() }
}

pub fn draw(f: &mut Frame, area: Rect, app: &App) {
    let t = app.theme;
    let panel_label = match app.current_panel {
        Panel::Chat => "Chat", Panel::Gateway => "Gateway",
        Panel::Files => "Files", Panel::Memory => "Memory",
        Panel::Skills => "Skills", Panel::Delegate => "Delegates",
    };
    let status = if app.turn_active {
        format!("{} Thinking", app.spinner_char())
    } else {
        String::from("✓ Ready")
    };
    let mut spans = vec![
        Span::styled("Cowd", Style::default().fg(t.accent()).bold()),
        Span::styled(format!(" │ {panel_label} │ {status} │ {}", app.model), Style::default().fg(t.fg())),
    ];
    if app.input_tokens > 0 || app.output_tokens > 0 {
        spans.push(Span::styled(
            format!(" │ in:{} out:{}", fmt_tokens(app.input_tokens), fmt_tokens(app.output_tokens)),
            Style::default().fg(Color::DarkGray),
        ));
    }
    if app.turn_active && (app.turn_input_tokens > 0 || app.turn_output_tokens > 0) {
        spans.push(Span::styled(
            format!(" │ turn:in:{} out:{}", fmt_tokens(app.turn_input_tokens), fmt_tokens(app.turn_output_tokens)),
            Style::default().fg(Color::Yellow),
        ));
    }
    if app.compaction_count > 0 {
        spans.push(Span::styled(format!(" │ cmp:{}", app.compaction_count), Style::default().fg(Color::DarkGray)));
    }
    if app.cache_hits > 0 {
        spans.push(Span::styled(format!(" │ cache:{}", app.cache_hits), Style::default().fg(Color::Green)));
    }
    let text = Line::from(spans);
    f.render_widget(Paragraph::new(text).style(Style::default().bg(t.bg())), area);
}
