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

/// Build a character-based progress bar: "██████░░░░░░ 6.2K / 128K (39%)"
fn token_bar(app: &App) -> Option<String> {
    let window = app.context_window;
    if window == 0 { return None; }
    let used = app.token_count;
    let pct = (used as f64 / window as f64 * 100.0).min(100.0);
    let bar_width: i32 = 12;
    let filled = ((pct / 100.0) * bar_width as f64).round() as usize;
    let empty = (bar_width as usize).saturating_sub(filled);

    // Use █ for filled, ░ for empty
    let mut bar = String::with_capacity(bar_width as usize + 32);
    for _ in 0..filled { bar.push('█'); }
    for _ in 0..empty { bar.push('░'); }

    Some(format!(
        "{} {}/{} ({:.0}%)",
        bar,
        fmt_tokens(used),
        fmt_tokens(window),
        pct
    ))
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

    // Token progress bar
    if let Some(bar) = token_bar(app) {
        spans.push(Span::styled(
            format!(" │ {bar}"),
            Style::default().fg(Color::DarkGray),
        ));
    }

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
    if app.search_active {
        spans.push(Span::styled(
            format!(" │ /{}", app.search_query),
            Style::default().fg(Color::Yellow).bold(),
        ));
    } else if !app.search_matches.is_empty() {
        spans.push(Span::styled(
            format!(" │ /{} [{}/{}]", app.search_query, app.search_current + 1, app.search_matches.len()),
            Style::default().fg(Color::Yellow),
        ));
    }
    if let Some(hidx) = app.history_idx {
        spans.push(Span::styled(
            format!(" │ hist:{}", hidx + 1),
            Style::default().fg(Color::DarkGray),
        ));
    }

    let text = Line::from(spans);
    f.render_widget(Paragraph::new(text).style(Style::default().bg(t.bg())), area);

    // ── Notification banner (above status line if present) ──
    if let Some(ref notification) = app.notification {
        let note_span = Span::styled(
            notification.clone(),
            Style::default().fg(Color::Black).bg(Color::Cyan),
        );
        let note_line = Line::from(vec![note_span]);
        f.render_widget(
            Paragraph::new(note_line).style(Style::default().bg(Color::Cyan)),
            area, // overlay on the same area — notification overlays status line
        );
    }
}
