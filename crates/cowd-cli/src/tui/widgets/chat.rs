use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use super::super::app::{App, Theme, TimelineEntry};

pub fn draw(f: &mut Frame, area: Rect, app: &mut App) {
    // ── Render cache: rebuild only when data has changed ──
    if app.msg_version != app.last_drawn_version {
        // Full rebuild due to structural change
        app.cached_chat_lines = build_new_lines(app);
        app.entry_line_counts = compute_entry_line_counts(app);
        app.last_drawn_version = app.msg_version;
        app.lines_dirty = false;
    } else if app.lines_dirty {
        // Incremental rebuild for streaming updates (no structural change)
        rebuild_streaming_tail(app);
        app.lines_dirty = false;
    }

    let content_height = app.cached_chat_lines.len() as u16;
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
    let paragraph = Paragraph::new(Text::from(app.cached_chat_lines.clone()))
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

/// Rebuild only the last entry (being streamed) in-place.
/// Avoids iterating the entire timeline on every TextDelta.
fn rebuild_streaming_tail(app: &mut App) {
    let n = app.timeline.len();
    if n == 0 { return; }

    // Compute prefix line count (all entries except the last one, plus their separator blanks)
    let prefix_count: usize = app.entry_line_counts.iter()
        .take(n.saturating_sub(1))
        .sum::<u16>()
        .saturating_add((n.saturating_sub(1)) as u16) // separator blanks for previous entries
        as usize;

    // Clone data we need before mutable borrow of cached_chat_lines
    let last_entry = app.timeline[n - 1].clone();
    let is_focused = (n - 1) == app.timeline_cursor;
    let theme = app.theme;
    let turn_active = app.turn_active;
    let spinner_str = if turn_active { Some(app.spinner_char().to_string()) } else { None };

    // Now do the mutable operations
    app.cached_chat_lines.truncate(prefix_count.min(app.cached_chat_lines.len()));
    let before_len = app.cached_chat_lines.len();
    build_entry(&last_entry, is_focused, &mut app.cached_chat_lines, theme);
    app.cached_chat_lines.push(Line::raw(""));

    // Update the line count for the last entry
    if let Some(count) = app.entry_line_counts.get_mut(n - 1) {
        *count = (app.cached_chat_lines.len() - before_len) as u16;
    }

    // Re-add loading spinner if turn is active
    if let Some(spinner) = spinner_str {
        app.cached_chat_lines.push(Line::from(vec![
            Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(Color::Blue),
            ),
        ]));
    }
}

/// Pre-compute per-entry line counts for virtual scrolling / incremental rebuild.
fn compute_entry_line_counts(app: &App) -> Vec<u16> {
    app.timeline.iter()
        .map(|e| e.expanded_lines() as u16)
        .collect()
}

/// Full rebuild of chat lines from app state.
fn build_new_lines(app: &App) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();

    if app.timeline.is_empty() {
        lines.push(Line::from(Span::styled(
            "Type to start. /help /resume /exit",
            Style::default().fg(Color::DarkGray),
        )));
    }

    for (idx, entry) in app.timeline.iter().enumerate() {
        let is_focused = idx == app.timeline_cursor;
        build_entry(entry, is_focused, &mut lines, app.theme);
        // Add a blank separator line between entries
        lines.push(Line::raw(""));
    }

    // Loading spinner at the bottom when turn is active
    if app.turn_active {
        let spinner = app.spinner_char();
        lines.push(Line::from(vec![
            Span::styled(
                format!("{spinner} Processing..."),
                Style::default().fg(Color::Blue),
            ),
        ]));
    }

    lines
}

/// Build ratatui Lines for a single timeline entry, using owned strings for caching.
pub fn build_entry(entry: &TimelineEntry, is_focused: bool, lines: &mut Vec<Line<'static>>, theme: Theme) {
    match entry {
        TimelineEntry::Message { role, content } => {
            let (color, prefix) = match role.as_str() {
                "user" => (theme.user_color(), "> "),
                "system" => (Color::DarkGray, "  "),
                _ => (theme.fg(), ""),
            };
            for line in content.lines() {
                lines.push(Line::from(vec![
                    Span::styled(prefix.to_string(), Style::default().fg(color).bold()),
                    Span::styled(line.to_string(), Style::default().fg(color)),
                ]));
            }
        }

        TimelineEntry::Thinking { id: _, content, complete, expanded } => {
            let line_count = content.lines().count();
            let status = if *complete { "complete" } else { "thinking" };
            let focus_marker = if is_focused { "● " } else { "  " };

            if *expanded {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}┌─ 💭 Thinking [{status}] ({line_count} lines)"),
                        Style::default().fg(Color::Cyan),
                    ),
                    Span::styled(
                        if is_focused { "[Enter=collapse]".to_string() } else { String::new() },
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));

                for line in content.lines().take(200) {
                    lines.push(Line::from(vec![
                        Span::styled("│  ".to_string(), Style::default().fg(Color::Cyan)),
                        Span::styled(line.to_string(), Style::default().fg(Color::DarkGray)),
                    ]));
                }
                if content.lines().count() > 200 {
                    lines.push(Line::from(vec![
                        Span::styled("│  ".to_string(), Style::default().fg(Color::Cyan)),
                        Span::styled(
                            format!("... ({} more lines)", content.lines().count() - 200),
                            Style::default().fg(Color::DarkGray),
                        ),
                    ]));
                }
                lines.push(Line::from(Span::styled(
                    "└─".to_string(),
                    Style::default().fg(Color::Cyan),
                )));
            } else {
                let preview: String = content.chars().take(80).collect();
                let more = if content.len() > 80 { "..." } else { "" };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}💭 Thinking [{status}] ({line_count}L): {preview}{more}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        if is_focused && *complete { "[Enter=expand]".to_string() } else { String::new() },
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        TimelineEntry::ToolCall { id: _, name, preview, output, done, expanded, exit_code } => {
            let status_style = if *done {
                if exit_code == &Some(0) { Style::default().fg(Color::Green) }
                else { Style::default().fg(Color::Red) }
            } else {
                Style::default().fg(Color::Yellow)
            };
            let status_icon = if *done {
                if exit_code == &Some(0) { "✅" } else { "❌" }
            } else { "⏳" };
            let status_text = if *done {
                format!("exit:{}", exit_code.unwrap_or(0))
            } else {
                "running...".to_string()
            };
            let focus_marker = if is_focused { "● " } else { "  " };

            if *expanded && !output.is_empty() {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}┌─ 🔧 {name}"),
                        Style::default().fg(Color::Yellow).bold(),
                    ),
                    Span::styled(
                        format!(" [{status_text}]"),
                        status_style,
                    ),
                    Span::styled(
                        if is_focused { "[Enter=collapse]".to_string() } else { String::new() },
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
                let display_lines: Vec<String> = output.lines().take(100).map(|s| s.to_string()).collect();
                for line in &display_lines {
                    lines.push(Line::from(Span::styled(
                        format!("│ {line}"),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                if output.lines().count() > 100 {
                    lines.push(Line::from(Span::styled(
                        format!("│ ... ({} more lines)", output.lines().count() - 100),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                lines.push(Line::from(Span::styled("└─".to_string(), Style::default().fg(Color::Yellow))));
            } else {
                let preview_text = if preview.is_empty() { name.as_str() } else { preview.as_str() };
                let short_preview: String = preview_text.chars().take(60).collect();
                let more = if preview_text.len() > 60 { "..." } else { "" };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}🔧 {name}"),
                        Style::default().fg(Color::Yellow).bold(),
                    ),
                    Span::styled(
                        format!(": {short_preview}{more}"),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(
                        format!(" [{status_icon} {status_text}]"),
                        status_style,
                    ),
                    Span::styled(
                        if is_focused && *done { "[Enter=expand]".to_string() } else { String::new() },
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }

        TimelineEntry::SlashOutput { command, output, expanded } => {
            let line_count = output.lines().count();
            let focus_marker = if is_focused { "● " } else { "  " };

            if *expanded {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}┌─ /{command} ({line_count} lines)"),
                        Style::default().fg(Color::Magenta).bold(),
                    ),
                    Span::styled(
                        if is_focused { "[Enter=collapse] [Ctrl+Y=copy]".to_string() } else { String::new() },
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
                for line in output.lines().take(100) {
                    lines.push(Line::from(vec![
                        Span::styled("│  ".to_string(), Style::default().fg(Color::Magenta)),
                        Span::styled(line.to_string(), Style::default().fg(Color::White)),
                    ]));
                }
                if output.lines().count() > 100 {
                    lines.push(Line::from(Span::styled(
                        format!("│ ... ({} more lines)", output.lines().count() - 100),
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                lines.push(Line::from(Span::styled(
                    "└─".to_string(),
                    Style::default().fg(Color::Magenta),
                )));
            } else {
                let preview: String = output.chars().take(80).collect();
                let more = if output.len() > 80 { "..." } else { "" };
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}/ {command} ({line_count}L): {preview}{more}"),
                        Style::default().fg(Color::Magenta).bold(),
                    ),
                    Span::styled(
                        if is_focused { "[Enter=expand] [Ctrl+Y=copy]".to_string() } else { String::new() },
                        Style::default().fg(Color::DarkGray),
                    ),
                ]));
            }
        }
    }
}
