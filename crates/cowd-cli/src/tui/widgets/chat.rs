use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Clear, Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use super::super::app::{App, Theme, TimelineEntry};

pub fn draw(f: &mut Frame, area: Rect, app: &mut App) {
    let viewport_h = area.height as usize;

    // ── Compute total content lines (even for virtual-mode, used by scrollbar) ──
    let mut total_lines: usize = app.entry_line_counts.iter()
        .map(|&c| c as usize + 1) // +1 for blank separator between entries
        .sum::<usize>();
    if total_lines == 0 && app.timeline.is_empty() {
        total_lines = 1; // placeholder line
    }
    if app.turn_active {
        total_lines += 1; // spinner line
    }

    // ── Auto-scroll logic ──
    if app.auto_scroll && total_lines > viewport_h {
        app.scroll_offset = (total_lines - viewport_h) as u16;
    }
    let scroll_off = app.scroll_offset.min(total_lines.saturating_sub(1) as u16) as usize;

    // ── Build visible lines ──
    let visible_lines: Vec<Line<'static>>;
    let paragraph_scroll: u16;

    // Store viewport height for PageUp/PageDown calculations
    app.viewport_height = viewport_h as u16;

    // Threshold: if content is more than 3× viewport, use virtual scrolling
    if total_lines > viewport_h.saturating_mul(3) {
        // Virtual scrolling: only build entries visible in the viewport
        visible_lines = build_visible(app, scroll_off, viewport_h);
        paragraph_scroll = 0; // visible_lines already starts at the right offset
    } else {
        // Small timeline: use render cache with full-line rebuild
        if app.msg_version != app.last_drawn_version {
            app.cached_chat_lines = build_new_lines(app);
            app.entry_line_counts = compute_entry_line_counts(app);
            app.last_drawn_version = app.msg_version;
            app.lines_dirty = false;
        } else if app.lines_dirty {
            rebuild_streaming_tail(app);
            app.entry_line_counts = compute_entry_line_counts(app);
            app.lines_dirty = false;
        }
        visible_lines = app.cached_chat_lines.clone();
        paragraph_scroll = scroll_off as u16;
    }

    // ── Layout & render ──
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
    let paragraph = Paragraph::new(Text::from(visible_lines))
        .wrap(Wrap { trim: false })
        .scroll((paragraph_scroll, 0));
    f.render_widget(paragraph, inner_area);

    // ── Scrollbar ──
    if total_lines > viewport_h {
        let scrollbar = Scrollbar::new(ScrollbarOrientation::VerticalRight)
            .begin_symbol(Some("↑"))
            .end_symbol(Some("↓"))
            .track_symbol(Some("│"))
            .thumb_symbol("█");
        let mut scroll_state = ScrollbarState::new(total_lines)
            .position(scroll_off)
            .viewport_content_length(viewport_h);
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

/// Build only the entries visible within [scroll_offset, scroll_offset + viewport_height].
/// Uses entry_line_counts for O(entries) offset computation without rendering off-screen entries.
fn build_visible(app: &App, scroll_offset: usize, viewport_h: usize) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut cumulative: usize = 0;
    let viewport_end = scroll_offset + viewport_h;

    if app.timeline.is_empty() {
        lines.push(Line::from(Span::styled(
            "Type to start. /help /resume /exit",
            Style::default().fg(Color::DarkGray),
        )));
        return lines;
    }

    for (idx, entry) in app.timeline.iter().enumerate() {
        let entry_lines = app.entry_line_counts.get(idx).copied().unwrap_or(1) as usize + 1; // +1 for separator blank
        let entry_end = cumulative + entry_lines;

        // Include entry if it overlaps the visible viewport
        if entry_end > scroll_offset && cumulative < viewport_end {
            let is_focused = idx == app.timeline_cursor;
            build_entry(entry, is_focused, &mut lines, app.theme);
            lines.push(Line::raw(""));
        }

        cumulative = entry_end;
        if cumulative >= viewport_end {
            break;
        }
    }

    // Loading spinner at the bottom when turn is active and viewport reaches the end
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

/// Highlight inline markdown spans in a message line.
/// Handles: `code` (yellow), **bold** (bold), *italic* (italic/dim).
fn highlight_line(line: &str, spans: &mut Vec<Span<'static>>, base_color: Color) {
    let mut remaining = line;
    while !remaining.is_empty() {
        // Find the next markdown token: backtick, double-star, or single-star
        let bt = remaining.find('`');
        let bs = remaining.find("**");
        let is = remaining.find('*');

        // Determine which token comes first (if any)
        let earliest = [bt, bs, is].iter().filter_map(|&o| o).min();

        match earliest {
            None => {
                // No more markdown — push remaining text as-is
                spans.push(Span::styled(
                    remaining.to_string(),
                    Style::default().fg(base_color),
                ));
                break;
            }
            Some(pos) => {
                // Push text before the token
                if pos > 0 {
                    spans.push(Span::styled(
                        remaining[..pos].to_string(),
                        Style::default().fg(base_color),
                    ));
                }

                // Determine token type
                if bt == Some(pos) {
                    // Inline code: `...`
                    remaining = &remaining[pos + 1..];
                    if let Some(end) = remaining.find('`') {
                        spans.push(Span::styled(
                            remaining[..end].to_string(),
                            Style::default().fg(Color::Yellow),
                        ));
                        remaining = &remaining[end + 1..];
                    } else {
                        // Unclosed backtick — treat rest as code
                        spans.push(Span::styled(
                            remaining.to_string(),
                            Style::default().fg(Color::Yellow),
                        ));
                        remaining = "";
                    }
                } else if bs == Some(pos) {
                    // Bold: **...**
                    remaining = &remaining[pos + 2..];
                    if let Some(end) = remaining.find("**") {
                        spans.push(Span::styled(
                            remaining[..end].to_string(),
                            Style::default().fg(base_color).bold(),
                        ));
                        remaining = &remaining[end + 2..];
                    } else {
                        // Unclosed ** — treat rest as bold
                        spans.push(Span::styled(
                            remaining.to_string(),
                            Style::default().fg(base_color).bold(),
                        ));
                        remaining = "";
                    }
                } else if is == Some(pos) {
                    // Italic: *...*
                    remaining = &remaining[pos + 1..];
                    if let Some(end) = remaining.find('*') {
                        // Make sure it's not a **
                        if remaining.get(end + 1..end + 2) == Some("*") {
                            // False positive: single * followed by *, treat as text
                            spans.push(Span::styled(
                                format!("*{}", &remaining[..end]),
                                Style::default().fg(base_color),
                            ));
                            remaining = &remaining[end..];
                        } else {
                            spans.push(Span::styled(
                                remaining[..end].to_string(),
                                Style::default().fg(base_color).italic(),
                            ));
                            remaining = &remaining[end + 1..];
                        }
                    } else {
                        // Unclosed * — treat rest as italic
                        spans.push(Span::styled(
                            remaining.to_string(),
                            Style::default().fg(base_color).italic(),
                        ));
                        remaining = "";
                    }
                } else {
                    // Shouldn't happen — push the char and continue
                    spans.push(Span::styled(
                        remaining[..1].to_string(),
                        Style::default().fg(base_color),
                    ));
                    remaining = &remaining[1..];
                }
            }
        }
    }
}

/// Build ratatui Lines for a single timeline entry, using owned strings for caching.
pub fn build_entry(entry: &TimelineEntry, is_focused: bool, lines: &mut Vec<Line<'static>>, theme: Theme) {
    match entry {
        TimelineEntry::Message { role, content, .. } => {
            let (color, prefix) = match role.as_str() {
                "user" => (theme.user_color(), "> "),
                "system" => (Color::DarkGray, "  "),
                _ => (theme.fg(), ""),
            };
            let total_lines = content.lines().count();
            const MAX_LINES: usize = 500;
            for (i, line) in content.lines().enumerate() {
                if i >= MAX_LINES {
                    lines.push(Line::from(Span::styled(
                        format!("  ... ({} more lines truncated)", total_lines.saturating_sub(MAX_LINES)),
                        Style::default().fg(Color::DarkGray),
                    )));
                    break;
                }
                let mut spans = vec![
                    Span::styled(prefix.to_string(), Style::default().fg(color).bold()),
                ];
                highlight_line(line, &mut spans, color);
                lines.push(Line::from(spans));
            }
        }

        TimelineEntry::Thinking { id: _, content, complete, expanded } => {
            // Pre-compute once to avoid O(n²) repeated traversal
            let total_lines = content.lines().count();
            let status = if *complete { "complete" } else { "thinking" };
            let focus_marker = if is_focused { "● " } else { "  " };

            if *expanded {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}┌─ 💭 Thinking [{status}] ({total_lines} lines)"),
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
                if total_lines > 200 {
                    lines.push(Line::from(vec![
                        Span::styled("│  ".to_string(), Style::default().fg(Color::Cyan)),
                        Span::styled(
                            format!("... ({} more lines)", total_lines - 200),
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
                        format!("{focus_marker}💭 Thinking [{status}] ({total_lines}L): {preview}{more}"),
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
                // Pre-compute total lines to avoid repeated O(n) traversal
                let total_lines = output.lines().count();
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
                if total_lines > 100 {
                    lines.push(Line::from(Span::styled(
                        format!("│ ... ({} more lines)", total_lines - 100),
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
            // Pre-compute total lines to avoid repeated O(n) traversal
            let total_lines = output.lines().count();
            let focus_marker = if is_focused { "● " } else { "  " };

            if *expanded {
                lines.push(Line::from(vec![
                    Span::styled(
                        format!("{focus_marker}┌─ /{command} ({total_lines} lines)"),
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
                if total_lines > 100 {
                    lines.push(Line::from(Span::styled(
                        format!("│ ... ({} more lines)", total_lines - 100),
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
                        format!("{focus_marker}/ {command} ({total_lines}L): {preview}{more}"),
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
