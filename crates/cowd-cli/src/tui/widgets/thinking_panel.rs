use ratatui::{
    layout::Rect,
    style::{Color, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Paragraph, Scrollbar, ScrollbarOrientation, ScrollbarState, Wrap},
    Frame,
};
use super::super::app::App;

pub fn draw(f: &mut Frame, area: Rect, app: &mut App) {
    if app.streaming_thinking.is_empty() {
        return;
    }

    let status = if app.thinking_complete { "complete" } else { "thinking" };
    let title = if app.thinking_expanded {
        format!(" Thinking ({status}) — ↑↓/PgUp/PgDn scroll ")
    } else {
        format!(" Thinking ({status}) ")
    };
    let block = ratatui::widgets::Block::default()
        .borders(ratatui::widgets::Borders::ALL)
        .title(title)
        .fg(Color::Cyan);

    let display_text = if app.thinking_expanded {
        let text = &app.streaming_thinking;
        let total = text.chars().count();
        if total > 30000 {
            let head: String = text.chars().take(15000).collect();
            let tail: String = text.chars().rev().take(15000).collect::<String>().chars().rev().collect();
            format!("{head}\n... ({} total chars, showing first & last 15k)\n...\n{tail}", total)
        } else {
            text.clone()
        }
    } else {
        let preview: String = app.streaming_thinking.chars().take(120).collect();
        let total = app.streaming_thinking.chars().count();
        format!("{preview}... ({} chars, Enter to expand)", total)
    };

    let text = Text::from(display_text);
    let content_height = text.height() as u16;
    let viewport_height = area.height.saturating_sub(2); // account for borders

    // Auto-scroll to bottom when thinking is streaming and auto-scroll is on
    if app.thinking_auto_scroll && !app.thinking_complete && content_height > viewport_height {
        app.thinking_scroll_offset = content_height.saturating_sub(viewport_height);
    }
    let scroll_offset = app.thinking_scroll_offset.min(content_height.saturating_sub(1));

    // Only show scrollbar when expanded
    let show_scrollbar = app.thinking_expanded && content_height > viewport_height;

    let text_area = if show_scrollbar {
        Rect {
            x: area.x,
            y: area.y,
            width: area.width.saturating_sub(1),
            height: area.height,
        }
    } else {
        area
    };

    f.render_widget(
        Paragraph::new(text)
            .block(block)
            .wrap(Wrap { trim: false })
            .scroll((scroll_offset, 0)),
        text_area,
    );

    if show_scrollbar {
        let scrollbar_area = Rect {
            x: area.right().saturating_sub(1),
            y: area.y,
            width: 1,
            height: area.height,
        };
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

pub fn height_for(app: &App) -> u16 {
    if app.streaming_thinking.is_empty() {
        0
    } else if app.thinking_expanded {
        12
    } else {
        5
    }
}
