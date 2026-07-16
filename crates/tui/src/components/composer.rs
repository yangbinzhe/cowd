pub mod layout;

use ratatui::{
    layout::Rect,
    style::Style,
    text::Line,
    widgets::{Block, Borders, Paragraph, Wrap},
};
use tui_textarea::TextArea;

use crate::components::context_suggestions::ContextSuggestions;
use crate::components::prompt::Prompt;
use crate::components::RenderContext;

#[derive(Debug, Clone, Default)]
pub struct Composer {
    pub mode_label: String,
    pub last_pending_resources: usize,
}

impl Composer {
    pub fn new() -> Self {
        Self {
            mode_label: "Chat".to_string(),
            last_pending_resources: 0,
        }
    }

    /// Calculate composer height from visual rows at the actual available
    /// width.  The textarea remains the canonical input model; resize only
    /// invalidates this derived layout.
    #[must_use]
    pub fn desired_height(&self, input: &TextArea<'_>, outer_width: u16, max_height: u16) -> u16 {
        let content_width = outer_width.saturating_sub(2).max(1);
        let max_content_height = max_height.saturating_sub(2).max(1);
        let layout = layout::ComposerLayout::from_textarea(input, content_width);
        layout
            .desired_content_height(max_content_height)
            .saturating_add(2)
            .clamp(3, max_height.max(3))
    }

    pub fn render(
        &mut self,
        ctx: &mut RenderContext,
        area: Rect,
        input: &mut TextArea<'static>,
        prompt: &mut Prompt,
        context_suggestions: &mut ContextSuggestions,
        pending_resources: usize,
    ) {
        self.last_pending_resources = pending_resources;
        let resource_hint = if pending_resources == 0 {
            String::new()
        } else {
            format!(" · {pending_resources} resource(s)")
        };
        let block = Block::default().borders(Borders::ALL).title(format!(
            " {} Composer (Enter=send, Ctrl+J newline, Ctrl+P actions{}) ",
            self.mode_label, resource_hint
        ));
        let layout = layout::ComposerLayout::from_textarea(input, area.width.saturating_sub(2));
        let viewport = layout.viewport(area.height.saturating_sub(2));
        let lines = viewport
            .rows
            .iter()
            .map(|row| Line::styled(row.text.clone(), Style::default()))
            .collect::<Vec<_>>();
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
        let cursor_x = area
            .x
            .saturating_add(1)
            .saturating_add(viewport.cursor.column)
            .min(area.right().saturating_sub(1));
        let cursor_y = area
            .y
            .saturating_add(1)
            .saturating_add(u16::try_from(viewport.cursor.visual_row).unwrap_or(u16::MAX))
            .min(area.bottom().saturating_sub(1));
        ctx.frame_mut().set_cursor_position((cursor_x, cursor_y));
        prompt.render_dropdown(ctx, area);
        if context_suggestions.is_active() && !prompt.suggestions_visible() {
            context_suggestions.render(ctx, area);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composer_tracks_pending_resources() {
        let mut composer = Composer::new();
        composer.last_pending_resources = 4;
        assert_eq!(composer.last_pending_resources, 4);
    }

    #[test]
    fn desired_height_uses_visual_rows_without_mutating_input() {
        let composer = Composer::new();
        let mut input = TextArea::default();
        input.insert_str("中文🙂中文🙂");
        let original = input.lines().join("\n");
        assert!(composer.desired_height(&input, 8, 12) > 3);
        assert_eq!(input.lines().join("\n"), original);
    }
}
