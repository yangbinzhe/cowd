pub mod layout;
pub mod model;

use std::{cell::RefCell, ops::Range, rc::Rc};

use crate::components::context_suggestions::ContextSuggestions;
use crate::components::prompt::Prompt;
use crate::components::RenderContext;
use ratatui::{
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use self::model::ComposerModel;

#[derive(Debug, Clone)]
struct ComposerLayoutCache {
    revision: u64,
    content_width: u16,
    layout: Rc<layout::ComposerLayout>,
}

#[derive(Debug, Clone, Default)]
pub struct Composer {
    pub mode_label: String,
    pub last_pending_resources: usize,
    /// Layout only changes when canonical editor bytes or content width
    /// changes. Keeping it here avoids re-wrapping every grapheme twice per
    /// frame (height calculation and render) during a streaming turn.
    layout_cache: RefCell<Option<ComposerLayoutCache>>,
}

impl Composer {
    pub fn new() -> Self {
        Self {
            mode_label: "Chat".to_string(),
            last_pending_resources: 0,
            layout_cache: RefCell::new(None),
        }
    }

    /// Calculate composer height from visual rows at the actual available
    /// width. The canonical model is never changed by resize or rendering.
    #[must_use]
    pub fn desired_height(&self, input: &ComposerModel, outer_width: u16, max_height: u16) -> u16 {
        let content_width = outer_width.saturating_sub(2).max(1);
        let max_content_height = max_height.saturating_sub(2).max(1);
        let layout = self.layout_for(input, content_width);
        layout
            .desired_content_height(max_content_height)
            .saturating_add(2)
            .clamp(3, max_height.max(3))
    }

    pub fn render(
        &mut self,
        ctx: &mut RenderContext,
        area: Rect,
        input: &ComposerModel,
        prompt: &mut Prompt,
        context_suggestions: &mut ContextSuggestions,
        pending_resources: usize,
        queued_follow_ups: usize,
        queued_preview: Option<&str>,
    ) {
        self.last_pending_resources = pending_resources;
        let resource_hint = if pending_resources == 0 {
            String::new()
        } else {
            format!(" · {pending_resources} resource(s)")
        };
        let queue_hint = if queued_follow_ups == 0 {
            String::new()
        } else if area.width < 56 {
            format!(" · {queued_follow_ups} queued")
        } else {
            let preview = queued_preview
                .map(compact_queue_preview)
                .filter(|preview| !preview.is_empty())
                .unwrap_or_else(|| "follow-up".to_string());
            format!(" · {queued_follow_ups} queued: {preview}")
        };
        let active = !matches!(
            self.mode_label.as_str(),
            "Chat" | "Runtime: Completed" | "Runtime: Failed" | "Runtime: Cancelled"
        );
        let running_hint = if active { " · Esc cancel" } else { "" };
        let block = Block::default().borders(Borders::ALL).title(format!(
            " {} · Enter send · Ctrl+J newline · Ctrl+P actions{}{}{} ",
            self.mode_label, resource_hint, queue_hint, running_hint
        ));
        let layout = self.layout_for(input, area.width.saturating_sub(2));
        let viewport = layout.viewport(area.height.saturating_sub(2));
        let lines = viewport
            .rows
            .iter()
            .map(|row| composer_line(row, input.selection_range()))
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

    fn layout_for(&self, input: &ComposerModel, content_width: u16) -> Rc<layout::ComposerLayout> {
        let content_width = content_width.max(1);
        {
            let cache = self.layout_cache.borrow();
            if let Some(cache) = cache.as_ref() {
                if cache.revision == input.revision() && cache.content_width == content_width {
                    return Rc::clone(&cache.layout);
                }
            }
        }
        let layout = Rc::new(layout::ComposerLayout::from_model(input, content_width));
        *self.layout_cache.borrow_mut() = Some(ComposerLayoutCache {
            revision: input.revision(),
            content_width,
            layout: Rc::clone(&layout),
        });
        layout
    }
}

fn compact_queue_preview(value: &str) -> String {
    const LIMIT: usize = 28;
    let compact = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut chars = compact.chars();
    let preview = chars.by_ref().take(LIMIT).collect::<String>();
    if chars.next().is_some() {
        format!("{preview}…")
    } else {
        preview
    }
}

fn composer_line(
    row: &layout::ComposerVisualRow,
    selection: Option<Range<usize>>,
) -> Line<'static> {
    let Some(selection) = selection else {
        return Line::styled(row.text.clone(), Style::default());
    };
    let spans =
        unicode_segmentation::UnicodeSegmentation::grapheme_indices(row.text.as_str(), true)
            .map(|(offset, grapheme)| {
                let start = row.start_byte.saturating_add(offset);
                let end = start.saturating_add(grapheme.len());
                let selected = start < selection.end && selection.start < end;
                let style = if selected {
                    Style::default().bg(Color::DarkGray).fg(Color::White)
                } else {
                    Style::default()
                };
                Span::styled(grapheme.to_string(), style)
            })
            .collect::<Vec<_>>();
    Line::from(spans)
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
        let input = ComposerModel::new("中文🙂中文🙂");
        let original = input.text().to_string();
        assert!(composer.desired_height(&input, 8, 12) > 3);
        assert_eq!(input.text(), original);
    }

    #[test]
    fn layout_cache_is_keyed_by_model_revision_and_content_width() {
        let composer = Composer::new();
        let mut input = ComposerModel::new("中文🙂中文🙂");
        let first = composer.layout_for(&input, 8);
        let same = composer.layout_for(&input, 8);
        assert!(Rc::ptr_eq(&first, &same));

        let resized = composer.layout_for(&input, 12);
        assert!(!Rc::ptr_eq(&first, &resized));

        input.insert("x");
        let edited = composer.layout_for(&input, 12);
        assert!(!Rc::ptr_eq(&resized, &edited));
    }

    #[test]
    fn selection_is_split_on_grapheme_boundaries_for_rendering() {
        let input = ComposerModel::new("a🙂e\u{301}");
        let row = layout::ComposerLayout::from_model(&input, 20)
            .rows
            .remove(0);
        let line = composer_line(&row, Some(1.."a🙂".len()));
        assert_eq!(line.spans.len(), 3);
        assert_eq!(line.spans[1].content, "🙂");
    }

    #[test]
    fn queue_preview_is_compact_without_rewriting_authored_text() {
        assert_eq!(
            compact_queue_preview("  follow   up  with 中文🙂 and detail"),
            "follow up with 中文🙂 and detai…"
        );
        assert!(compact_queue_preview(&"x".repeat(40)).ends_with('…'));
    }
}
