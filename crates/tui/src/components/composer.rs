use ratatui::layout::Rect;
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
        input.set_block(
            ratatui::widgets::Block::default()
                .borders(ratatui::widgets::Borders::ALL)
                .title(format!(
                    " {} Composer (Enter=send, Ctrl+J newline, Ctrl+P actions{}) ",
                    self.mode_label, resource_hint
                )),
        );
        ctx.frame_mut().render_widget(&*input, area);
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
}
