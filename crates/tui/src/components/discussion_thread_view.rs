#![allow(dead_code)]

use std::collections::BTreeMap;

use crossterm::event::{Event, KeyCode};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
};

use crate::components::{Component, EventResult, RenderContext};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiscussionPhase {
    Contributing,
    Synthesizing,
    CheckingConsensus,
    Complete,
}

#[derive(Debug, Clone)]
pub struct Contribution {
    pub agent_id: String,
    pub round: u32,
    pub content: String,
    pub confidence: f32,
}

#[derive(Debug, Clone)]
pub struct Discussion {
    pub topic: String,
    pub phase: DiscussionPhase,
    pub current_round: u32,
    pub max_rounds: u32,
    pub contributions: BTreeMap<u32, Vec<Contribution>>,
    pub consensus_summary: Option<String>,
}

pub struct DiscussionThreadView {
    pub discussion: Option<Discussion>,
    pub visible: bool,
    pub scroll_offset: usize,
}

impl DiscussionThreadView {
    #[must_use]
    pub fn new() -> Self {
        Self {
            discussion: None,
            visible: false,
            scroll_offset: 0,
        }
    }

    pub fn sync_from_projection(&mut self, discussion: Option<Discussion>) {
        self.discussion = discussion;
    }

    pub fn toggle(&mut self) {
        self.visible = !self.visible;
    }

    fn phase_color(phase: DiscussionPhase) -> Color {
        match phase {
            DiscussionPhase::Contributing => Color::Blue,
            DiscussionPhase::Synthesizing => Color::Yellow,
            DiscussionPhase::CheckingConsensus => Color::Cyan,
            DiscussionPhase::Complete => Color::Green,
        }
    }

    fn phase_label(phase: DiscussionPhase) -> &'static str {
        match phase {
            DiscussionPhase::Contributing => "Contributing",
            DiscussionPhase::Synthesizing => "Synthesizing",
            DiscussionPhase::CheckingConsensus => "Checking Consensus",
            DiscussionPhase::Complete => "Complete",
        }
    }

    fn format_confidence(confidence: f32) -> String {
        format!("{:.0}%", confidence * 100.0)
    }
}

impl Default for DiscussionThreadView {
    fn default() -> Self {
        Self::new()
    }
}

impl Component for DiscussionThreadView {
    fn render(&mut self, ctx: &mut RenderContext, area: Rect) {
        let Some(discussion) = &self.discussion else {
            let block = Block::default()
                .title(" Discussion ")
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray));
            ctx.frame_mut().render_widget(
                Paragraph::new("No active discussion.")
                    .block(block)
                    .style(Style::default().fg(Color::DarkGray)),
                area,
            );
            return;
        };

        let mut lines = Vec::new();
        lines.push(Line::from(vec![
            Span::styled("Topic: ", Style::default().fg(Color::DarkGray)),
            Span::styled(discussion.topic.clone(), Style::default().fg(Color::White)),
        ]));
        lines.push(Line::from(vec![
            Span::styled("Phase: ", Style::default().fg(Color::DarkGray)),
            Span::styled(
                Self::phase_label(discussion.phase),
                Style::default()
                    .fg(Self::phase_color(discussion.phase))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("  {}/{}", discussion.current_round, discussion.max_rounds),
                Style::default().fg(Color::DarkGray),
            ),
        ]));
        lines.push(Line::raw(""));

        for (round, contributions) in &discussion.contributions {
            lines.push(Line::from(Span::styled(
                format!("Round {round}"),
                Style::default().fg(Color::Cyan),
            )));
            for contribution in contributions {
                lines.push(Line::from(vec![
                    Span::styled("  ", Style::default()),
                    Span::styled(&contribution.agent_id, Style::default().fg(Color::Yellow)),
                    Span::styled(
                        format!(" ({}) ", Self::format_confidence(contribution.confidence)),
                        Style::default().fg(Color::DarkGray),
                    ),
                    Span::styled(&contribution.content, Style::default().fg(Color::White)),
                ]));
            }
        }

        if let Some(summary) = &discussion.consensus_summary {
            lines.push(Line::raw(""));
            lines.push(Line::from(vec![
                Span::styled("Consensus: ", Style::default().fg(Color::Green)),
                Span::styled(summary.clone(), Style::default().fg(Color::White)),
            ]));
        }

        let block = Block::default()
            .title(format!(
                " Discussion [{}] ",
                Self::phase_label(discussion.phase)
            ))
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Self::phase_color(discussion.phase)));
        ctx.frame_mut().render_widget(
            Paragraph::new(lines)
                .block(block)
                .wrap(Wrap { trim: false }),
            area,
        );
    }

    fn handle_event(&mut self, event: &Event) -> EventResult {
        let Event::Key(key) = event else {
            return EventResult::NotConsumed;
        };
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => {
                self.scroll_offset = self.scroll_offset.saturating_add(1);
                EventResult::Consumed
            }
            KeyCode::Up | KeyCode::Char('k') => {
                self.scroll_offset = self.scroll_offset.saturating_sub(1);
                EventResult::Consumed
            }
            _ => EventResult::NotConsumed,
        }
    }

    fn focusable(&self) -> bool {
        true
    }

    fn id(&self) -> &str {
        "discussion_thread_view"
    }
}
