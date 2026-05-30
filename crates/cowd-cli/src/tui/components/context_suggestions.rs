// ── ContextSuggestions — Bottom suggestion bar for L4 events ─────────
// 1-line floating bar that shows context-aware suggestions when an agent
// performs an L4 Insert operation. Auto-dismisses after TTL.
// Renders above the prompt line, below the chat area.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Clear, Paragraph},
};

use memory::L4Event;

use crate::tui::components::RenderContext;

/// Default time-to-live for a suggestion before auto-dismiss (5 seconds).
const DEFAULT_TTL: Duration = Duration::from_secs(5);

/// A single context-aware suggestion for the suggestion bar.
#[derive(Debug, Clone)]
struct Suggestion {
    /// Human-readable suggestion text (short description of the event).
    text: String,
    /// Action hint shown at the end (e.g., "Review? [Enter]").
    action_label: String,
    /// The original L4 event that triggered this suggestion.
    source_event: L4Event,
}

impl Suggestion {
    /// Build a suggestion from an L4 Insert event.
    fn from_insert_event(event: &L4Event) -> Self {
        let text = format!(
            "{} just completed '{}'.",
            event.agent_id, event.title
        );
        let action_label = " Review? [Enter]".to_string();
        Self {
            text,
            action_label,
            source_event: event.clone(),
        }
    }

    /// Full display text for the suggestion bar, including the bulb icon.
    fn display_text(&self) -> String {
        format!("💡 {}{}", self.text, self.action_label)
    }
}

/// Bottom floating suggestion bar showing context-aware suggestions
/// triggered by L4EventBus events (new Insert operations).
///
/// Non-intrusive: auto-dismisses after [`DEFAULT_TTL`] (5 seconds).
/// Renders as a single-line bar above the prompt input area.
pub struct ContextSuggestions {
    /// Currently active suggestion, if any.
    current: Option<Suggestion>,
    /// Instant when the current suggestion was shown (for TTL tracking).
    shown_at: Option<Instant>,
    /// Time-to-live before auto-dismissal. Default: 5 seconds.
    ttl: Duration,
    /// Subscriber to the L4 event bus for receiving push notifications.
    l4_rx: Option<tokio::sync::broadcast::Receiver<L4Event>>,
}

impl ContextSuggestions {
    /// Create a new empty suggestions component.
    pub fn new() -> Self {
        Self {
            current: None,
            shown_at: None,
            ttl: DEFAULT_TTL,
            l4_rx: None,
        }
    }

    /// Attach a subscriber to the L4 event bus.
    ///
    /// Call this once after the memory orchestrator is configured to start
    /// receiving L4 insert events.
    pub fn set_l4_receiver(&mut self, rx: tokio::sync::broadcast::Receiver<L4Event>) {
        self.l4_rx = Some(rx);
    }

    /// Advance time by one tick (call once per render loop iteration).
    ///
    /// Pumps pending L4 events from the broadcast channel and expires
    /// stale suggestions whose TTL has elapsed.
    pub fn tick(&mut self) {
        // Drain pending L4 events from the broadcast channel
        if let Some(rx) = &mut self.l4_rx {
            loop {
                match rx.try_recv() {
                    Ok(event) => {
                        // Only show suggestions for Insert operations
                        if matches!(event.operation, memory::L4Operation::Insert) {
                            self.current = Some(Suggestion::from_insert_event(&event));
                            self.shown_at = Some(Instant::now());
                        }
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                    Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                        // Channel closed — no more events; clear the receiver
                        self.l4_rx = None;
                        break;
                    }
                    Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                        // We missed some events — skip and continue draining
                        continue;
                    }
                }
            }
        }

        // Auto-dismiss expired suggestions
        if let Some(shown) = self.shown_at {
            if shown.elapsed() >= self.ttl {
                self.current = None;
                self.shown_at = None;
            }
        }
    }

    /// Returns `true` if there is an active suggestion to display.
    pub fn is_active(&self) -> bool {
        self.current.is_some()
    }

    /// Render the suggestion bar as a single floating line above `prompt_area`.
    ///
    /// The bar is drawn at `prompt_area.y - 1` with a `Clear` widget to
    /// prevent visual artifacts from previous frames.
    pub fn render(&self, ctx: &mut RenderContext, prompt_area: Rect) {
        let Some(ref suggestion) = self.current else {
            return;
        };

        let bar_y = prompt_area.y.saturating_sub(1);
        let bar_area = Rect::new(prompt_area.x, bar_y, prompt_area.width, 1);

        // Clear the area first (prevents artifacts from previous frames)
        ctx.frame_mut().render_widget(Clear, bar_area);

        // Build and render the display line
        let full_text = suggestion.display_text();
        let line = Line::from(Span::styled(
            full_text,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        ctx.frame_mut().render_widget(Paragraph::new(line), bar_area);
    }
}

impl Default for ContextSuggestions {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::RenderContext;
    use crate::tui::skin::SkinConfig;
    use crate::tui::test_utils::MockTerminal;
    use memory::{L4Event, L4Operation};
    use ratatui::Frame;

    // ── Basic lifecycle ─────────────────────────────────────────

    #[test]
    fn new_is_inactive() {
        let cs = ContextSuggestions::new();
        assert!(!cs.is_active());
    }

    #[test]
    fn insert_event_triggers_suggestion() {
        let event = L4Event {
            agent_id: "Alice".into(),
            memory_id: "mem-1".into(),
            operation: L4Operation::Insert,
            title: "auth refactor".into(),
            timestamp_ms: 1000,
        };

        let mut cs = ContextSuggestions::new();
        let (tx, rx) = tokio::sync::broadcast::channel(16);
        cs.set_l4_receiver(rx);

        tx.send(event).ok();
        cs.tick();

        assert!(cs.is_active());
    }

    // ── Event filtering ─────────────────────────────────────────

    #[test]
    fn ignores_update_and_delete_events() {
        let event_update = L4Event {
            agent_id: "Bob".into(),
            memory_id: "mem-2".into(),
            operation: L4Operation::Update,
            title: "docs update".into(),
            timestamp_ms: 2000,
        };
        let event_delete = L4Event {
            agent_id: "Bob".into(),
            memory_id: "mem-3".into(),
            operation: L4Operation::Delete,
            title: "old entry".into(),
            timestamp_ms: 3000,
        };

        let mut cs = ContextSuggestions::new();
        let (tx, rx) = tokio::sync::broadcast::channel(16);
        cs.set_l4_receiver(rx);

        tx.send(event_update).ok();
        tx.send(event_delete).ok();
        cs.tick();

        assert!(!cs.is_active(), "Update/Delete should not trigger suggestions");
    }

    // ── Auto-dismiss ────────────────────────────────────────────

    #[test]
    fn auto_dismisses_after_ttl() {
        let event = L4Event {
            agent_id: "Charlie".into(),
            memory_id: "mem-4".into(),
            operation: L4Operation::Insert,
            title: "test entry".into(),
            timestamp_ms: 4000,
        };

        let mut cs = ContextSuggestions::new();
        cs.ttl = Duration::from_millis(1);

        let (tx, rx) = tokio::sync::broadcast::channel(16);
        cs.set_l4_receiver(rx);
        tx.send(event).ok();
        cs.tick();

        assert!(cs.is_active());

        std::thread::sleep(Duration::from_millis(5));
        cs.tick();

        assert!(!cs.is_active(), "Suggestion should dismiss after TTL");
    }

    // ── Display format ──────────────────────────────────────────

    #[test]
    fn suggestion_display_includes_agent_and_title() {
        let event = L4Event {
            agent_id: "Alice".into(),
            memory_id: "mem-5".into(),
            operation: L4Operation::Insert,
            title: "auth refactor".into(),
            timestamp_ms: 5000,
        };
        let suggestion = Suggestion::from_insert_event(&event);
        let text = suggestion.display_text();
        assert!(text.contains("Alice"));
        assert!(text.contains("auth refactor"));
        assert!(text.contains("Review? [Enter]"));
    }

    // ── Render stability ────────────────────────────────────────

    #[test]
    fn inactive_renders_nothing() {
        let mut terminal = MockTerminal::new(80, 24);
        let theme = SkinConfig::default();
        let cs = ContextSuggestions::new();

        terminal.draw(|f: &mut Frame| {
            let area = f.area();
            let mut ctx = RenderContext::new(f, &theme);
            cs.render(&mut ctx, area);
        });

        // Should not panic — just a no-op
        let lines = terminal.buffer_lines();
        assert!(lines.iter().all(|l| l.is_empty()));
    }
}
