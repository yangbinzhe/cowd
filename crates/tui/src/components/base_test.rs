// ── TDD Tests for Component Trait System ──────────────────────────
// Written BEFORE implementation (red phase). Tests verify:
// - Component trait dispatch (render, handle_event, id, focusable)
// - EventResult variants (Consumed, NotConsumed, Propagate)
// - RenderContext theme access, area shortcut, text measurement
// - ComponentId newtype (Display, Copy, Hash)
// -----------------------------------------------------------------

#![cfg(test)]

use ratatui::backend::TestBackend;
use ratatui::layout::Rect;
use ratatui::Terminal;

use crate::components::base::*;
use crate::skin::SkinConfig;

// ── Helper: a minimal test component ─────────────────────────────

struct TestComponent {
    id: &'static str,
    focusable: bool,
    render_called: bool,
    last_area: Option<Rect>,
}

impl TestComponent {
    fn new(id: &'static str) -> Self {
        Self {
            id,
            focusable: true,
            render_called: false,
            last_area: None,
        }
    }
}

impl Component for TestComponent {
    fn render(&mut self, _ctx: &mut RenderContext, area: Rect) {
        self.render_called = true;
        self.last_area = Some(area);
    }

    fn handle_event(&mut self, _event: &crossterm::event::Event) -> EventResult {
        EventResult::Consumed
    }

    fn focusable(&self) -> bool {
        self.focusable
    }

    fn id(&self) -> &str {
        self.id
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[test]
fn component_trait_render_called() {
    let mut comp = TestComponent::new("test_render");
    let area = Rect::new(0, 0, 80, 24);

    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    let theme = SkinConfig::default();

    terminal
        .draw(|frame: &mut ratatui::Frame<'_>| {
            let mut ctx = RenderContext::new(frame, &theme);
            comp.render(&mut ctx, area);
        })
        .unwrap();

    assert!(
        comp.render_called,
        "render() should set render_called to true"
    );
    assert_eq!(
        comp.last_area,
        Some(area),
        "render() should receive the correct area"
    );
}

#[test]
fn component_trait_id_and_focusable() {
    let comp = TestComponent::new("test_id");

    assert_eq!(comp.id(), "test_id");
    assert!(comp.focusable(), "should be focusable by default");

    let mut comp2 = TestComponent::new("non_focusable");
    comp2.focusable = false;
    assert!(!comp2.focusable(), "should be non-focusable");
}

#[test]
fn event_result_consumed() {
    let result = EventResult::Consumed;
    assert!(result.is_consumed());
    assert!(!result.is_not_consumed());
}

#[test]
fn event_result_not_consumed() {
    let result = EventResult::NotConsumed;
    assert!(!result.is_consumed());
    assert!(result.is_not_consumed());
}

#[test]
fn event_result_propagate_roundtrip() {
    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};

    let original = Event::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    let result = EventResult::Propagate(original.clone());

    // Debug output should contain variant name
    let debug_str = format!("{result:?}");
    assert!(
        debug_str.contains("Propagate"),
        "Debug should contain Propagate, got: {debug_str}"
    );

    // Pattern matching roundtrip
    match result {
        EventResult::Propagate(event) => {
            assert_eq!(
                event, original,
                "Propagate should preserve the wrapped event"
            );
        }
        _ => panic!("Expected Propagate variant"),
    }
}

#[test]
fn component_handle_event_consumed() {
    let mut comp = TestComponent::new("event_consumer");
    let event = crossterm::event::Event::Key(crossterm::event::KeyEvent::new(
        crossterm::event::KeyCode::Enter,
        crossterm::event::KeyModifiers::NONE,
    ));

    let result = comp.handle_event(&event);
    assert!(
        result.is_consumed(),
        "TestComponent should consume all events"
    );
}

#[test]
fn render_context_theme_access() {
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    let theme = SkinConfig::default();

    terminal
        .draw(|frame: &mut ratatui::Frame<'_>| {
            let mut ctx = RenderContext::new(frame, &theme);
            let retrieved_theme = ctx.theme();

            assert_eq!(
                retrieved_theme.name, "default",
                "RenderContext should provide access to the theme"
            );

            // Verify area() shortcut
            let area = ctx.area();
            assert_eq!(area.width, 80);
            assert_eq!(area.height, 24);

            // Verify frame_mut() returns a usable frame
            let _f = ctx.frame_mut();
        })
        .unwrap();
}

#[test]
fn render_context_measure_text() {
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    let theme = SkinConfig::default();

    terminal
        .draw(|frame: &mut ratatui::Frame<'_>| {
            let ctx = RenderContext::new(frame, &theme);

            let (w, h) = ctx.measure_text("hello");
            assert_eq!(w, 5, "width of 'hello' should be 5");
            assert_eq!(h, 1, "height of single line should be 1");

            let (w, h) = ctx.measure_text("line1\nline2\nline3");
            assert_eq!(w, 5, "max width of three lines should be 5");
            assert_eq!(h, 3, "height of three lines should be 3");

            let (w, h) = ctx.measure_text("");
            assert_eq!(w, 0, "empty string width should be 0");
            assert_eq!(h, 0, "empty string height should be 0");
        })
        .unwrap();
}

#[test]
fn component_id_newtype() {
    let id = ComponentId("main_panel");
    assert_eq!(id.as_str(), "main_panel");
    assert_eq!(format!("{id}"), "main_panel");

    // Copy semantics
    let id2 = id;
    assert_eq!(id, id2);

    // Hash works (enables use in HashMaps)
    use std::collections::HashSet;
    let mut set = HashSet::new();
    set.insert(ComponentId("a"));
    set.insert(ComponentId("a")); // duplicate
    assert_eq!(
        set.len(),
        1,
        "duplicate ComponentIds should be deduplicated"
    );
}

#[test]
fn render_context_new() {
    let backend = TestBackend::new(80, 24);
    let mut terminal = Terminal::new(backend).unwrap();
    let theme = SkinConfig::default();

    terminal
        .draw(|frame: &mut ratatui::Frame<'_>| {
            let ctx = RenderContext::new(frame, &theme);
            assert_eq!(ctx.area(), Rect::new(0, 0, 80, 24));
        })
        .unwrap();
}
