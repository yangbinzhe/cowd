// ── Base Component Trait ──────────────────────────────────────────
// Minimal trait for all TUI components: render(), handle_event(),
// focusable(), id(). No virtual DOM, no lifecycle hooks beyond these
// four methods. ratatui is immediate-mode.
//
// Architecture decision (see decisions.md):
//   - Component trait must be minimal: render() + handle_event()
//     + focusable() + id()
//   - ratatui immediate-mode rendering (no virtual DOM)
// -----------------------------------------------------------------

use ratatui::layout::Rect;
use ratatui::Frame;

use crate::tui::skin::SkinConfig;

// ── ComponentId ──────────────────────────────────────────────────

/// A newtype wrapper around `&'static str` for type-safe component
/// identification. Enables zero-allocation lookups in component registries.
///
/// # Examples
///
/// ```
/// use cowd_cli::tui::components::ComponentId;
///
/// let id = ComponentId("status_bar");
/// assert_eq!(id.as_str(), "status_bar");
/// assert_eq!(format!("{id}"), "status_bar");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ComponentId(pub &'static str);

impl ComponentId {
    /// Return the inner string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        self.0
    }
}

impl std::fmt::Display for ComponentId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// ── EventResult ──────────────────────────────────────────────────

/// The result of a component's event handling.
///
/// # Variants
///
/// * `Consumed` — The event was handled. Processing should stop for this
///   event (no further handlers should see it).
/// * `NotConsumed` — The event was not handled. Other handlers or
///   fallback processing should be attempted.
/// * `Propagate(Event)` — The component transformed the event and wishes
///   to pass it to a parent or outer handler for further processing.
#[derive(Debug, Clone, PartialEq)]
pub enum EventResult {
    /// The event was handled; stop processing.
    Consumed,
    /// The event was not handled; continue to the next handler.
    NotConsumed,
    /// The component transformed the event; pass it onward.
    Propagate(crossterm::event::Event),
}

impl EventResult {
    /// Returns `true` if this result is `Consumed`.
    #[must_use]
    pub fn is_consumed(&self) -> bool {
        matches!(self, Self::Consumed)
    }

    /// Returns `true` if this result is `NotConsumed`.
    #[must_use]
    pub fn is_not_consumed(&self) -> bool {
        matches!(self, Self::NotConsumed)
    }

    /// Returns `true` if this result is `Propagate`.
    #[must_use]
    pub fn is_propagate(&self) -> bool {
        matches!(self, Self::Propagate(_))
    }
}

// ── RenderContext ────────────────────────────────────────────────

/// Rendering context passed to every `Component::render` call.
///
/// Wraps a [`ratatui::Frame`] and provides access to the active
/// theme/skin and utility methods for measuring content dimensions.
///
/// # Lifetimes
///
/// * `'frame` — the duration of the borrow on [`Frame`]; typically the
///   body of the closure passed to [`Terminal::draw`].
/// * `'buf` — the lifetime of the frame's internal buffer (also tied
///   to [`Terminal::draw`]).
///
/// Three lifetimes are needed:
/// - `'frame` — the borrow on [`Frame`] (duration of the draw closure).
/// - `'buf` — the lifetime of the frame's internal buffer.
/// - `'theme` — the lifetime of the skin/theme config reference.
///
/// Using separate lifetimes for frame and theme avoids invariance
/// issues with `&mut Frame<'buf>` in test closures.
pub struct RenderContext<'frame, 'buf, 'theme> {
    frame: &'frame mut Frame<'buf>,
    theme: &'theme SkinConfig,
}

impl<'frame, 'buf, 'theme> RenderContext<'frame, 'buf, 'theme> {
    /// Create a new render context from a frame and theme reference.
    #[must_use]
    pub fn new(
        frame: &'frame mut Frame<'buf>,
        theme: &'theme SkinConfig,
    ) -> Self {
        Self { frame, theme }
    }

    /// Access the underlying [`ratatui::Frame`] for direct drawing operations.
    #[must_use]
    pub fn frame_mut(&mut self) -> &mut Frame<'buf> {
        self.frame
    }

    /// Access the current theme/skin configuration.
    #[must_use]
    pub fn theme(&self) -> &SkinConfig {
        self.theme
    }

    /// Shortcut for `self.frame.area()` — the full screen area.
    #[must_use]
    pub fn area(&self) -> Rect {
        self.frame.area()
    }

    /// Measure the display width and line count of a text string.
    ///
    /// Returns `(width, height)` where `width` is the maximum Unicode
    /// display width in columns and `height` is the number of lines.
    #[must_use]
    pub fn measure_text(&self, text: &str) -> (u16, u16) {
        let lines: Vec<&str> = text.lines().collect();
        let width = lines.iter().map(|l| l.len() as u16).max().unwrap_or(0);
        let height = lines.len() as u16;
        (width, height)
    }
}

// ── Component Trait ──────────────────────────────────────────────

/// The base trait for all TUI components.
///
/// Every component on the screen — panels, dialogs, status bars,
/// overlays, modals — must implement this trait. The interface is
/// deliberately minimal (four methods) to keep components simple
/// and composable.
///
/// # Lifecycle
///
/// Components are **immediate-mode**. There is no mount/unmount,
/// no virtual DOM diffing, no reconciliation. The render loop calls
/// `render()` on every frame and `handle_event()` on every keyboard
/// or mouse event.
///
/// # Implementation Notes
///
/// * `render()` receives a [`RenderContext`] for access to the frame
///   and theme, plus a [`Rect`] bounding the component's area.
/// * `handle_event()` receives a reference to a
///   [`crossterm::event::Event`] and returns an [`EventResult`]
///   indicating whether the event was consumed.
/// * `focusable()` controls keyboard focus targeting.
/// * `id()` returns `&str` (not `String`) for zero-allocation lookups
///   in component registries and focus chains.
pub trait Component {
    /// Render the component into the given screen area.
    ///
    /// Called every frame by the TUI event loop. The `area` parameter
    /// specifies the screen region this component should draw into.
    fn render(&mut self, ctx: &mut RenderContext, area: Rect);

    /// Handle an input event.
    ///
    /// Return [`EventResult::Consumed`] if the event was handled,
    /// [`EventResult::NotConsumed`] to pass it to the next handler,
    /// or [`EventResult::Propagate`] to transform and propagate it.
    fn handle_event(&mut self, event: &crossterm::event::Event) -> EventResult;

    /// Whether this component can receive keyboard focus.
    fn focusable(&self) -> bool;

    /// Unique identifier for this component instance.
    ///
    /// Returns `&str` for zero-allocation lookups in registries and
    /// focus chains. Should be a compile-time constant where possible.
    fn id(&self) -> &str;
}
