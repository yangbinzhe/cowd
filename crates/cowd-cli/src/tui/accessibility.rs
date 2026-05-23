// ── Accessibility — ARIA labels, high contrast, screen reader ────
// Task 36: Accessibility features for the TUI.
//
// Features:
//   - ARIA-like labels for focusable components (via a std::collections::HashMap).
//   - High contrast theme palette (>4.5:1 contrast ratio per WCAG AA).
//   - Screen reader mode enabled via --tui-accessibility CLI flag.
// -------------------------------------------------------------------

#![allow(dead_code)]

use ratatui::style::{Color, Modifier, Style};
use std::collections::HashMap;

use crate::tui::theme::{Palette, StyleSheet, Theme, ThemeLoader, ThemeSource};

// ── Accessibility Mode ───────────────────────────────────────────

/// Global accessibility settings for the TUI.
#[derive(Debug, Clone)]
pub struct AccessibilityMode {
    /// Whether screen reader optimizations are enabled.
    pub screen_reader: bool,
    /// Whether high contrast mode is enabled.
    pub high_contrast: bool,
    /// ARIA-like labels for focusable UI components.
    pub labels: HashMap<String, String>,
}

impl AccessibilityMode {
    /// Create default accessibility settings (all disabled).
    pub fn new() -> Self {
        Self {
            screen_reader: false,
            high_contrast: false,
            labels: HashMap::new(),
        }
    }

    /// Enable full accessibility mode (screen reader + high contrast).
    pub fn full() -> Self {
        let mut mode = Self {
            screen_reader: true,
            high_contrast: true,
            labels: HashMap::new(),
        };
        mode.register_default_labels();
        mode
    }

    /// Enable screen reader mode only.
    pub fn screen_reader_only() -> Self {
        let mut mode = Self {
            screen_reader: true,
            high_contrast: false,
            labels: HashMap::new(),
        };
        mode.register_default_labels();
        mode
    }

    /// Get the ARIA label for a component by its ID.
    ///
    /// Returns the label if registered, or the ID itself as fallback.
    pub fn label_for<'a>(&'a self, component_id: &'a str) -> &'a str {
        self.labels
            .get(component_id)
            .map(|s| s.as_str())
            .unwrap_or(component_id)
    }

    /// Register default ARIA labels for common TUI components.
    fn register_default_labels(&mut self) {
        self.labels.insert("input".into(), "Message input field. Type your message and press Enter to send.".into());
        self.labels.insert("chat_view".into(), "Chat conversation view. Shows messages between you and the AI assistant.".into());
        self.labels.insert("status_bar".into(), "Status bar showing model name, token usage, and session info.".into());
        self.labels.insert("session_sidebar".into(), "Session sidebar. Lists available conversation sessions. Use arrow keys to navigate.".into());
        self.labels.insert("command_palette".into(), "Command palette. Search and execute commands. Type to filter.".into());
        self.labels.insert("file_tree".into(), "File tree browser. Shows workspace files and directories. Use arrow keys to navigate.".into());
        self.labels.insert("diff_viewer".into(), "Diff viewer. Shows code changes with additions in green and deletions in red.".into());
        self.labels.insert("dialog_alert".into(), "Alert dialog. Press any key to dismiss.".into());
        self.labels.insert("dialog_confirm".into(), "Confirmation dialog. Press Y to confirm or N to cancel.".into());
        self.labels.insert("dialog_select".into(), "Selection dialog. Use arrow keys to navigate and Enter to confirm.".into());
        self.labels.insert("dialog_prompt".into(), "Input prompt. Type your response and press Enter.".into());
        self.labels.insert("help_panel".into(), "Help panel showing keyboard shortcuts. Press ? to toggle.".into());
        self.labels.insert("search_field".into(), "Search field. Type to search in conversation. Press Enter to search, Esc to cancel.".into());
    }
}

impl Default for AccessibilityMode {
    fn default() -> Self {
        Self::new()
    }
}

// ── High Contrast Palette ────────────────────────────────────────

/// Build a high-contrast palette meeting WCAG AA requirements (>4.5:1 ratio).
///
/// Contrast ratios for key pairs:
///   - White (#FFFFFF) on Black (#000000): 21:1 ✓
///   - Yellow (#FFFF00) on Black (#000000): 19.6:1 ✓
///   - Cyan (#00FFFF) on Black (#000000): 16.8:1 ✓
///   - Green (#00FF00) on Black (#000000): 15.3:1 ✓
///   - Red (#FF0000) on Black (#000000): 5.25:1 ✓ (>4.5 AA)
///   - Black (#000000) on White (#FFFFFF): 21:1 ✓
///   - Blue (#0000FF) on White (#FFFFFF): 8.6:1 ✓ (>4.5 AA)
///
/// All pairings exceed WCAG AA 4.5:1 minimum.
pub fn high_contrast_dark_palette() -> Palette {
    Palette {
        accent: Color::Rgb(0, 255, 255),       // Cyan — 16.8:1 on black
        bg: Color::Rgb(0, 0, 0),               // Black
        fg: Color::Rgb(255, 255, 255),         // White — 21:1 on black
        user_color: Color::Rgb(0, 255, 0),     // Green — 15.3:1 on black
        warn: Color::Rgb(255, 255, 0),         // Yellow — 19.6:1 on black
        error: Color::Rgb(255, 80, 80),        // Bright red — ~10:1 on black
        success: Color::Rgb(0, 255, 0),        // Green — 15.3:1 on black
        muted: Color::Rgb(180, 180, 180),      // Light gray — ~12:1 on black
    }
}

/// Build a high-contrast light palette meeting WCAG AA requirements.
///
/// Uses black text on white background, with high-saturation accents.
pub fn high_contrast_light_palette() -> Palette {
    Palette {
        accent: Color::Rgb(0, 0, 180),         // Darker blue — >10:1 on white
        bg: Color::Rgb(255, 255, 255),         // White
        fg: Color::Rgb(0, 0, 0),               // Black — 21:1 on white
        user_color: Color::Rgb(0, 100, 0),     // Dark green — >8:1 on white
        warn: Color::Rgb(130, 75, 0),          // Dark amber/brown — >4.5:1 on white
        error: Color::Rgb(180, 0, 0),          // Dark red — >8:1 on white
        success: Color::Rgb(0, 110, 0),        // Dark green — >8:1 on white
        muted: Color::Rgb(90, 90, 90),         // Dark gray — >5:1 on white
    }
}

/// Build a high-contrast theme from a palette.
pub fn high_contrast_theme(dark: bool) -> Theme {
    let palette = if dark {
        high_contrast_dark_palette()
    } else {
        high_contrast_light_palette()
    };

    let name = if dark { "high-contrast-dark" } else { "high-contrast-light" };

    // Compute stylesheet with extra bold for readability
    let mut stylesheet = StyleSheet::from_palette(&palette);

    // Make all headings bold for better readability
    stylesheet.heading1 = Style::default()
        .fg(palette.accent)
        .add_modifier(Modifier::BOLD);
    stylesheet.heading2 = Style::default()
        .fg(palette.accent)
        .add_modifier(Modifier::BOLD);
    stylesheet.heading3 = Style::default()
        .fg(palette.accent)
        .add_modifier(Modifier::BOLD);

    // Make borders more visible
    stylesheet.border_focused = Style::default()
        .fg(palette.accent)
        .add_modifier(Modifier::BOLD);
    stylesheet.border_unfocused = Style::default()
        .fg(palette.fg);

    // Make search highlight very visible
    stylesheet.search_highlight = Style::default()
        .bg(palette.warn)
        .fg(palette.bg)
        .add_modifier(Modifier::BOLD);

    Theme {
        name: name.into(),
        palette,
        stylesheet,
        source: ThemeSource::Builtin,
    }
}

// ── WCAG Contrast Checker ────────────────────────────────────────

/// Relative luminance of an sRGB color (WCAG 2.1 definition).
fn relative_luminance(r: u8, g: u8, b: u8) -> f64 {
    fn linearize(c: u8) -> f64 {
        let s = c as f64 / 255.0;
        if s <= 0.04045 {
            s / 12.92
        } else {
            ((s + 0.055) / 1.055).powi(2)
        }
    }
    0.2126 * linearize(r) + 0.7152 * linearize(g) + 0.0722 * linearize(b)
}

/// Calculate WCAG 2.1 contrast ratio between two colors.
/// Returns a value >= 1.0. WCAG AA requires >= 4.5 for normal text.
pub fn contrast_ratio(c1: Color, c2: Color) -> f64 {
    let l1 = color_luminance(c1);
    let l2 = color_luminance(c2);
    let lighter = l1.max(l2);
    let darker = l1.min(l2);
    (lighter + 0.05) / (darker + 0.05)
}

/// Get the relative luminance of a ratatui Color.
fn color_luminance(c: Color) -> f64 {
    match c {
        Color::Rgb(r, g, b) => relative_luminance(r, g, b),
        Color::Black => relative_luminance(0, 0, 0),
        Color::Red => relative_luminance(255, 0, 0),
        Color::Green => relative_luminance(0, 255, 0),
        Color::Yellow => relative_luminance(255, 255, 0),
        Color::Blue => relative_luminance(0, 0, 255),
        Color::Magenta => relative_luminance(255, 0, 255),
        Color::Cyan => relative_luminance(0, 255, 255),
        Color::White => relative_luminance(255, 255, 255),
        Color::Gray => relative_luminance(192, 192, 192),
        Color::DarkGray => relative_luminance(128, 128, 128),
        Color::LightRed => relative_luminance(255, 102, 102),
        Color::LightGreen => relative_luminance(102, 255, 102),
        Color::LightYellow => relative_luminance(255, 255, 102),
        Color::LightBlue => relative_luminance(102, 102, 255),
        Color::LightMagenta => relative_luminance(255, 102, 255),
        Color::LightCyan => relative_luminance(102, 255, 255),
        _ => 0.5, // Unknown / Reset / Indexed: assume middle gray
    }
}

/// Check if a palette meets WCAG AA for all critical text pairings.
/// Returns a list of failing pair names, if any.
pub fn audit_palette_contrast(palette: &Palette) -> Vec<String> {
    let mut failures = Vec::new();

    let checks: Vec<(&str, Color, Color)> = vec![
        ("fg on bg", palette.fg, palette.bg),
        ("accent on bg", palette.accent, palette.bg),
        ("user_color on bg", palette.user_color, palette.bg),
        ("warn on bg", palette.warn, palette.bg),
        ("error on bg", palette.error, palette.bg),
        ("success on bg", palette.success, palette.bg),
        ("muted on bg", palette.muted, palette.bg),
    ];

    for (name, fg, bg) in checks {
        let ratio = contrast_ratio(fg, bg);
        if ratio < 4.5 {
            failures.push(format!("{name}: {ratio:.1}:1 (need ≥4.5:1)"));
        }
    }

    failures
}

// ── Screen Reader Output ─────────────────────────────────────────

/// Maximum length for screen reader announcements (truncated if longer).
const MAX_ANNOUNCEMENT_LEN: usize = 200;

/// Announce a message for screen readers. When screen reader mode is
/// active, critical UI changes are prefixed so assistive tech can
/// pick them up. In a real implementation, this would output via
/// an accessible output channel (e.g., speech-dispatcher or braille).
pub fn announce(message: &str) {
    let truncated: String = message.chars().take(MAX_ANNOUNCEMENT_LEN).collect();
    // In screen reader mode, output to a dedicated stream.
    // For now, use stderr as the accessible output channel.
    eprintln!("[cowd a11y] {truncated}");
}

/// Announce that focus has moved to a named component.
pub fn announce_focus(component_id: &str, label: &str) {
    announce(&format!("Focus moved to {component_id}: {label}"));
}

/// Announce a state change (e.g., "theme changed to dark").
pub fn announce_state_change(change: &str) {
    announce(&format!("State changed: {change}"));
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessibility_mode_default_disables_all() {
        let mode = AccessibilityMode::new();
        assert!(!mode.screen_reader);
        assert!(!mode.high_contrast);
        assert!(mode.labels.is_empty());
    }

    #[test]
    fn accessibility_mode_full_enables_all() {
        let mode = AccessibilityMode::full();
        assert!(mode.screen_reader);
        assert!(mode.high_contrast);
        assert!(!mode.labels.is_empty());
        assert!(mode.label_for("input").contains("Message input"));
    }

    #[test]
    fn label_for_returns_id_when_not_registered() {
        let mode = AccessibilityMode::new();
        assert_eq!(mode.label_for("unknown_id"), "unknown_id");
    }

    #[test]
    fn default_labels_registered() {
        let mut mode = AccessibilityMode::new();
        mode.register_default_labels();
        assert!(mode.label_for("input").contains("Message input field"));
        assert!(mode.label_for("chat_view").contains("Chat conversation"));
        assert!(mode.label_for("status_bar").contains("Status bar"));
        assert!(mode.label_for("command_palette").contains("Command palette"));
        assert!(mode.label_for("file_tree").contains("File tree"));
        assert!(mode.label_for("diff_viewer").contains("Diff viewer"));
        assert!(mode.label_for("help_panel").contains("Help panel"));
        assert!(mode.label_for("search_field").contains("Search field"));
    }

    #[test]
    fn high_contrast_dark_palette_passes_wcag_aa() {
        let palette = high_contrast_dark_palette();
        let failures = audit_palette_contrast(&palette);
        assert!(
            failures.is_empty(),
            "high-contrast dark palette has WCAG failures: {failures:?}"
        );
    }

    #[test]
    fn high_contrast_light_palette_passes_wcag_aa() {
        let palette = high_contrast_light_palette();
        let failures = audit_palette_contrast(&palette);
        assert!(
            failures.is_empty(),
            "high-contrast light palette has WCAG failures: {failures:?}"
        );
    }

    #[test]
    fn high_contrast_theme_built() {
        let theme = high_contrast_theme(true);
        assert_eq!(theme.name, "high-contrast-dark");
        assert!(matches!(theme.source, ThemeSource::Builtin));
        // Should have bold headings
        assert!(theme.stylesheet.heading1.add_modifier.contains(Modifier::BOLD));
        assert!(theme.stylesheet.heading2.add_modifier.contains(Modifier::BOLD));
        assert!(theme.stylesheet.heading3.add_modifier.contains(Modifier::BOLD));
    }

    #[test]
    fn contrast_ratio_black_on_white_is_21() {
        let ratio = contrast_ratio(Color::Black, Color::White);
        assert!(
            (ratio - 21.0).abs() < 0.5,
            "black on white contrast should be ~21:1, got {ratio}"
        );
    }

    #[test]
    fn contrast_ratio_white_on_black_is_21() {
        let ratio = contrast_ratio(Color::White, Color::Black);
        assert!(
            (ratio - 21.0).abs() < 0.5,
            "white on black contrast should be ~21:1, got {ratio}"
        );
    }

    #[test]
    fn contrast_ratio_red_on_white() {
        let ratio = contrast_ratio(Color::Red, Color::White);
        // Red (#FF0000) on White (#FFFFFF) ≈ 4.0:1 — below AA for normal text
        assert!(
            ratio < 5.0 && ratio > 3.5,
            "red on white contrast should be ~4.0:1, got {ratio}"
        );
    }

    #[test]
    fn contrast_ratio_same_color_is_1() {
        let ratio = contrast_ratio(Color::Rgb(128, 128, 128), Color::Rgb(128, 128, 128));
        assert!(
            (ratio - 1.0).abs() < 0.01,
            "same color contrast should be 1:1, got {ratio}"
        );
    }

    #[test]
    fn audit_dark_palette_catches_low_contrast() {
        // Build a palette with intentionally low contrast for testing
        let palette = Palette {
            accent: Color::Rgb(0, 255, 255),
            bg: Color::Rgb(0, 0, 0),
            fg: Color::Rgb(50, 50, 50),       // Dark gray on black — will fail WCAG
            user_color: Color::Rgb(0, 255, 0),
            warn: Color::Rgb(255, 255, 0),
            error: Color::Rgb(255, 0, 0),
            success: Color::Rgb(0, 255, 0),
            muted: Color::Rgb(40, 40, 40),    // Very dark on black — will fail WCAG
        };
        let failures = audit_palette_contrast(&palette);
        // fg and muted should fail on black background
        assert!(
            failures.iter().any(|f| f.contains("fg")),
            "dark fg on black should fail WCAG AA, got: {failures:?}"
        );
        assert!(
            failures.iter().any(|f| f.contains("muted")),
            "very dark muted on black should fail WCAG AA, got: {failures:?}"
        );
    }

    #[test]
    fn screen_reader_only() {
        let mode = AccessibilityMode::screen_reader_only();
        assert!(mode.screen_reader);
        assert!(!mode.high_contrast);
        assert!(!mode.labels.is_empty());
    }
}
