// ── ThemeEngine — Hot-reloadable theme runtime ──────────────────
// Wraps a Theme with poll-based file watching, style computation,
// and dark/light toggling.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use ratatui::style::{Color, Style};

use crate::tui::theme::{Palette, StyleSheet, Theme, ThemeLoader};

/// A theme engine with hot-reload and semantic style lookup.
///
/// Callers poll `hot_reload()` to pick up YAML file changes; no background
/// thread is spawned.  Use `compute_style(context)` to get a ratatui `Style`
/// for a named UI element (e.g. `"heading1"`, `"code_block"`).
///
/// # Example
/// ```ignore
/// let mut engine = ThemeEngine::new_dark();
/// let s = engine.compute_style("heading1");
/// let changed = engine.hot_reload(); // poll for file changes
/// ```
pub struct ThemeEngine {
    /// The current active theme.
    pub theme: Theme,
    /// Optional path to the YAML file being watched for hot-reload.
    file_path: Option<PathBuf>,
    /// Last known modification time of the watched file.
    last_modified: Option<SystemTime>,
}

impl ThemeEngine {
    // ── Constructors ────────────────────────────────────────────

    /// Wrap an already-constructed `Theme`.
    pub fn new(theme: Theme) -> Self {
        Self {
            theme,
            file_path: None,
            last_modified: None,
        }
    }

    /// Create an engine pre-loaded with the builtin dark theme.
    pub fn new_dark() -> Self {
        Self::new(ThemeLoader::builtin_dark())
    }

    /// Create an engine pre-loaded with the builtin light theme.
    pub fn new_light() -> Self {
        Self::new(ThemeLoader::builtin_light())
    }

    /// Load a theme from a YAML file and configure hot-reload on that file.
    ///
    /// The returned engine will poll `path` whenever `hot_reload()` is called.
    pub fn load(path: &Path) -> Result<Self, String> {
        let theme = ThemeLoader::load(path)?;
        let mtime = Self::mtime_of(path);
        Ok(Self {
            theme,
            file_path: Some(path.to_path_buf()),
            last_modified: mtime,
        })
    }

    // ── Hot-reload ──────────────────────────────────────────────

    /// Poll-based hot-reload.  If the YAML file has changed on disk
    /// the theme is reloaded and `true` is returned.
    ///
    /// When the file is missing or the reload fails the old theme is
    /// preserved and `false` is returned (the mtime is still updated
    /// to avoid re-trying a broken file on every frame).
    pub fn hot_reload(&mut self) -> bool {
        let path = match &self.file_path {
            Some(p) => p,
            None => return false,
        };

        let current_mtime = match Self::mtime_of(path) {
            Some(t) => t,
            None => return false,
        };

        // Nothing changed → skip
        if self.last_modified == Some(current_mtime) {
            return false;
        }

        // Try to reload; keep old theme on failure but still advance mtime
        // to avoid busy-looping on a corrupt file.
        match ThemeLoader::load(path) {
            Ok(theme) => {
                self.theme = theme;
                self.last_modified = Some(current_mtime);
                true
            }
            Err(_) => {
                self.last_modified = Some(current_mtime);
                false
            }
        }
    }

    // ── Style computation ───────────────────────────────────────

    /// Map a semantic context string to a ratatui `Style`.
    ///
    /// Supported contexts (16 tokens):
    /// `"heading1"`–`"heading6"`, `"code_block"`, `"inline_code"`,
    /// `"tool_status_running"`, `"tool_status_done"`, `"tool_status_error"`,
    /// `"diff_add"`, `"diff_del"`, `"search_highlight"`, `"border_focused"`,
    /// `"border_unfocused"`.
    ///
    /// Unknown contexts return `Style::default().fg(palette.fg)`.
    pub fn compute_style(&self, context: &str) -> Style {
        let ss = &self.theme.stylesheet;
        match context {
            "heading1" => ss.heading1,
            "heading2" => ss.heading2,
            "heading3" => ss.heading3,
            "heading4" => ss.heading4,
            "heading5" => ss.heading5,
            "heading6" => ss.heading6,
            "code_block" => ss.code_block,
            "inline_code" => ss.inline_code,
            "tool_status_running" => ss.tool_status_running,
            "tool_status_done" => ss.tool_status_done,
            "tool_status_error" => ss.tool_status_error,
            "diff_add" => ss.diff_add,
            "diff_del" => ss.diff_del,
            "search_highlight" => ss.search_highlight,
            "border_focused" => ss.border_focused,
            "border_unfocused" => ss.border_unfocused,
            _ => Style::default().fg(self.theme.palette.fg),
        }
    }

    // ── Toggle dark/light ───────────────────────────────────────

    /// Replace the current theme with the opposite builtin preset.
    ///
    /// After toggling the engine is no longer watching any file
    /// (`hot_reload()` will return `false`) because the builtin
    /// themes are embedded, not file-based.
    pub fn toggle_dark_light(&mut self) {
        let is_dark = self.theme.palette.bg == Color::Black;
        self.theme = if is_dark {
            ThemeLoader::builtin_light()
        } else {
            ThemeLoader::builtin_dark()
        };
        // Builtin themes aren't file-backed — reset reload state.
        self.file_path = None;
        self.last_modified = None;
    }

    // ── Read-only accessors ─────────────────────────────────────

    /// Read-only reference to the current palette.
    pub fn palette(&self) -> &Palette {
        &self.theme.palette
    }

    /// Read-only reference to the current stylesheet.
    pub fn stylesheet(&self) -> &StyleSheet {
        &self.theme.stylesheet
    }

    // ── Helpers ─────────────────────────────────────────────────

    fn mtime_of(path: &Path) -> Option<SystemTime> {
        std::fs::metadata(path).ok()?.modified().ok()
    }
}

// ── Tests ───────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::{Color, Modifier};
    use std::io::Write;
    use std::time::Duration;

    // ── Construction ────────────────────────────────────────────

    #[test]
    fn new_dark_creates_dark_theme() {
        let engine = ThemeEngine::new_dark();
        assert_eq!(engine.theme.name, "dark");
        assert_eq!(engine.theme.palette.accent, Color::Cyan);
        assert_eq!(engine.theme.palette.bg, Color::Black);
        assert!(engine.file_path.is_none());
    }

    #[test]
    fn new_light_creates_light_theme() {
        let engine = ThemeEngine::new_light();
        assert_eq!(engine.theme.name, "light");
        assert_eq!(engine.theme.palette.accent, Color::Blue);
        assert_eq!(engine.theme.palette.bg, Color::White);
        assert!(engine.file_path.is_none());
    }

    // ── compute_style ───────────────────────────────────────────

    #[test]
    fn compute_style_heading1() {
        let engine = ThemeEngine::new_dark();
        let style = engine.compute_style("heading1");
        assert_eq!(style.fg, Some(Color::Cyan));
        assert!(
            style.add_modifier.contains(Modifier::BOLD),
            "heading1 should be bold"
        );
    }

    #[test]
    fn compute_style_code_block() {
        let engine = ThemeEngine::new_dark();
        let style = engine.compute_style("code_block");
        // code_block fg is palette.fg = White in dark mode
        assert_eq!(style.fg, Some(Color::White));
    }

    #[test]
    fn compute_style_unknown_context_defaults_to_fg() {
        let engine = ThemeEngine::new_dark();
        let style = engine.compute_style("nonexistent_token");
        assert_eq!(style.fg, Some(Color::White));
        assert_eq!(
            style.add_modifier,
            Modifier::empty(),
            "default style should have no modifiers"
        );
    }

    #[test]
    fn compute_style_all_known_contexts_return_some_style() {
        let engine = ThemeEngine::new_dark();
        let contexts = [
            "heading1", "heading2", "heading3", "heading4", "heading5", "heading6",
            "code_block", "inline_code",
            "tool_status_running", "tool_status_done", "tool_status_error",
            "diff_add", "diff_del",
            "search_highlight", "border_focused", "border_unfocused",
        ];
        for ctx in &contexts {
            let style = engine.compute_style(ctx);
            // Every known context should have a defined fg
            assert!(
                style.fg.is_some(),
                "context '{ctx}' should have a foreground color"
            );
        }
    }

    // ── toggle_dark_light ───────────────────────────────────────

    #[test]
    fn toggle_dark_light() {
        let mut engine = ThemeEngine::new_dark();
        assert_eq!(engine.theme.palette.bg, Color::Black);

        // dark → light
        engine.toggle_dark_light();
        assert_eq!(engine.theme.name, "light");
        assert_eq!(engine.theme.palette.bg, Color::White);
        // reload state should be cleared
        assert!(engine.file_path.is_none());
        assert!(engine.last_modified.is_none());

        // light → dark
        engine.toggle_dark_light();
        assert_eq!(engine.theme.name, "dark");
        assert_eq!(engine.theme.palette.bg, Color::Black);
    }

    #[test]
    fn toggle_dark_light_clears_reload_state() {
        let mut engine = ThemeEngine::new_dark();
        // After a toggle, hot_reload should return false (no file)
        engine.toggle_dark_light();
        assert!(!engine.hot_reload(), "no file to watch after toggle");
    }

    // ── load_from_yaml ──────────────────────────────────────────

    #[test]
    fn load_from_yaml() {
        let tmp = std::env::temp_dir()
            .join(format!("cowd-engine-load-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let path = tmp.join("theme.yaml");

        let content = r###"
name: "test-theme"
colors:
  accent: "#FF8800"
  bg: "#111111"
  fg: "#EEEEEE"
  user_color: "#3366FF"
  warn: "#FFAA00"
  error: "#DD0000"
  success: "#00DD00"
  muted: "#808080"
"###;
        std::fs::write(&path, content).expect("write theme.yaml");

        let engine = ThemeEngine::load(&path).expect("load theme from yaml");
        assert_eq!(engine.theme.name, "test-theme");
        assert_eq!(engine.theme.palette.accent, Color::Rgb(0xFF, 0x88, 0x00));
        assert_eq!(engine.theme.palette.bg, Color::Rgb(0x11, 0x11, 0x11));
        assert_eq!(engine.theme.palette.fg, Color::Rgb(0xEE, 0xEE, 0xEE));
        // Should have file_path set for hot-reload
        assert!(engine.file_path.is_some());
        assert!(engine.last_modified.is_some());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ── hot_reload ──────────────────────────────────────────────

    #[test]
    fn hot_reload_detects_change() {
        let tmp = std::env::temp_dir()
            .join(format!("cowd-engine-hot-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let path = tmp.join("theme.yaml");

        // Write initial theme
        let initial = r###"
name: "initial"
colors:
  accent: "#FF0000"
  bg: "#000000"
  fg: "#FFFFFF"
  user_color: "#00FF00"
  warn: "#FFFF00"
  error: "#FF0000"
  success: "#00FF00"
  muted: "#808080"
"###;
        {
            let mut f = std::fs::File::create(&path).expect("create");
            f.write_all(initial.as_bytes()).expect("write initial");
            f.flush().expect("flush");
        }

        let mut engine = ThemeEngine::load(&path).expect("load initial theme");
        assert_eq!(engine.theme.name, "initial");

        // Ensure mtime advances (some filesystems have 1-second granularity)
        std::thread::sleep(Duration::from_millis(50));

        // Write updated theme
        let updated = r###"
name: "updated"
colors:
  accent: "#00FF00"
  bg: "#000000"
  fg: "#FFFFFF"
  user_color: "#00FF00"
  warn: "#FFFF00"
  error: "#FF0000"
  success: "#00FF00"
  muted: "#808080"
"###;
        {
            let mut f = std::fs::File::create(&path).expect("create");
            f.write_all(updated.as_bytes()).expect("write updated");
            f.flush().expect("flush");
        }

        // hot_reload should detect the change
        assert!(
            engine.hot_reload(),
            "should detect file modification"
        );
        assert_eq!(engine.theme.name, "updated");

        // Second call with no change should return false
        assert!(
            !engine.hot_reload(),
            "should not reload on unchanged file"
        );

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn hot_reload_no_file_returns_false() {
        let mut engine = ThemeEngine::new_dark();
        assert!(!engine.hot_reload(), "no file path configured");
    }

    #[test]
    fn hot_reload_preserves_old_theme_on_bad_yaml() {
        let tmp = std::env::temp_dir()
            .join(format!("cowd-engine-bad-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let path = tmp.join("theme.yaml");

        let content = r###"
name: "good"
colors:
  accent: "#00FFFF"
  bg: "#000000"
  fg: "#FFFFFF"
  user_color: "#00FF00"
  warn: "#FFFF00"
  error: "#FF0000"
  success: "#00FF00"
  muted: "#808080"
"###;
        std::fs::write(&path, content).expect("write good theme");
        let mut engine = ThemeEngine::load(&path).expect("load good theme");
        assert_eq!(engine.theme.name, "good");

        std::thread::sleep(Duration::from_millis(50));

        // Write invalid YAML
        std::fs::write(&path, "::: not yaml :::").expect("write bad yaml");

        // hot_reload should return false but NOT panic
        assert!(!engine.hot_reload(), "bad yaml should not trigger reload");
        // Old theme should be preserved
        assert_eq!(engine.theme.name, "good");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ── Accessors ───────────────────────────────────────────────

    #[test]
    fn palette_accessor() {
        let engine = ThemeEngine::new_dark();
        assert_eq!(engine.palette().accent, Color::Cyan);
    }

    #[test]
    fn stylesheet_accessor() {
        let engine = ThemeEngine::new_dark();
        assert_eq!(engine.stylesheet().heading1.fg, Some(Color::Cyan));
    }
}
