// ── Theme Engine Types ──────────────────────────────────────────
// Theme, StyleSheet, ThemeLoader for YAML-based theming.
// Builtin dark/light presets + migration from legacy SkinConfig.
//
// Usage:
//   let theme = ThemeLoader::builtin_dark();
//   let loaded = ThemeLoader::load(Path::new("theme.yaml"))?;
//   let migrated = ThemeLoader::migrate_from_skin(Path::new("skin.yaml"))?;

#![allow(dead_code)]

mod palette;
pub use palette::{parse_hex, Palette};

use std::path::{Path, PathBuf};
use ratatui::style::{Color, Modifier, Style};

// ── ThemeSource ─────────────────────────────────────────────────

/// Identifies where a Theme was loaded from.
#[derive(Debug, Clone)]
pub enum ThemeSource {
    Builtin,
    YamlFile(PathBuf),
}

// ── StyleSheet ──────────────────────────────────────────────────

/// Semantic style tokens derived from a Palette.
///
/// Each field is a fully-formed ratatui `Style` computed from the
/// palette's semantic colors by `StyleSheet::from_palette()`.
#[derive(Debug, Clone)]
pub struct StyleSheet {
    pub heading1: Style,
    pub heading2: Style,
    pub heading3: Style,
    pub heading4: Style,
    pub heading5: Style,
    pub heading6: Style,
    pub code_block: Style,
    pub inline_code: Style,
    pub tool_status_running: Style,
    pub tool_status_done: Style,
    pub tool_status_error: Style,
    pub diff_add: Style,
    pub diff_del: Style,
    pub search_highlight: Style,
    pub border_focused: Style,
    pub border_unfocused: Style,
}

impl StyleSheet {
    /// Build a complete StyleSheet from a Palette with sensible defaults.
    pub fn from_palette(p: &Palette) -> Self {
        // code_block gets a background that contrasts with the palette bg
        let code_bg = match p.bg {
            Color::Black => Color::Rgb(30, 30, 30),
            Color::White => Color::Rgb(230, 230, 230),
            _ => p.muted,
        };

        Self {
            heading1: Style::default().fg(p.accent).add_modifier(Modifier::BOLD),
            heading2: Style::default().fg(p.accent).add_modifier(Modifier::BOLD),
            heading3: Style::default().fg(p.accent),
            heading4: Style::default().fg(p.accent),
            heading5: Style::default().fg(p.fg),
            heading6: Style::default().fg(p.muted),

            code_block: Style::default().fg(p.fg).bg(code_bg),
            inline_code: Style::default().fg(p.accent).bg(p.muted),

            tool_status_running: Style::default()
                .fg(p.warn)
                .add_modifier(Modifier::BOLD),
            tool_status_done: Style::default().fg(p.success),
            tool_status_error: Style::default()
                .fg(p.error)
                .add_modifier(Modifier::BOLD),

            diff_add: Style::default().fg(p.success),
            diff_del: Style::default().fg(p.error),

            search_highlight: Style::default().bg(p.warn).fg(p.bg),
            border_focused: Style::default().fg(p.accent),
            border_unfocused: Style::default().fg(p.muted),
        }
    }
}

// ── Theme ───────────────────────────────────────────────────────

/// A complete TUI theme combining a palette and derived stylesheet.
#[derive(Debug, Clone)]
pub struct Theme {
    pub name: String,
    pub palette: Palette,
    pub stylesheet: StyleSheet,
    pub source: ThemeSource,
}

// ── ThemeLoader ─────────────────────────────────────────────────

/// Loads themes from YAML files or produces builtin presets.
pub struct ThemeLoader;

impl ThemeLoader {
    /// Load a Theme from a YAML file.
    ///
    /// Expected YAML structure:
    /// ```yaml
    /// name: "my-theme"
    /// colors:
    ///   accent: "#00FFFF"
    ///   bg: "#000000"
    ///   fg: "#FFFFFF"
    ///   user_color: "#00FF00"
    ///   warn: "#FFFF00"
    ///   error: "#FF0000"
    ///   success: "#00FF00"
    ///   muted: "#808080"
    /// ```
    pub fn load(path: &Path) -> Result<Theme, String> {
        let yaml = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
        let data: serde_yaml::Value =
            serde_yaml::from_str(&yaml).map_err(|e| format!("parse: {e}"))?;

        let name = data
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("custom")
            .to_string();

        let palette_value = data
            .get("colors")
            .ok_or_else(|| "missing 'colors' key".to_string())?;

        let palette: Palette = serde_yaml::from_value(palette_value.clone())
            .map_err(|e| format!("palette: {e}"))?;

        let stylesheet = StyleSheet::from_palette(&palette);

        Ok(Theme {
            name,
            palette,
            stylesheet,
            source: ThemeSource::YamlFile(path.to_path_buf()),
        })
    }

    /// Build the builtin dark theme.
    pub fn builtin_dark() -> Theme {
        let palette = Palette::dark();
        let stylesheet = StyleSheet::from_palette(&palette);
        Theme {
            name: "dark".into(),
            palette,
            stylesheet,
            source: ThemeSource::Builtin,
        }
    }

    /// Build the builtin light theme.
    pub fn builtin_light() -> Theme {
        let palette = Palette::light();
        let stylesheet = StyleSheet::from_palette(&palette);
        Theme {
            name: "light".into(),
            palette,
            stylesheet,
            source: ThemeSource::Builtin,
        }
    }

    /// Migrate an old `SkinConfig` (skin.yaml) into a `Theme`.
    ///
    /// Legacy SkinConfig has 7 color fields (no `muted`);
    /// the migrated palette sets `muted` to `Color::DarkGray`.
    pub fn migrate_from_skin(path: &Path) -> Result<Theme, String> {
        let skin = crate::tui::skin::SkinConfig::load(path)?;
        let palette = Palette {
            accent: parse_hex(&skin.colors.accent),
            bg: parse_hex(&skin.colors.bg),
            fg: parse_hex(&skin.colors.fg),
            user_color: parse_hex(&skin.colors.user_color),
            warn: parse_hex(&skin.colors.warn),
            error: parse_hex(&skin.colors.error),
            success: parse_hex(&skin.colors.success),
            muted: Color::DarkGray,
        };
        let stylesheet = StyleSheet::from_palette(&palette);
        Ok(Theme {
            name: skin.name,
            palette,
            stylesheet,
            source: ThemeSource::YamlFile(path.to_path_buf()),
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dark_theme_defaults() {
        let theme = ThemeLoader::builtin_dark();
        assert_eq!(theme.name, "dark");
        assert!(matches!(theme.source, ThemeSource::Builtin));
        // Palette colors are inherited from Palette::dark() — just spot-check
        assert_eq!(theme.palette.accent, Color::Cyan);
        assert_eq!(theme.palette.bg, Color::Black);
        assert_eq!(theme.palette.fg, Color::White);
    }

    #[test]
    fn light_theme_defaults() {
        let theme = ThemeLoader::builtin_light();
        assert_eq!(theme.name, "light");
        assert!(matches!(theme.source, ThemeSource::Builtin));
        assert_eq!(theme.palette.accent, Color::Blue);
        assert_eq!(theme.palette.bg, Color::White);
        assert_eq!(theme.palette.fg, Color::Black);
    }

    #[test]
    fn heading1_is_bold_accent() {
        let theme = ThemeLoader::builtin_dark();
        let s = &theme.stylesheet.heading1;
        // fg should be accent color
        assert_eq!(s.fg, Some(Color::Cyan));
        // bold modifier should be set
        assert!(
            s.add_modifier.contains(Modifier::BOLD),
            "heading1 should be bold"
        );
    }

    #[test]
    fn stylesheet_derived_from_palette() {
        let theme = ThemeLoader::builtin_dark();
        let ss = &theme.stylesheet;

        // Focused borders use accent
        assert_eq!(ss.border_focused.fg, Some(Color::Cyan));
        // Unfocused borders use muted
        assert_eq!(ss.border_unfocused.fg, Some(Color::DarkGray));
        // Error status is bold red
        assert_eq!(ss.tool_status_error.fg, Some(Color::Red));
        assert!(ss.tool_status_error.add_modifier.contains(Modifier::BOLD));
        // Done status uses success (green)
        assert_eq!(ss.tool_status_done.fg, Some(Color::Green));
    }

    #[test]
    fn yaml_roundtrip() {
        let palette = Palette {
            accent: Color::Rgb(0, 255, 255),
            bg: Color::Rgb(0, 0, 0),
            fg: Color::Rgb(255, 255, 255),
            user_color: Color::Rgb(0, 255, 0),
            warn: Color::Rgb(255, 255, 0),
            error: Color::Rgb(255, 0, 0),
            success: Color::Rgb(0, 255, 0),
            muted: Color::Rgb(169, 169, 169),
        };
        let yaml = serde_yaml::to_string(&palette).expect("serialize");
        let deserialized: Palette = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(palette, deserialized);
    }

    #[test]
    fn skin_migration() {
        // Write a temporary skin.yaml in SkinConfig format
        let tmp = std::env::temp_dir()
            .join(format!("cowd-skin-migration-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&tmp);
        let path = tmp.join("skin.yaml");
        let content = r###"
name: "legacy"
colors:
  accent: "#FF8800"
  bg: "#111111"
  fg: "#EEEEEE"
  user_color: "#3366FF"
  warn: "#FFAA00"
  error: "#DD0000"
  success: "#00DD00"
branding:
  agent_name: "TestBot"
  prompt_symbol: "> "
"###;
        std::fs::write(&path, content).expect("write skin.yaml");

        let theme = ThemeLoader::migrate_from_skin(&path).expect("migration");
        assert_eq!(theme.name, "legacy");
        assert_eq!(theme.palette.accent, Color::Rgb(0xFF, 0x88, 0x00));
        assert_eq!(theme.palette.bg, Color::Rgb(0x11, 0x11, 0x11));
        assert_eq!(theme.palette.fg, Color::Rgb(0xEE, 0xEE, 0xEE));
        assert_eq!(theme.palette.user_color, Color::Rgb(0x33, 0x66, 0xFF));
        assert_eq!(theme.palette.warn, Color::Rgb(0xFF, 0xAA, 0x00));
        assert_eq!(theme.palette.error, Color::Rgb(0xDD, 0x00, 0x00));
        assert_eq!(theme.palette.success, Color::Rgb(0x00, 0xDD, 0x00));
        // muted is not in SkinConfig — defaults to DarkGray
        assert_eq!(theme.palette.muted, Color::DarkGray);

        // Verify source is YamlFile
        assert!(matches!(theme.source, ThemeSource::YamlFile(_)));

        // Cleanup
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
