// ── Palette — 8 Semantic Colors for TUI Theming ──────────────────
// Defines the core palette type and hex parsing, with custom serde
// so palettes can be serialized to/from YAML as hex strings.

#![allow(dead_code)]

use ratatui::style::Color;
use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, SerializeStruct, Serializer};

/// 14 semantic colors that make up a TUI theme palette.
#[derive(Debug, Clone, PartialEq)]
pub struct Palette {
    pub accent: Color,
    pub bg: Color,
    pub fg: Color,
    pub user_color: Color,
    pub warn: Color,
    pub error: Color,
    pub success: Color,
    pub muted: Color,
    pub agent_planner: Color,
    pub agent_executor: Color,
    pub agent_reviewer: Color,
    pub agent_idle: Color,
    pub agent_busy: Color,
    pub agent_offline: Color,
}

impl Palette {
    /// Builtin dark palette: black background, cyan accent, etc.
    pub fn dark() -> Self {
        Self {
            accent: Color::Cyan,
            bg: Color::Black,
            fg: Color::White,
            user_color: Color::Green,
            warn: Color::Yellow,
            error: Color::Red,
            success: Color::Green,
            muted: Color::DarkGray,
            agent_planner: Color::Blue,
            agent_executor: Color::Green,
            agent_reviewer: Color::Magenta,
            agent_idle: Color::Gray,
            agent_busy: Color::Yellow,
            agent_offline: Color::Red,
        }
    }

    /// Builtin light palette: white background, blue accent, etc.
    pub fn light() -> Self {
        Self {
            accent: Color::Blue,
            bg: Color::White,
            fg: Color::Black,
            user_color: Color::DarkGray,
            warn: Color::Yellow,
            error: Color::Red,
            success: Color::Green,
            muted: Color::Gray,
            agent_planner: Color::Blue,
            agent_executor: Color::Green,
            agent_reviewer: Color::Magenta,
            agent_idle: Color::DarkGray,
            agent_busy: Color::Yellow,
            agent_offline: Color::Red,
        }
    }
}

// ── Color ↔ hex-string helpers ──────────────────────────────────

fn color_to_hex_str(c: &Color) -> String {
    match c {
        Color::Rgb(r, g, b) => format!("#{:02X}{:02X}{:02X}", r, g, b),
        Color::Black => "#000000".into(),
        Color::Red => "#FF0000".into(),
        Color::Green => "#00FF00".into(),
        Color::Yellow => "#FFFF00".into(),
        Color::Blue => "#0000FF".into(),
        Color::Magenta => "#FF00FF".into(),
        Color::Cyan => "#00FFFF".into(),
        Color::White => "#FFFFFF".into(),
        Color::Gray => "#C0C0C0".into(),
        Color::DarkGray => "#808080".into(),
        Color::LightRed => "#FF6666".into(),
        Color::LightGreen => "#66FF66".into(),
        Color::LightYellow => "#FFFF66".into(),
        Color::LightBlue => "#6666FF".into(),
        Color::LightMagenta => "#FF66FF".into(),
        Color::LightCyan => "#66FFFF".into(),
        _ => "#00FFFF".into(), // Reset / Indexed → fallback to cyan
    }
}

/// Parse a hex color string (`#RRGGBB`) into a ratatui `Color`.
/// Returns `Color::Cyan` on failure (not parseable or wrong length).
pub fn parse_hex(hex: &str) -> Color {
    let hex = hex.trim_start_matches('#');
    if hex.len() == 6 {
        if let (Ok(r), Ok(g), Ok(b)) = (
            u8::from_str_radix(&hex[0..2], 16),
            u8::from_str_radix(&hex[2..4], 16),
            u8::from_str_radix(&hex[4..6], 16),
        ) {
            return Color::Rgb(r, g, b);
        }
    }
    Color::Cyan
}

// ── Custom serde: always roundtrip as hex strings ───────────────

impl Serialize for Palette {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut s = serializer.serialize_struct("Palette", 14)?;
        s.serialize_field("accent", &color_to_hex_str(&self.accent))?;
        s.serialize_field("bg", &color_to_hex_str(&self.bg))?;
        s.serialize_field("fg", &color_to_hex_str(&self.fg))?;
        s.serialize_field("user_color", &color_to_hex_str(&self.user_color))?;
        s.serialize_field("warn", &color_to_hex_str(&self.warn))?;
        s.serialize_field("error", &color_to_hex_str(&self.error))?;
        s.serialize_field("success", &color_to_hex_str(&self.success))?;
        s.serialize_field("muted", &color_to_hex_str(&self.muted))?;
        s.serialize_field("agent_planner", &color_to_hex_str(&self.agent_planner))?;
        s.serialize_field("agent_executor", &color_to_hex_str(&self.agent_executor))?;
        s.serialize_field("agent_reviewer", &color_to_hex_str(&self.agent_reviewer))?;
        s.serialize_field("agent_idle", &color_to_hex_str(&self.agent_idle))?;
        s.serialize_field("agent_busy", &color_to_hex_str(&self.agent_busy))?;
        s.serialize_field("agent_offline", &color_to_hex_str(&self.agent_offline))?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for Palette {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Helper struct that deserializes hex strings for each field.
        #[derive(serde::Deserialize)]
        struct PaletteData {
            accent: String,
            bg: String,
            fg: String,
            #[serde(rename = "user_color")]
            user_color: String,
            warn: String,
            error: String,
            success: String,
            muted: String,
            #[serde(rename = "agent_planner", default = "default_hex_blue")]
            agent_planner: String,
            #[serde(rename = "agent_executor", default = "default_hex_green")]
            agent_executor: String,
            #[serde(rename = "agent_reviewer", default = "default_hex_magenta")]
            agent_reviewer: String,
            #[serde(rename = "agent_idle", default = "default_hex_gray")]
            agent_idle: String,
            #[serde(rename = "agent_busy", default = "default_hex_yellow")]
            agent_busy: String,
            #[serde(rename = "agent_offline", default = "default_hex_red")]
            agent_offline: String,
        }

        fn default_hex_blue() -> String { "#0000FF".into() }
        fn default_hex_green() -> String { "#00FF00".into() }
        fn default_hex_magenta() -> String { "#FF00FF".into() }
        fn default_hex_gray() -> String { "#808080".into() }
        fn default_hex_yellow() -> String { "#FFFF00".into() }
        fn default_hex_red() -> String { "#FF0000".into() }

        let data = PaletteData::deserialize(deserializer)?;
        Ok(Palette {
            accent: parse_hex(&data.accent),
            bg: parse_hex(&data.bg),
            fg: parse_hex(&data.fg),
            user_color: parse_hex(&data.user_color),
            warn: parse_hex(&data.warn),
            error: parse_hex(&data.error),
            success: parse_hex(&data.success),
            muted: parse_hex(&data.muted),
            agent_planner: parse_hex(&data.agent_planner),
            agent_executor: parse_hex(&data.agent_executor),
            agent_reviewer: parse_hex(&data.agent_reviewer),
            agent_idle: parse_hex(&data.agent_idle),
            agent_busy: parse_hex(&data.agent_busy),
            agent_offline: parse_hex(&data.agent_offline),
        })
    }
}

// ── Tests ───────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dark_theme_defaults() {
        let p = Palette::dark();
        assert_eq!(p.accent, Color::Cyan);
        assert_eq!(p.bg, Color::Black);
        assert_eq!(p.fg, Color::White);
        assert_eq!(p.user_color, Color::Green);
        assert_eq!(p.warn, Color::Yellow);
        assert_eq!(p.error, Color::Red);
        assert_eq!(p.success, Color::Green);
        assert_eq!(p.muted, Color::DarkGray);
        assert_eq!(p.agent_planner, Color::Blue);
        assert_eq!(p.agent_executor, Color::Green);
        assert_eq!(p.agent_reviewer, Color::Magenta);
        assert_eq!(p.agent_idle, Color::Gray);
        assert_eq!(p.agent_busy, Color::Yellow);
        assert_eq!(p.agent_offline, Color::Red);
    }

    #[test]
    fn light_theme_defaults() {
        let p = Palette::light();
        assert_eq!(p.accent, Color::Blue);
        assert_eq!(p.bg, Color::White);
        assert_eq!(p.fg, Color::Black);
        assert_eq!(p.user_color, Color::DarkGray);
        assert_eq!(p.warn, Color::Yellow);
        assert_eq!(p.error, Color::Red);
        assert_eq!(p.success, Color::Green);
        assert_eq!(p.muted, Color::Gray);
        assert_eq!(p.agent_planner, Color::Blue);
        assert_eq!(p.agent_executor, Color::Green);
        assert_eq!(p.agent_reviewer, Color::Magenta);
        assert_eq!(p.agent_idle, Color::DarkGray);
        assert_eq!(p.agent_busy, Color::Yellow);
        assert_eq!(p.agent_offline, Color::Red);
    }

    #[test]
    fn hex_parse_valid() {
        assert_eq!(parse_hex("#FF0000"), Color::Rgb(255, 0, 0));
        assert_eq!(parse_hex("00FF00"), Color::Rgb(0, 255, 0));
        assert_eq!(parse_hex("#0000FF"), Color::Rgb(0, 0, 255));
        assert_eq!(parse_hex("ABCDEF"), Color::Rgb(0xAB, 0xCD, 0xEF));
    }

    #[test]
    fn hex_parse_invalid() {
        // Wrong length
        assert_eq!(parse_hex("#FFF"), Color::Cyan);
        assert_eq!(parse_hex("#FFFFF"), Color::Cyan);
        assert_eq!(parse_hex("#FFFFFFFF"), Color::Cyan);
        // Non-hex characters
        assert_eq!(parse_hex("#GGGGGG"), Color::Cyan);
        assert_eq!(parse_hex("not-hex"), Color::Cyan);
        // Empty
        assert_eq!(parse_hex(""), Color::Cyan);
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
            agent_planner: Color::Rgb(0, 0, 255),
            agent_executor: Color::Rgb(0, 255, 0),
            agent_reviewer: Color::Rgb(255, 0, 255),
            agent_idle: Color::Rgb(128, 128, 128),
            agent_busy: Color::Rgb(255, 255, 0),
            agent_offline: Color::Rgb(255, 0, 0),
        };
        let yaml = serde_yaml::to_string(&palette).expect("serialize");
        let deserialized: Palette = serde_yaml::from_str(&yaml).expect("deserialize");
        assert_eq!(palette, deserialized);
    }
}
