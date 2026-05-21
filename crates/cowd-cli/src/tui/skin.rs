// P2-1: SkinConfig — data-driven TUI theming.
// Derived from hermes-agent's skin_engine.py.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkinConfig {
    pub name: String,
    pub colors: ColorConfig,
    pub branding: BrandingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColorConfig {
    pub accent: String,
    pub bg: String,
    pub fg: String,
    pub user_color: String,
    pub warn: String,
    pub error: String,
    pub success: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrandingConfig {
    pub agent_name: String,
    pub prompt_symbol: String,
}

impl Default for SkinConfig {
    fn default() -> Self {
        Self {
            name: "default".into(),
            colors: ColorConfig {
                accent: "#00FFFF".into(), bg: "#000000".into(), fg: "#FFFFFF".into(),
                user_color: "#00FF00".into(), warn: "#FFFF00".into(),
                error: "#FF0000".into(), success: "#00FF00".into(),
            },
            branding: BrandingConfig { agent_name: "Cowd".into(), prompt_symbol: "> ".into() },
        }
    }
}

impl SkinConfig {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let yaml = std::fs::read_to_string(path).map_err(|e| format!("read: {e}"))?;
        serde_yaml::from_str(&yaml).map_err(|e| format!("parse: {e}"))
    }

    pub fn accent_color(&self) -> ratatui::style::Color {
        parse_hex(&self.colors.accent)
    }
}

fn parse_hex(hex: &str) -> ratatui::style::Color {
    let hex = hex.trim_start_matches('#');
    if hex.len() == 6 {
        if let (Ok(r), Ok(g), Ok(b)) = (
            u8::from_str_radix(&hex[0..2], 16), u8::from_str_radix(&hex[2..4], 16), u8::from_str_radix(&hex[4..6], 16)
        ) { return ratatui::style::Color::Rgb(r, g, b); }
    }
    ratatui::style::Color::Cyan
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test] fn p21_default_skin_is_valid() { let s = SkinConfig::default(); assert_eq!(s.name, "default"); }
    #[test] fn p21_hex_parse_works() { assert_eq!(parse_hex("#FF0000"), ratatui::style::Color::Rgb(255, 0, 0)); }
}