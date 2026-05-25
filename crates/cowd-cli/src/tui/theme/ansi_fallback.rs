// ── ANSI Fallback — RGB→8-bit color degradation ─────────────────
// When the terminal doesn't support truecolor (24-bit), this module
// maps arbitrary (R, G, B) triples to the closest 256-color ANSI
// palette entry using Euclidean distance.
//
// Results are cached so repeated lookups are O(1) after the first.
//
// Usage:
//   let color = to_terminal_color(255, 128, 64);
//   match color {
//     Color::Rgb(..) => …,   // terminal supports truecolor
//     Color::Indexed(i) => …, // fallback to 8-bit
//   }

#![allow(dead_code)]

use ratatui::style::Color;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

// ── ANSI 256-color palette ───────────────────────────────────────

/// Build the 256-color ANSI palette as a list of (r, g, b) tuples.
///
/// Layout:
///   0..=15   — 16 standard / bright colors
///   16..=231 — 216 colors from a 6×6×6 RGB cube
///   232..=255 — 24-step grayscale ramp
fn build_ansi_palette() -> Vec<(u8, u8, u8)> {
    let mut colors = Vec::with_capacity(256);

    // Standard 16 colors (indices 0–15)
    let standard: [(u8, u8, u8); 16] = [
        (0, 0, 0),         // 0  Black
        (128, 0, 0),       // 1  Red
        (0, 128, 0),       // 2  Green
        (128, 128, 0),     // 3  Yellow
        (0, 0, 128),       // 4  Blue
        (128, 0, 128),     // 5  Magenta
        (0, 128, 128),     // 6  Cyan
        (192, 192, 192),   // 7  White
        (128, 128, 128),   // 8  Bright Black (Gray)
        (255, 0, 0),       // 9  Bright Red
        (0, 255, 0),       // 10 Bright Green
        (255, 255, 0),     // 11 Bright Yellow
        (0, 0, 255),       // 12 Bright Blue
        (255, 0, 255),     // 13 Bright Magenta
        (0, 255, 255),     // 14 Bright Cyan
        (255, 255, 255),   // 15 Bright White
    ];
    colors.extend_from_slice(&standard);

    // 216-color cube (indices 16–231)
    // Each channel: r/g/b ∈ {0, 95, 135, 175, 215, 255}
    for r in 0..6 {
        for g in 0..6 {
            for b in 0..6 {
                let rv = if r == 0 { 0 } else { r * 40 + 55 };
                let gv = if g == 0 { 0 } else { g * 40 + 55 };
                let bv = if b == 0 { 0 } else { b * 40 + 55 };
                colors.push((rv, gv, bv));
            }
        }
    }

    // Grayscale ramp (indices 232–255): values 8, 18, 28, …, 238
    for i in 0..24 {
        let v = i * 10 + 8;
        colors.push((v, v, v));
    }

    colors
}

/// Returns a reference to the lazily-initialized ANSI 256-color palette.
fn ansi_palette() -> &'static [(u8, u8, u8)] {
    static PALETTE: OnceLock<Vec<(u8, u8, u8)>> = OnceLock::new();
    PALETTE.get_or_init(build_ansi_palette)
}

// ── Cache ────────────────────────────────────────────────────────

/// Global cache mapping (R, G, B) → closest ANSI 8-bit index.
fn cache() -> &'static Mutex<HashMap<(u8, u8, u8), u8>> {
    static CACHE: OnceLock<Mutex<HashMap<(u8, u8, u8), u8>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Clear the ANSI color cache (useful for testing).
pub fn clear_cache() {
    if let Ok(mut cache) = cache().lock() {
        cache.clear();
    }
}

// ── Truecolor detection ──────────────────────────────────────────

/// Detect whether the terminal supports truecolor (24-bit color).
///
/// Checks the `COLORTERM` environment variable; returns `true` if
/// its value contains `"truecolor"` or `"24bit"`.
pub fn detect_truecolor() -> bool {
    std::env::var("COLORTERM")
        .map(|v| v.contains("truecolor") || v.contains("24bit"))
        .unwrap_or(false)
}

// ── RGB → ANSI 8-bit mapping ─────────────────────────────────────

/// Map an (R, G, B) triple to the closest ANSI 256-color index using
/// Euclidean distance (squared distance; no sqrt needed for comparison).
///
/// Results are cached in a global `HashMap<(u8, u8, u8), u8>` so
/// repeated lookups of the same color are O(1).
pub fn rgb_to_ansi8(r: u8, g: u8, b: u8) -> u8 {
    // Check cache first
    if let Ok(cache) = cache().lock() {
        if let Some(&idx) = cache.get(&(r, g, b)) {
            return idx;
        }
    }

    let palette = ansi_palette();
    let mut best_idx = 0u8;
    let mut best_dist = u32::MAX;

    for (i, &(pr, pg, pb)) in palette.iter().enumerate() {
        let dr = r as i32 - pr as i32;
        let dg = g as i32 - pg as i32;
        let db = b as i32 - pb as i32;
        // Squared Euclidean distance — no sqrt needed for comparison
        let dist = (dr * dr + dg * dg + db * db) as u32;

        if dist < best_dist {
            best_dist = dist;
            best_idx = i as u8;
            if dist == 0 {
                break; // exact match, no need to keep searching
            }
        }
    }

    // Store in cache
    if let Ok(mut cache) = cache().lock() {
        cache.insert((r, g, b), best_idx);
    }

    best_idx
}

// ── Terminal color dispatch ──────────────────────────────────────

/// Return the appropriate ratatui `Color` for an (R, G, B) triple.
///
/// If the terminal supports truecolor (detected via `detect_truecolor()`),
/// returns `Color::Rgb(r, g, b)`. Otherwise falls back to the closest
/// 8-bit ANSI color via `Color::Indexed(idx)`.
pub fn to_terminal_color(r: u8, g: u8, b: u8) -> Color {
    if detect_truecolor() {
        Color::Rgb(r, g, b)
    } else {
        Color::Indexed(rgb_to_ansi8(r, g, b))
    }
}

// ── Tests ────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_no_truecolor() {
        // When COLORTERM is unset, detect_truecolor should return false
        std::env::remove_var("COLORTERM");
        assert!(!detect_truecolor());
    }

    #[test]
    fn chooses_best_8bit() {
        // Pure red → ANSI 9 (Bright Red)
        assert_eq!(rgb_to_ansi8(255, 0, 0), 9);
        // Pure green → ANSI 10 (Bright Green)
        assert_eq!(rgb_to_ansi8(0, 255, 0), 10);
        // Pure blue → ANSI 12 (Bright Blue)
        assert_eq!(rgb_to_ansi8(0, 0, 255), 12);
        // Pure white → ANSI 15 (Bright White)
        assert_eq!(rgb_to_ansi8(255, 255, 255), 15);
        // Pure black → ANSI 0 (Black)
        assert_eq!(rgb_to_ansi8(0, 0, 0), 0);
    }

    #[test]
    fn truecolor_passthrough() {
        let old = std::env::var("COLORTERM").ok();
        std::env::set_var("COLORTERM", "truecolor");
        let c = to_terminal_color(100, 150, 200);
        assert_eq!(c, Color::Rgb(100, 150, 200));
        // Restore original state
        match old {
            Some(v) => std::env::set_var("COLORTERM", v),
            None => std::env::remove_var("COLORTERM"),
        }
    }

    #[test]
    fn cache_hit() {
        clear_cache();
        // First call — cache miss, computes and stores
        let idx1 = rgb_to_ansi8(123, 45, 67);
        // Second call — should hit cache and return same value
        let idx2 = rgb_to_ansi8(123, 45, 67);
        assert_eq!(idx1, idx2, "cache should return identical result");

        // Verify cache actually stored the mapping
        let guard = cache().lock().unwrap_or_else(|e| e.into_inner());
        assert_eq!(
            guard.get(&(123, 45, 67)),
            Some(&idx1),
            "cache should contain (123, 45, 67)"
        );
    }
}
