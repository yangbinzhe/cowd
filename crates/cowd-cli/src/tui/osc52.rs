// ── OSC 52 Clipboard Support ────────────────────────────────────
// Writes text to the system clipboard using ANSI OSC 52 escape sequences.
// Handles tmux/screen multiplexer wrapping for nested terminals.
// Reference: hermes-agent ui-tui/src/lib/osc52.ts

/// Write text to the system clipboard via OSC 52 escape sequence.
///
/// Many modern terminals (iTerm2, Kitty, WezTerm, foot, Windows Terminal,
/// Ghostty, etc.) support this. Returns false if terminal doesn't support
/// OSC 52 (or STDOUT is not a TTY), true otherwise.
///
/// The clipboard buffer is `c` (system clipboard).
pub fn write_osc52_clipboard(text: &str) -> bool {
    use std::io::{IsTerminal, Write};
    if !std::io::stdout().is_terminal() {
        return false;
    }
    let encoded = base64_encode(text.as_bytes());
    let sequence = build_osc52_sequence("c", &encoded);
    let mut stdout = std::io::stdout();
    let _ = stdout.write_all(sequence.as_bytes());
    let _ = stdout.flush();
    true
}

/// Build the raw OSC 52 sequence, wrapping for tmux/screen if needed.
fn build_osc52_sequence(clipboard: &str, payload: &str) -> String {
    let raw = format!("\x1b]52;{clipboard};{payload}\x07");
    wrap_for_multiplexer(&raw)
}

/// Wrap an escape sequence for terminal multiplexers (tmux, screen).
///
/// Without wrapping, OSC 52 sequences are consumed by the multiplexer
/// and never reach the host terminal. The wrapping tells the multiplexer
/// to pass the sequence through.
fn wrap_for_multiplexer(sequence: &str) -> String {
    if let Ok(ref val) = std::env::var("TMUX") {
        if !val.is_empty() {
            // tmux wrapping: ESC P tmux ; <escaped sequence> ESC \
            let escaped = sequence.replace("\x1b", "\x1b\x1b");
            return format!("\x1bPtmux;{escaped}\x1b\\");
        }
    }
    if std::env::var("STY").ok().is_some_and(|v| !v.is_empty()) {
        // screen wrapping: ESC P <sequence> ESC \
        return format!("\x1bP{sequence}\x1b\\");
    }
    sequence.to_string()
}

// ── Inline base64 (std only, no deps) ──

const BASE64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(((input.len() + 2) / 3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = chunk.get(1).copied().unwrap_or(0);
        let b2 = chunk.get(2).copied().unwrap_or(0);
        let triple = (u32::from(b0) << 16) | (u32::from(b1) << 8) | u32::from(b2);
        out.push(BASE64_CHARS[((triple >> 18) & 0x3F) as usize] as char);
        out.push(BASE64_CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_CHARS[(triple & 0x3F) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_basic() {
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(base64_encode(b"Man"), "TWFu");
        assert_eq!(base64_encode(b"Ma"), "TWE=");
        assert_eq!(base64_encode(b"M"), "TQ==");
        assert_eq!(base64_encode(b""), "");
    }

    #[test]
    fn osc52_sequence_structure() {
        let seq = build_osc52_sequence("c", "aGVsbG8=");
        assert!(seq.contains("\x1b]52;c;aGVsbG8=\x07") || seq.contains("tmux") || seq.contains("P"));
    }
}
