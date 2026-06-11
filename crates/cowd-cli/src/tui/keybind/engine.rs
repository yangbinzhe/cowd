// ── KeybindEngine ──────────────────────────────────────────────────
// Modal-layer stacking, multi-chord dispatch with 1 s timeout, and
// Which-Key visibility driven by the Space leader key.
//
// Architecture:
//   Space pushes into pending_chord (no single-key Space binding),
//   so it stays unresolved as a prefix. This naturally enters
//   "leader mode" — the Which-Key component renders all leader
//   bindings. Subsequent keys extend the chord, narrow the display,
//   and dispatch the matching Action on full match.
//
//   When no prefix match exists, the chord is immediately flushed.
//   A 1 s timeout fires via check_timeout() to clear stale prefixes.
// -------------------------------------------------------------------

#![allow(dead_code)]

use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use super::types::{Action, KeyBinding, KeyChord, KeyMap, ModalLayer};
use crate::tui::layout::LayoutPreset;

// ── Group constants for which-key ─────────────────────────────────
pub const GROUP_NAVIGATION: &str = "Navigation";
pub const GROUP_SESSION: &str = "Session";
pub const GROUP_FILES: &str = "Files";
pub const GROUP_DIALOG: &str = "Dialog";
pub const GROUP_SYSTEM: &str = "System";
pub const GROUP_LAYOUT: &str = "Layout";

// ── Constants ──────────────────────────────────────────────────────

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_MODAL_DEPTH: usize = 3;

// ── KeybindEngine ──────────────────────────────────────────────────

/// Central keybinding engine backed by a `KeyMap` and a modal-layer stack.
///
/// # Chord Resolution
///
/// On every key event the engine pushes the key onto `pending_chord` and
/// checks three outcomes in order:
///
/// 1. **Full match** — the accumulated chord exactly matches a binding.
///    The action is returned, pending is cleared, which-key closes.
/// 2. **Prefix match** — the accumulated chord is a prefix of at least one
///    longer binding. The engine waits (returns `None`), which-key renders
///    the continuation options.
/// 3. **No match** — the chord matches nothing and has no extensions.
///    Pending is flushed, no action dispatched.
///
/// # Space Leader
///
/// `Space` is treated like any other key — pushed onto `pending_chord`.
/// Because there are no single-key Space bindings, Space alone is always a
/// **prefix match** (it leads into Space-leader chords like `SPC f`).
/// This naturally causes which-key to appear.
///
/// # Timeout
///
/// `check_timeout()` should be called periodically. If the configured
/// timeout has elapsed since the last key press, pending is flushed.
///
/// # Modal Layers
///
/// Pushed via `push_modal()` / popped via `pop_modal()`. Resolution walks
/// the stack top-down before falling back to the base keymap. Max depth 3.
#[derive(Debug)]
pub struct KeybindEngine {
    key_map: KeyMap,
    modal_stack: Vec<ModalLayer>,
    pending_chord: Vec<KeyEvent>,
    last_key_time: Option<Instant>,
    /// Whether the which-key overlay is currently visible.
    pub which_key_visible: bool,
    /// Which group is selected in the which-key overlay (index into ALL_GROUPS).
    pub which_key_group: usize,
    timeout: Duration,
}

impl KeybindEngine {
    // ── Construction ────────────────────────────────────────────────

    /// Create a new engine backed by the given keymap.
    pub fn new(key_map: KeyMap) -> Self {
        Self {
            key_map,
            modal_stack: Vec::new(),
            pending_chord: Vec::new(),
            last_key_time: None,
            which_key_visible: false,
            which_key_group: 0,
            timeout: DEFAULT_TIMEOUT,
        }
    }

    /// Builder-style: override the chord timeout (for testing).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    // ── Key Handling ────────────────────────────────────────────────

    /// Process a single key event and return an action if a chord completes.
    ///
    /// Returns `Some(Action)` when the accumulated chord matches a binding
    /// exactly. Returns `None` when the chord is still pending (prefix match)
    /// or was invalid (no match, pending flushed).
    pub fn handle_key(&mut self, event: KeyEvent) -> Option<Action> {
        self.last_key_time = Some(Instant::now());

        self.pending_chord.push(event);
        let chord = KeyChord {
            keys: self.pending_chord.clone(),
        };

        // ── Full match? ─────────────────────────────────────────────
        if let Some(action) = self.resolve_chord(&chord) {
            let action = action.clone();
            self.pending_chord.clear();
            self.which_key_visible = false;
            return Some(action);
        }

        // ── Prefix match? → wait, show which-key ────────────────────
        if self.has_prefix_matches(&chord) {
            self.which_key_visible = true;
            return None;
        }

        // ── No match at all → flush ─────────────────────────────────
        self.pending_chord.clear();
        None
    }

    // ── Chord Resolution ────────────────────────────────────────────

    /// Walk modal stack top-down, then fall back to the base keymap.
    fn resolve_chord(&self, chord: &KeyChord) -> Option<&Action> {
        for layer in self.modal_stack.iter().rev() {
            for binding in &layer.bindings {
                if &binding.chord == chord {
                    return Some(&binding.action);
                }
            }
        }
        self.key_map.resolve(chord)
    }

    /// Returns `true` if any binding in the active scope extends `chord`.
    fn has_prefix_matches(&self, chord: &KeyChord) -> bool {
        let prefix_len = chord.keys.len();
        Self::layer_stack_has_prefix(&self.modal_stack, chord, prefix_len)
            || Self::map_has_prefix(&self.key_map, chord, prefix_len)
    }

    fn layer_stack_has_prefix(stack: &[ModalLayer], chord: &KeyChord, prefix_len: usize) -> bool {
        stack.iter().rev().any(|layer| {
            layer
                .bindings
                .iter()
                .any(|b| Self::chord_extends(b, chord, prefix_len))
        })
    }

    fn map_has_prefix(map: &KeyMap, chord: &KeyChord, prefix_len: usize) -> bool {
        map.bindings
            .iter()
            .any(|b| Self::chord_extends(b, chord, prefix_len))
    }

    /// `true` when `binding.chord.keys[..prefix_len]` equals `chord.keys`
    /// and the binding's chord is strictly longer (i.e. the chord is a prefix).
    fn chord_extends(binding: &KeyBinding, chord: &KeyChord, prefix_len: usize) -> bool {
        binding.chord.keys.len() > prefix_len
            && binding.chord.keys[..prefix_len]
                .iter()
                .zip(chord.keys.iter())
                .all(|(a, b)| a.code == b.code && a.modifiers == b.modifiers)
    }

    // ── Timeout ────────────────────────────────────────────────────

    /// Check if the chord timeout has elapsed and flush if so.
    ///
    /// Call this periodically from the event loop. When pending is
    /// flushed, `which_key_visible` is also cleared.
    pub fn check_timeout(&mut self) {
        if let Some(last) = self.last_key_time {
            if last.elapsed() >= self.timeout && !self.pending_chord.is_empty() {
                self.flush_pending();
            }
        }
    }

    /// Explicitly flush the pending chord and hide which-key.
    pub fn flush_pending(&mut self) {
        self.pending_chord.clear();
        self.which_key_visible = false;
    }

    // ── Modal Stack ────────────────────────────────────────────────

    /// Push a modal layer onto the stack (max depth [`MAX_MODAL_DEPTH`]).
    pub fn push_modal(&mut self, layer: ModalLayer) {
        if self.modal_stack.len() < MAX_MODAL_DEPTH {
            self.modal_stack.push(layer);
        }
    }

    /// Pop and return the topmost modal layer.
    pub fn pop_modal(&mut self) -> Option<ModalLayer> {
        self.modal_stack.pop()
    }

    /// Return the name of the active modal layer, if any.
    pub fn active_modal_name(&self) -> Option<&str> {
        self.modal_stack.last().map(|l| l.name.as_str())
    }

    /// Return the current modal stack depth.
    pub fn modal_depth(&self) -> usize {
        self.modal_stack.len()
    }

    // ── Which-Key Helpers ──────────────────────────────────────────

    /// Access the current pending chord (used by the which-key title bar).
    pub fn pending_chord(&self) -> &[KeyEvent] {
        &self.pending_chord
    }

    /// Return all bindings visible in the current scope, filtered to those
    /// that extend the pending prefix.
    ///
    /// * Empty prefix → only single-key top-level bindings.
    /// * Non-empty prefix → bindings that start with that prefix.
    pub fn visible_bindings(&self) -> Vec<&KeyBinding> {
        let prefix = &self.pending_chord;
        let mut bindings = Vec::new();

        for layer in self.modal_stack.iter().rev() {
            for binding in &layer.bindings {
                if Self::binding_matches_filter(binding, prefix) {
                    bindings.push(binding);
                }
            }
        }

        for binding in &self.key_map.bindings {
            if Self::binding_matches_filter(binding, prefix) {
                bindings.push(binding);
            }
        }

        bindings
    }

    fn binding_matches_filter(binding: &KeyBinding, prefix: &[KeyEvent]) -> bool {
        if prefix.is_empty() {
            return binding.chord.keys.len() == 1;
        }

        if binding.chord.keys.len() <= prefix.len() {
            return false;
        }

        binding.chord.keys[..prefix.len()]
            .iter()
            .zip(prefix.iter())
            .all(|(a, b)| a.code == b.code && a.modifiers == b.modifiers)
    }
}

// ── Default Bindings ──────────────────────────────────────────────

/// Build a `KeyMap` pre-populated with ALL keyboard shortcuts from input.rs.
///
/// Includes every shortcut: Enter, Esc, Ctrl+C, Tab, PgUp/Down, Home, End,
/// Ctrl+T/Y/A/E/W/U/K/Z/M, Alt+↑↓, /, ?, n/N, j/k, gg, Space leader chords.
/// Text-editing keys (Ctrl+A/E/W/U/K/Z, Shift+Enter) are NOT bound —
/// they pass directly to the tui-textarea widget.
#[must_use]
pub fn default_bindings() -> KeyMap {
    let mut map = KeyMap::new();

    // ── Navigation (j/k, arrows, PgUp/Dn, Home, End) ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE)],
        },
        Action::Scroll(1),
        "Scroll down",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE)],
        },
        Action::Scroll(-1),
        "Scroll up",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)],
        },
        Action::Scroll(-1),
        "Cursor/scroll up",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Down, KeyModifiers::NONE)],
        },
        Action::Scroll(1),
        "Cursor/scroll down",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::PageUp, KeyModifiers::NONE)],
        },
        Action::ScrollPage(-1),
        "Page up",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::PageDown, KeyModifiers::NONE)],
        },
        Action::ScrollPage(1),
        "Page down",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Home, KeyModifiers::NONE)],
        },
        Action::ScrollTop,
        "Scroll to top",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::End, KeyModifiers::NONE)],
        },
        Action::ScrollBottom,
        "Scroll to bottom",
        GROUP_NAVIGATION,
    );

    // ── Multi-chord ──
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('g'), KeyModifiers::NONE),
            ],
        },
        Action::ExpandCollapse,
        "Toggle expand/collapse",
        GROUP_NAVIGATION,
    );

    // ── Ctrl keys ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('b'), KeyModifiers::CONTROL)],
        },
        Action::TogglePanel("sidebar".into()),
        "Toggle sidebar",
        GROUP_FILES,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)],
        },
        Action::Quit,
        "Quit",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('t'), KeyModifiers::CONTROL)],
        },
        Action::ToggleTheme,
        "Toggle theme",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('y'), KeyModifiers::CONTROL)],
        },
        Action::Copy,
        "Copy focused entry",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('m'), KeyModifiers::CONTROL)],
        },
        Action::NextModel,
        "Switch model",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('p'), KeyModifiers::CONTROL)],
        },
        Action::TogglePerformanceDashboard,
        "Performance dashboard",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(
                KeyCode::Char('P'),
                KeyModifiers::CONTROL | KeyModifiers::SHIFT,
            )],
        },
        Action::ToggleCommandPalette,
        "Command palette",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('A'), KeyModifiers::NONE)],
        },
        Action::ToggleAgentsOverlay,
        "Toggle agents overlay",
        GROUP_SYSTEM,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('a'), KeyModifiers::CONTROL)],
        },
        Action::ToggleAgentPanel,
        "Toggle agent team panel",
        GROUP_SYSTEM,
    );

    // ── Search ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('/'), KeyModifiers::NONE)],
        },
        Action::Search,
        "Search timeline",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('n'), KeyModifiers::NONE)],
        },
        Action::SearchNext,
        "Next search match",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('N'), KeyModifiers::NONE)],
        },
        Action::SearchPrev,
        "Previous search match",
        GROUP_DIALOG,
    );

    // ── Help / which-key ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE)],
        },
        Action::ToggleHelp,
        "Toggle which-key",
        GROUP_DIALOG,
    );

    // ── History navigation ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Up, KeyModifiers::ALT)],
        },
        Action::HistoryBrowse(true),
        "Input history (older)",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Down, KeyModifiers::ALT)],
        },
        Action::HistoryBrowse(false),
        "Input history (newer)",
        GROUP_SESSION,
    );

    // ── Special keys ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE)],
        },
        Action::NextPanel,
        "Next panel/tab",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::BackTab, KeyModifiers::SHIFT)],
        },
        Action::PrevPanel,
        "Previous panel/tab",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)],
        },
        Action::Cancel,
        "Cancel",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)],
        },
        Action::SubmitInput,
        "Submit input",
        GROUP_SESSION,
    );

    // ── F-key layout presets ──
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::F(1), KeyModifiers::NONE)],
        },
        Action::ApplyPreset(LayoutPreset::Coding),
        "Coding layout (70/30)",
        GROUP_LAYOUT,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::F(2), KeyModifiers::NONE)],
        },
        Action::ApplyPreset(LayoutPreset::Review),
        "Review layout (50/50)",
        GROUP_LAYOUT,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![KeyEvent::new(KeyCode::F(3), KeyModifiers::NONE)],
        },
        Action::ApplyPreset(LayoutPreset::Collaboration),
        "Collaboration layout (30/30/40)",
        GROUP_LAYOUT,
    );

    // ── Space-leader chords ──
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('f'), KeyModifiers::NONE),
            ],
        },
        Action::FocusFileTree,
        "Focus file tree",
        GROUP_FILES,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('p'), KeyModifiers::NONE),
            ],
        },
        Action::ToggleCommandPalette,
        "Command palette",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE),
            ],
        },
        Action::Quit,
        "Quit",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('t'), KeyModifiers::NONE),
            ],
        },
        Action::ToggleTheme,
        "Toggle theme",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('e'), KeyModifiers::NONE),
            ],
        },
        Action::OpenDialog("export".into()),
        "Export session",
        GROUP_SESSION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('h'), KeyModifiers::NONE),
            ],
        },
        Action::ToggleHelp,
        "Toggle which-key",
        GROUP_DIALOG,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('j'), KeyModifiers::NONE),
            ],
        },
        Action::Scroll(5),
        "Scroll down 5",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('k'), KeyModifiers::NONE),
            ],
        },
        Action::Scroll(-5),
        "Scroll up 5",
        GROUP_NAVIGATION,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('d'), KeyModifiers::NONE),
            ],
        },
        Action::FocusDiff,
        "Show diff viewer",
        GROUP_FILES,
    );
    map.add_grouped(
        KeyChord {
            keys: vec![
                KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
                KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE),
            ],
        },
        Action::FocusSessions,
        "Focus sessions",
        GROUP_SESSION,
    );

    map
}

// ── Tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    // ── helpers ────────────────────────────────────────────────────

    fn k(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn k_ctrl(ch: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(ch), KeyModifiers::CONTROL)
    }

    fn engine() -> KeybindEngine {
        KeybindEngine::new(default_bindings())
    }

    // ── single_key_dispatch ────────────────────────────────────────

    #[test]
    fn single_key_dispatch_j_scrolls_down() {
        let mut eng = engine();
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('j'))),
            Some(Action::Scroll(1))
        );
    }

    #[test]
    fn single_key_dispatch_k_scrolls_up() {
        let mut eng = engine();
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('k'))),
            Some(Action::Scroll(-1))
        );
    }

    #[test]
    fn single_key_dispatch_ctrl_c_quits() {
        let mut eng = engine();
        assert_eq!(eng.handle_key(k_ctrl('c')), Some(Action::Quit));
    }

    #[test]
    fn single_key_dispatch_esc_cancels() {
        let mut eng = engine();
        assert_eq!(eng.handle_key(k(KeyCode::Esc)), Some(Action::Cancel));
    }

    #[test]
    fn single_key_dispatch_enter_submits() {
        let mut eng = engine();
        assert_eq!(eng.handle_key(k(KeyCode::Enter)), Some(Action::SubmitInput));
    }

    #[test]
    fn single_key_dispatch_ctrl_b_toggles_sidebar() {
        let mut eng = engine();
        assert_eq!(
            eng.handle_key(k_ctrl('b')),
            Some(Action::TogglePanel("sidebar".into()))
        );
    }

    #[test]
    fn single_key_unbound_returns_none() {
        let mut eng = engine();
        assert_eq!(eng.handle_key(k(KeyCode::Char('x'))), None);
        assert!(eng.pending_chord().is_empty());
    }

    // ── multi_chord_dispatch ───────────────────────────────────────

    #[test]
    fn multi_chord_gg_dispatches_expand_collapse() {
        let mut eng = engine();

        // First 'g' — prefix match, no action, which-key visible
        assert_eq!(eng.handle_key(k(KeyCode::Char('g'))), None);
        assert_eq!(eng.pending_chord().len(), 1);
        assert!(eng.which_key_visible);

        // Second 'g' — full match
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('g'))),
            Some(Action::ExpandCollapse)
        );
        assert!(eng.pending_chord().is_empty());
        assert!(!eng.which_key_visible);
    }

    #[test]
    fn multi_chord_wrong_second_key_flushes_pending() {
        let mut eng = engine();

        // 'g' → pending prefix
        assert_eq!(eng.handle_key(k(KeyCode::Char('g'))), None);
        assert_eq!(eng.pending_chord().len(), 1);

        // 'x' does not extend 'g' prefix → flushed
        assert_eq!(eng.handle_key(k(KeyCode::Char('x'))), None);
        assert!(eng.pending_chord().is_empty());
    }

    // ── space leader ───────────────────────────────────────────────

    #[test]
    fn space_alone_activates_which_key_no_action_dispatched() {
        let mut eng = engine();

        // Space alone: no single-key Space binding, so prefix match.
        assert_eq!(eng.handle_key(k(KeyCode::Char(' '))), None);
        assert_eq!(eng.pending_chord().len(), 1);
        assert!(eng.which_key_visible);
    }

    #[test]
    fn space_leader_chord_f_dispatches_focus_file_tree() {
        let mut eng = engine();

        // Space → prefix, which-key shows
        assert_eq!(eng.handle_key(k(KeyCode::Char(' '))), None);
        assert!(eng.which_key_visible);

        // f completes SPC-f → FocusFileTree
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('f'))),
            Some(Action::FocusFileTree)
        );
        assert!(eng.pending_chord().is_empty());
        assert!(!eng.which_key_visible);
    }

    #[test]
    fn space_leader_chord_p_dispatches_command_palette() {
        let mut eng = engine();
        assert_eq!(eng.handle_key(k(KeyCode::Char(' '))), None);
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('p'))),
            Some(Action::ToggleCommandPalette)
        );
    }

    #[test]
    fn space_leader_chord_q_dispatches_quit() {
        let mut eng = engine();
        assert_eq!(eng.handle_key(k(KeyCode::Char(' '))), None);
        assert_eq!(eng.handle_key(k(KeyCode::Char('q'))), Some(Action::Quit));
    }

    // ── modal_override ─────────────────────────────────────────────

    #[test]
    fn modal_layer_overrides_base_binding() {
        let mut eng = engine();

        // Base: 'j' → Scroll(1)
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('j'))),
            Some(Action::Scroll(1))
        );

        // Push a modal that overrides 'j' with Noop
        let mut modal = ModalLayer {
            name: "insert".into(),
            bindings: Vec::new(),
        };
        modal.bindings.push(KeyBinding {
            chord: KeyChord {
                keys: vec![k(KeyCode::Char('j'))],
            },
            action: Action::Noop,
            description: "j is noop in insert mode",
            modal: Some("insert".into()),
            group: "System",
        });
        eng.push_modal(modal);

        // Now 'j' resolves to Noop from modal
        assert_eq!(eng.handle_key(k(KeyCode::Char('j'))), Some(Action::Noop));

        // Pop → 'j' goes back to Scroll(1)
        eng.pop_modal();
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('j'))),
            Some(Action::Scroll(1))
        );
    }

    #[test]
    fn modal_layer_adds_new_binding() {
        let mut eng = engine();

        // 'x' is unbound by default
        assert_eq!(eng.handle_key(k(KeyCode::Char('x'))), None);

        // Push modal that adds 'x' → Copy
        let mut modal = ModalLayer {
            name: "special".into(),
            bindings: Vec::new(),
        };
        modal.bindings.push(KeyBinding {
            chord: KeyChord {
                keys: vec![k(KeyCode::Char('x'))],
            },
            action: Action::Copy,
            description: "Copy in special mode",
            modal: Some("special".into()),
            group: "System",
        });
        eng.push_modal(modal);

        assert_eq!(eng.handle_key(k(KeyCode::Char('x'))), Some(Action::Copy));

        // Pop → 'x' unbound again
        eng.pop_modal();
        assert_eq!(eng.handle_key(k(KeyCode::Char('x'))), None);
    }

    #[test]
    fn modal_max_depth_enforced() {
        let mut eng = engine();

        for i in 0..5 {
            eng.push_modal(ModalLayer {
                name: format!("layer{i}"),
                bindings: Vec::new(),
            });
        }

        assert_eq!(eng.modal_depth(), MAX_MODAL_DEPTH);
    }

    #[test]
    fn modal_pop_empty_stack_returns_none() {
        let mut eng = engine();
        assert!(eng.pop_modal().is_none());
    }

    // ── timeout_flushes ────────────────────────────────────────────

    #[test]
    fn timeout_flushes_pending_chord_and_hides_which_key() {
        let mut eng = KeybindEngine::new(default_bindings()).with_timeout(Duration::from_millis(1));

        // Start a prefix
        assert_eq!(eng.handle_key(k(KeyCode::Char('g'))), None);
        assert_eq!(eng.pending_chord().len(), 1);
        assert!(eng.which_key_visible);

        // Wait for timeout
        std::thread::sleep(Duration::from_millis(5));
        eng.check_timeout();

        assert!(eng.pending_chord().is_empty());
        assert!(!eng.which_key_visible);
    }

    #[test]
    fn timeout_does_not_panic_on_empty_pending() {
        let mut eng = KeybindEngine::new(default_bindings()).with_timeout(Duration::from_millis(1));

        // No keys pressed — nothing to flush
        eng.check_timeout();
        assert!(eng.pending_chord().is_empty());
    }

    #[test]
    fn timeout_resets_after_new_key_press() {
        let mut eng =
            KeybindEngine::new(default_bindings()).with_timeout(Duration::from_millis(500));

        // Press 'g'
        assert_eq!(eng.handle_key(k(KeyCode::Char('g'))), None);

        // Wait a bit, but not long enough
        std::thread::sleep(Duration::from_millis(100));

        // Complete the chord before timeout
        assert_eq!(
            eng.handle_key(k(KeyCode::Char('g'))),
            Some(Action::ExpandCollapse)
        );
    }

    // ── flush_pending ──────────────────────────────────────────────

    #[test]
    fn flush_pending_clears_state() {
        let mut eng = engine();

        assert_eq!(eng.handle_key(k(KeyCode::Char(' '))), None);
        assert_eq!(eng.pending_chord().len(), 1);
        assert!(eng.which_key_visible);

        eng.flush_pending();
        assert!(eng.pending_chord().is_empty());
        assert!(!eng.which_key_visible);
    }

    // ── visible_bindings ───────────────────────────────────────────

    #[test]
    fn visible_bindings_empty_prefix_shows_only_single_key() {
        let eng = engine();
        let bindings = eng.visible_bindings();

        assert!(!bindings.is_empty());
        for b in &bindings {
            assert_eq!(
                b.chord.keys.len(),
                1,
                "empty prefix should only show single-key bindings, got: {:?}",
                b.chord.keys
            );
        }
    }

    #[test]
    fn visible_bindings_with_space_prefix_shows_leader_chords() {
        let mut eng = engine();

        // Put Space into pending
        assert_eq!(eng.handle_key(k(KeyCode::Char(' '))), None);
        let bindings = eng.visible_bindings();

        for b in &bindings {
            assert!(
                b.chord.keys.len() >= 2,
                "space prefix should show multi-key chords, got {:?}",
                b.chord.keys
            );
            assert_eq!(
                b.chord.keys[0].code,
                KeyCode::Char(' '),
                "all should start with Space"
            );
        }

        // 10 leader chords in default_bindings
        assert_eq!(bindings.len(), 10);
    }

    // ── active_modal_name ──────────────────────────────────────────

    #[test]
    fn active_modal_name_reports_top_of_stack() {
        let mut eng = engine();
        assert!(eng.active_modal_name().is_none());

        eng.push_modal(ModalLayer {
            name: "picker".into(),
            bindings: Vec::new(),
        });
        assert_eq!(eng.active_modal_name(), Some("picker"));

        eng.push_modal(ModalLayer {
            name: "confirm".into(),
            bindings: Vec::new(),
        });
        assert_eq!(eng.active_modal_name(), Some("confirm"));

        eng.pop_modal();
        assert_eq!(eng.active_modal_name(), Some("picker"));
    }
}
