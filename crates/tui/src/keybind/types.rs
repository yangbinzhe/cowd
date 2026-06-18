use crossterm::event::KeyEvent;
use std::hash::{Hash, Hasher};

use crate::layout::LayoutPreset;

/// A sequence of key presses that triggers an action.
///
/// Supports multi-chord keybindings (e.g., `g` then `g` for "go to top").
/// `PartialEq` and `Hash` only consider `code` and `modifiers` (not `kind` or `state`)
/// since release/repeat variants should match their press counterpart.
#[derive(Debug, Clone)]
pub struct KeyChord {
    pub keys: Vec<KeyEvent>,
}

impl PartialEq for KeyChord {
    fn eq(&self, other: &Self) -> bool {
        if self.keys.len() != other.keys.len() {
            return false;
        }
        self.keys
            .iter()
            .zip(other.keys.iter())
            .all(|(a, b)| a.code == b.code && a.modifiers == b.modifiers)
    }
}

impl Eq for KeyChord {}

impl Hash for KeyChord {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.keys.len().hash(state);
        for key in &self.keys {
            key.code.hash(state);
            key.modifiers.hash(state);
        }
    }
}

/// All possible actions that can be triggered by keybindings.
///
/// Covers every keyboard shortcut found in the current `input.rs` handler,
/// plus expected actions for future panels and dialogs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    /// Execute a command string (e.g., `":help"`, `":session list"`).
    Execute(String),
    /// Respond to a pending daemon approval through the projection API.
    RespondGatewayApproval { id: String, approved: bool },
    /// Cancel a daemon task through the projection API.
    CancelGatewayTask(String),
    /// Complete a daemon task through the projection API.
    CompleteGatewayTask(String),
    /// Update a connector resource lifecycle state through the daemon API.
    RevalidateConnectorResource { reference: String, state: String },
    /// Promote connector resource metadata into memory through the daemon API.
    PromoteConnectorResourceToMemory {
        reference: String,
        session_id: Option<String>,
    },
    /// Toggle visibility of a named panel.
    TogglePanel(String),
    /// Open a dialog by name (picker, approval, file chooser, etc.).
    OpenDialog(String),
    /// Scroll by `delta` lines (positive = down, negative = up).
    Scroll(i16),
    /// Page up/down scroll (by viewport height).
    ScrollPage(i16),
    /// Scroll to top (offset = 0).
    ScrollTop,
    /// Scroll to bottom (auto-scroll on).
    ScrollBottom,
    /// Expand or collapse the focused timeline entry.
    ExpandCollapse,
    /// Copy the focused content to clipboard.
    Copy,
    /// Quit the application.
    Quit,
    /// Focus the next panel in the layout.
    NextPanel,
    /// Focus the previous panel in the layout.
    PrevPanel,
    /// Toggle between light and dark themes.
    ToggleTheme,
    /// Toggle help panel / which-key visibility.
    ToggleHelp,
    /// Activate search / incremental-find mode.
    Search,
    /// Navigate to next search match.
    SearchNext,
    /// Navigate to previous search match.
    SearchPrev,
    /// Cancel the current operation (search, dialog, picker, etc.).
    Cancel,
    /// Submit the current input buffer as a message.
    SubmitInput,
    /// Cycle to the next available model.
    NextModel,
    /// Reload provider/model registry from the active runtime configuration.
    ReloadProviders,
    /// Browse input history (true = older, false = newer).
    HistoryBrowse(bool),
    /// Toggle the command palette overlay.
    ToggleCommandPalette,
    /// Toggle the performance dashboard overlay.
    TogglePerformanceDashboard,
    /// Toggle the agents overlay visibility.
    ToggleAgentsOverlay,
    /// Toggle the agent team panel visibility.
    ToggleAgentPanel,
    /// Focus the Diff sidebar tab.
    FocusDiff,
    /// Focus the File Tree sidebar tab.
    FocusFileTree,
    /// Focus the Sessions sidebar tab.
    FocusSessions,
    /// Apply a F-key layout preset (F1=Coding, F2=Review, F3=Collaboration).
    ApplyPreset(LayoutPreset),
    /// No operation — consumes the event without side effects.
    Noop,
}

/// A single keybinding: a chord mapped to an action with metadata.
#[derive(Debug, Clone)]
pub struct KeyBinding {
    pub chord: KeyChord,
    pub action: Action,
    pub description: &'static str,
    pub modal: Option<String>,
    /// Group for which-key rendering: "Session" | "Navigation" | "Files" | "Dialog" | "System"
    pub group: &'static str,
}

/// A flat map of keybindings with lookup and query methods.
///
/// This is intentionally a simple `Vec`-backed structure. The engine layer
/// (Task 10) owns the active chord-pending state and multi-layer resolution.
/// `resolve` performs exact-match only; prefix matching and modal stacking
/// are handled at the engine level.
#[derive(Debug, Clone)]
pub struct KeyMap {
    pub bindings: Vec<KeyBinding>,
}

impl KeyMap {
    pub fn new() -> Self {
        KeyMap {
            bindings: Vec::new(),
        }
    }

    /// Resolve a chord to the first matching action via exact comparison.
    ///
    /// Returns `None` when no binding matches the chord exactly.
    /// Does **not** perform prefix matching — that is the engine's responsibility.
    pub fn resolve(&self, chord: &KeyChord) -> Option<&Action> {
        self.bindings
            .iter()
            .find(|b| &b.chord == chord)
            .map(|b| &b.action)
    }

    /// Add a keybinding without a modal scope.
    pub fn add(&mut self, chord: KeyChord, action: Action, description: &'static str) {
        self.bindings.push(KeyBinding {
            chord,
            action,
            description,
            modal: None,
            group: "System",
        });
    }

    /// Add a keybinding scoped to a specific modal layer.
    pub fn add_modal(
        &mut self,
        chord: KeyChord,
        action: Action,
        description: &'static str,
        modal: String,
    ) {
        self.bindings.push(KeyBinding {
            chord,
            action,
            description,
            modal: Some(modal),
            group: "System",
        });
    }

    /// Add a keybinding with a specified group.
    pub fn add_grouped(
        &mut self,
        chord: KeyChord,
        action: Action,
        description: &'static str,
        group: &'static str,
    ) {
        self.bindings.push(KeyBinding {
            chord,
            action,
            description,
            modal: None,
            group,
        });
    }

    /// Return all bindings that belong to a given modal layer.
    pub fn bindings_for_modal(&self, name: &str) -> Vec<&KeyBinding> {
        self.bindings
            .iter()
            .filter(|b| b.modal.as_deref() == Some(name))
            .collect()
    }

    /// Return all bindings that are *not* scoped to any modal layer.
    pub fn base_bindings(&self) -> Vec<&KeyBinding> {
        self.bindings.iter().filter(|b| b.modal.is_none()).collect()
    }
}

impl Default for KeyMap {
    fn default() -> Self {
        Self::new()
    }
}

/// A named modal layer with its own set of keybindings.
///
/// Modal layers are stacked at runtime: the most recently pushed layer
/// takes priority over layers below. The engine governs stacking and
/// resolution order.
#[derive(Debug, Clone)]
pub struct ModalLayer {
    pub name: String,
    pub bindings: Vec<KeyBinding>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    // ── helpers ──────────────────────────────────────────────────────────
    fn k(code: KeyCode, modifiers: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, modifiers)
    }

    fn c(keys: Vec<KeyEvent>) -> KeyChord {
        KeyChord { keys }
    }

    // ── tests ────────────────────────────────────────────────────────────

    #[test]
    fn resolve_single_key() {
        let mut map = KeyMap::new();
        let esc = k(KeyCode::Esc, KeyModifiers::NONE);
        map.add(c(vec![esc.clone()]), Action::Quit, "Quit");

        // Exact match
        assert_eq!(map.resolve(&c(vec![esc])), Some(&Action::Quit));
    }

    #[test]
    fn resolve_multi_chord() {
        let mut map = KeyMap::new();
        // "gg" -> go to top (ExpandCollapse as proxy)
        let g1 = k(KeyCode::Char('g'), KeyModifiers::NONE);
        let g2 = k(KeyCode::Char('g'), KeyModifiers::NONE);
        map.add(c(vec![g1, g2]), Action::ExpandCollapse, "Go to top");

        // Single "g" should NOT match
        let single_g = k(KeyCode::Char('g'), KeyModifiers::NONE);
        assert_eq!(map.resolve(&c(vec![single_g])), None);

        // Double "g" should match
        assert_eq!(
            map.resolve(&c(vec![
                k(KeyCode::Char('g'), KeyModifiers::NONE),
                k(KeyCode::Char('g'), KeyModifiers::NONE),
            ])),
            Some(&Action::ExpandCollapse)
        );
    }

    #[test]
    fn resolve_nonexistent() {
        let map = KeyMap::new();
        let enter = k(KeyCode::Enter, KeyModifiers::NONE);
        assert_eq!(map.resolve(&c(vec![enter])), None);
    }

    #[test]
    fn modal_layer_overrides_base() {
        // Simulate two stacked maps: a base and a modal layer.
        // The engine picks the topmost layer first; here we just verify
        // that the same chord maps to different actions in different maps.
        let mut base = KeyMap::new();
        let j = k(KeyCode::Char('j'), KeyModifiers::NONE);
        base.add(c(vec![j.clone()]), Action::Scroll(1), "Scroll down");

        let mut modal = KeyMap::new();
        modal.add(c(vec![j.clone()]), Action::Noop, "Override in modal");

        assert_eq!(base.resolve(&c(vec![j.clone()])), Some(&Action::Scroll(1)));
        assert_eq!(modal.resolve(&c(vec![j])), Some(&Action::Noop));
    }

    #[test]
    fn empty_map_returns_none() {
        let map = KeyMap::new();
        let esc = k(KeyCode::Esc, KeyModifiers::NONE);
        assert_eq!(map.resolve(&c(vec![esc])), None);
    }

    #[test]
    fn bindings_for_modal_filters_correctly() {
        let mut map = KeyMap::new();
        map.add(
            c(vec![k(KeyCode::Char('q'), KeyModifiers::NONE)]),
            Action::Quit,
            "Quit",
        );
        map.add_modal(
            c(vec![k(KeyCode::Char('j'), KeyModifiers::NONE)]),
            Action::Scroll(1),
            "Scroll",
            "picker".into(),
        );
        map.add_modal(
            c(vec![k(KeyCode::Char('k'), KeyModifiers::NONE)]),
            Action::Scroll(-1),
            "Scroll up",
            "picker".into(),
        );

        let picker_bindings = map.bindings_for_modal("picker");
        assert_eq!(picker_bindings.len(), 2);
        assert_eq!(picker_bindings[0].description, "Scroll");
        assert_eq!(picker_bindings[1].description, "Scroll up");

        let base = map.base_bindings();
        assert_eq!(base.len(), 1);
        assert_eq!(base[0].description, "Quit");
    }

    #[test]
    fn keychord_partial_eq_and_hash_consistency() {
        let a = c(vec![
            k(KeyCode::Char('g'), KeyModifiers::NONE),
            k(KeyCode::Char('g'), KeyModifiers::NONE),
        ]);
        let b = c(vec![
            k(KeyCode::Char('g'), KeyModifiers::NONE),
            k(KeyCode::Char('g'), KeyModifiers::NONE),
        ]);
        let c_diff = c(vec![
            k(KeyCode::Char('g'), KeyModifiers::NONE),
            k(KeyCode::Char('G'), KeyModifiers::SHIFT),
        ]);

        assert_eq!(a, b);
        assert_ne!(a, c_diff);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(a.clone());
        set.insert(b); // duplicate — should not increase size
        assert_eq!(set.len(), 1);
        set.insert(c_diff);
        assert_eq!(set.len(), 2);
    }
}
