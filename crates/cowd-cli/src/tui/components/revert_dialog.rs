// Task 6.1 — Per-message revert dialog with diff preview.
//
// Architecture:
//   RevertDialog struct   — helper that opens a DialogKind::RevertConfirm
//                           on the DialogManager and later extracts the
//                           result as a pending_revert_to flag.
//   parse_diffs()         — parse unified diff → Vec<DiffFile>
//   DiffFile              — filename + addition/deletion counts
//
// Usage:
//   let mut rd = RevertDialog::new();
//   rd.open_revert_dialog(&mut dm, msg_idx, diff_text);
//   // ... render loop processes dialog ...
//   if let Some(idx) = rd.take_revert_result(&mut dm) {
//       // idx = message index to revert to
//   }
//
// The DialogKind::RevertConfirm variant is defined in dialog.rs.

use crate::tui::components::dialog::{DialogKind, DialogManager, DialogResult, DialogState};

// ─── DiffFile ──────────────────────────────────────────────────────────

/// A single file change extracted from unified diff output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiffFile {
    /// Relative file path (e.g. "src/main.rs").
    pub filename: String,
    /// Number of added lines (count of lines starting with `+` in hunks).
    pub additions: usize,
    /// Number of deleted lines (count of lines starting with `-` in hunks).
    pub deletions: usize,
}

// ─── Diff Parser ───────────────────────────────────────────────────────

/// Parse a unified diff string and return a list of changed files with
/// addition/deletion counts.
///
/// Uses the same algorithm as opencode revert-diff.ts:
/// - `diff --git a/<path> b/<path>` identifies the file
/// - `^+` lines (excluding `^+++`) count as additions
/// - `^-` lines (excluding `^---`) count as deletions
///
/// Returns an empty `Vec` when the diff text is empty or unparseable.
pub fn parse_diffs(diff_text: &str) -> Vec<DiffFile> {
    if diff_text.is_empty() {
        return Vec::new();
    }

    let mut files: Vec<DiffFile> = Vec::new();
    let mut current_file: Option<&str> = None;
    let mut additions: usize = 0;
    let mut deletions: usize = 0;

    for line in diff_text.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            if let Some(b_part) = rest.split_once(' ') {
                let path = b_part.1.strip_prefix("b/").unwrap_or(b_part.1);
                if let Some(fname) = current_file.take() {
                    if additions > 0 || deletions > 0 {
                        files.push(DiffFile {
                            filename: fname.to_string(),
                            additions,
                            deletions,
                        });
                    }
                }
                current_file = Some(path);
                additions = 0;
                deletions = 0;
            }
        } else if line.starts_with("+") && !line.starts_with("+++") {
            additions += 1;
        } else if line.starts_with("-") && !line.starts_with("---") {
            deletions += 1;
        }
    }

    if let Some(fname) = current_file {
        if additions > 0 || deletions > 0 {
            files.push(DiffFile {
                filename: fname.to_string(),
                additions,
                deletions,
            });
        }
    }

    files
}

// ─── RevertDialog ──────────────────────────────────────────────────────

/// Manages the "Revert to here" confirmation dialog lifecycle.
///
/// # State
///
/// - `pending_revert_to` — set to `Some(message_index)` when the user
///   confirms the revert. Reset to `None` once consumed.
///
/// # Pattern
///
/// Similar to `SessionSidebar::pending_fork_at`: the component sets a
/// flag that the caller (TuiState / event loop) polls after the dialog
/// is dismissed.
#[derive(Debug, Clone)]
pub struct RevertDialog {
    /// Set when user confirms revert. Caller implements actual reversal.
    pub pending_revert_to: Option<usize>,
}

impl RevertDialog {
    /// Create a new revert dialog with no pending revert.
    #[must_use]
    pub fn new() -> Self {
        Self {
            pending_revert_to: None,
        }
    }

    /// Open a revert confirmation dialog for the given message index.
    ///
    /// Parses `diff_text` to build the file-change preview, then pushes
    /// a `DialogKind::RevertConfirm` onto the dialog manager.
    pub fn open_revert_dialog(
        &mut self,
        dialog_manager: &mut DialogManager,
        _message_index: usize,
        diff_text: &str,
    ) {
        let files = parse_diffs(diff_text);
        let dialog = DialogState::new(DialogKind::RevertConfirm {
            title: " Revert to this point? ".to_string(),
            files: files
                .into_iter()
                .map(|f| (f.filename, f.additions, f.deletions))
                .collect(),
        });
        dialog_manager.push(dialog);
    }

    /// Consume the result of a dismissed revert dialog.
    ///
    /// Call after the dialog has been dismissed (dialog_manager is empty).
    /// Returns `Some(message_index)` if the user confirmed, `None` otherwise.
    ///
    /// As a side effect, sets `self.pending_revert_to` to the confirmed
    /// index (or leaves it unchanged on cancel).
    pub fn take_revert_result(&mut self, dialog_manager: &mut DialogManager) -> Option<usize> {
        let result = dialog_manager.take_last_dismissed_result()?;
        match result {
            DialogResult::Yes => {
                let idx = self.pending_revert_to?;
                Some(idx)
            }
            _ => None,
        }
    }

    /// Consume and return the pending revert flag, resetting it.
    pub fn take_pending_revert(&mut self) -> Option<usize> {
        self.pending_revert_to.take()
    }
}

impl Default for RevertDialog {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::components::dialog::DialogManager;

    // ── Diff parser tests ──────────────────────────────────────────

    #[test]
    fn parse_counts_add_del() {
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
index abc..def 100644
--- a/src/main.rs
+++ b/src/main.rs
@@ -1,5 +1,7 @@
 fn main() {
-    println!(\"old\");
+    println!(\"hello\");
+    println!(\"world\");
 }
";
        let files = parse_diffs(diff);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "src/main.rs");
        assert_eq!(files[0].additions, 2);
        assert_eq!(files[0].deletions, 1);
    }

    #[test]
    fn empty_diff_no_files() {
        let files = parse_diffs("");
        assert!(files.is_empty());
    }

    #[test]
    fn parse_multiple_files() {
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
@@ -1 +1 @@
-old
+new
diff --git a/src/utils.rs b/src/utils.rs
--- a/src/utils.rs
+++ b/src/utils.rs
@@ -1,2 +1,3 @@
 a
 b
+c
";
        let files = parse_diffs(diff);
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].filename, "src/main.rs");
        assert_eq!(files[0].additions, 1);
        assert_eq!(files[0].deletions, 1);
        assert_eq!(files[1].filename, "src/utils.rs");
        assert_eq!(files[1].additions, 1);
        assert_eq!(files[1].deletions, 0);
    }

    #[test]
    fn parse_garbage_diff_no_crash() {
        let diff = "this is not a valid diff\nbut it should not crash\n";
        let files = parse_diffs(diff);
        assert!(files.is_empty());
    }

    #[test]
    fn parse_only_headers_no_hunks() {
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
";
        let files = parse_diffs(diff);
        assert!(
            files.is_empty(),
            "No hunks should produce no file entries: got {len}",
            len = files.len()
        );
    }

    #[test]
    fn diff_with_new_file() {
        let diff = "\
diff --git a/src/new.rs b/src/new.rs
new file mode 100644
--- /dev/null
+++ b/src/new.rs
@@ -0,0 +1,3 @@
+fn greet() {
+    println!(\"hi\");
+}
";
        let files = parse_diffs(diff);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].filename, "src/new.rs");
        assert_eq!(files[0].additions, 3);
        assert_eq!(files[0].deletions, 0);
    }

    // ── RevertDialog lifecycle tests ───────────────────────────────

    #[test]
    fn revert_shows_diff_preview() {
        let mut dm = DialogManager::new();
        let mut rd = RevertDialog::new();
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
@@ -1 +1 @@
-old
+new
";
        rd.open_revert_dialog(&mut dm, 3, diff);
        assert!(!dm.is_empty(), "Dialog should be pushed");

        let current = dm.current().unwrap();
        match &current.kind {
            DialogKind::RevertConfirm { title, files } => {
                assert!(title.contains("Revert"), "Title should mention revert");
                assert_eq!(files.len(), 1);
                assert_eq!(files[0].0, "src/main.rs");
                assert_eq!(files[0].1, 1);
                assert_eq!(files[0].2, 1);
            }
            other => panic!("Expected RevertConfirm, got {other:?}"),
        }
    }

    #[test]
    fn confirm_sets_flag() {
        let mut dm = DialogManager::new();
        let mut rd = RevertDialog::new();
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
@@ -1 +1 @@
-old
+new
";
        rd.open_revert_dialog(&mut dm, 5, diff);

        let ev = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('y'),
            crossterm::event::KeyModifiers::NONE,
        );
        assert!(dm.handle_key(&ev), "y key should be consumed");
        assert!(dm.is_empty(), "Dialog should be dismissed after confirm");

        let result = dm.take_last_dismissed_result();
        assert_eq!(result, Some(DialogResult::Yes));
    }

    #[test]
    fn cancel_noop() {
        let mut dm = DialogManager::new();
        let mut rd = RevertDialog::new();
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
@@ -1 +1 @@
-old
+new
";
        rd.open_revert_dialog(&mut dm, 5, diff);

        let ev = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Char('n'),
            crossterm::event::KeyModifiers::NONE,
        );
        assert!(dm.handle_key(&ev), "n key should be consumed");
        assert!(dm.is_empty(), "Dialog should be dismissed after cancel");

        let result = dm.take_last_dismissed_result();
        assert_eq!(result, Some(DialogResult::No));
    }

    #[test]
    fn esc_cancels() {
        let mut dm = DialogManager::new();
        let mut rd = RevertDialog::new();
        let diff = "\
diff --git a/src/main.rs b/src/main.rs
--- a/src/main.rs
+++ b/src/main.rs
@@ -1 +1 @@
-old
+new
";
        rd.open_revert_dialog(&mut dm, 5, diff);

        let ev = crossterm::event::KeyEvent::new(
            crossterm::event::KeyCode::Esc,
            crossterm::event::KeyModifiers::NONE,
        );
        assert!(dm.handle_key(&ev), "Esc should be consumed");
        assert!(dm.is_empty(), "Dialog should be dismissed on Esc");
    }

    #[test]
    fn open_and_confirm_sets_pending_flag() {
        use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

        let mut dm = DialogManager::new();
        let mut rd = RevertDialog::new();
        let diff = "\
diff --git a/a.rs b/a.rs
--- a/a.rs
+++ a/a.rs
@@ -1 +1 @@
-a
+b
";
        rd.pending_revert_to = Some(3);
        rd.open_revert_dialog(&mut dm, 3, diff);

        dm.handle_key(&KeyEvent::new(KeyCode::Char('y'), KeyModifiers::NONE));

        let result = rd.take_revert_result(&mut dm);
        assert_eq!(
            result,
            Some(3),
            "take_revert_result should return confirmed message index"
        );

        let taken = rd.take_pending_revert();
        assert_eq!(taken, Some(3));
        assert!(rd.pending_revert_to.is_none(), "Flag should be consumed after take");
    }

    #[test]
    fn default_is_no_pending() {
        let rd = RevertDialog::new();
        assert!(rd.pending_revert_to.is_none());
    }

    #[test]
    fn take_pending_returns_none_when_empty() {
        let mut rd = RevertDialog::new();
        assert_eq!(rd.take_pending_revert(), None);
    }
}
