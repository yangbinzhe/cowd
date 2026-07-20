//! Canonical, grapheme-safe composer text model.
//!
//! The model owns authored bytes, cursor, selection and edit history.  Layout
//! is derived by the widget for a concrete terminal width; it never writes
//! back into this model.  This is deliberately independent from
//! `tui_textarea`, whose row/column cursor is a rendering concern rather than
//! the TUI's source of truth.

use std::ops::Range;

use unicode_segmentation::UnicodeSegmentation;

const MAX_EDIT_HISTORY: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComposerSnapshot {
    text: String,
    cursor: usize,
    selection_anchor: Option<usize>,
}

/// Canonical editor state for the TUI composer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposerModel {
    text: String,
    /// Byte offset, always snapped to a Unicode grapheme boundary.
    cursor: usize,
    /// The fixed end of a selection. `None` means there is no selection.
    selection_anchor: Option<usize>,
    revision: u64,
    undo: Vec<ComposerSnapshot>,
    redo: Vec<ComposerSnapshot>,
}

impl Default for ComposerModel {
    fn default() -> Self {
        Self::new("")
    }
}

impl ComposerModel {
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            cursor: text.len(),
            text,
            selection_anchor: None,
            revision: 0,
            undo: Vec::new(),
            redo: Vec::new(),
        }
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    #[must_use]
    pub const fn cursor_byte(&self) -> usize {
        self.cursor
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    #[must_use]
    pub fn selection_range(&self) -> Option<Range<usize>> {
        self.selection_anchor.map(|anchor| {
            let start = anchor.min(self.cursor);
            let end = anchor.max(self.cursor);
            start..end
        })
    }

    #[must_use]
    pub fn has_selection(&self) -> bool {
        self.selection_range()
            .is_some_and(|range| !range.is_empty())
    }

    pub fn clear_selection(&mut self) {
        self.selection_anchor = None;
    }

    pub fn select_all(&mut self) {
        if self.text.is_empty() {
            self.clear_selection();
            return;
        }
        self.selection_anchor = Some(0);
        self.cursor = self.text.len();
    }

    /// Replaces the model from an external source such as command history.
    /// This is an editor-state transition, not a text edit, so it deliberately
    /// does not make an undo entry for a stale draft from another history item.
    pub fn set_text(&mut self, text: impl Into<String>) {
        self.text = text.into();
        self.cursor = self.text.len();
        self.selection_anchor = None;
        self.undo.clear();
        self.redo.clear();
        self.bump_revision();
    }

    pub fn set_cursor_byte(&mut self, byte: usize) {
        self.cursor = grapheme_boundary_at_or_before(&self.text, byte);
        self.selection_anchor = None;
    }

    pub fn set_cursor_byte_with_selection(&mut self, byte: usize, extend_selection: bool) {
        if extend_selection {
            self.selection_anchor.get_or_insert(self.cursor);
        } else {
            self.selection_anchor = None;
        }
        self.cursor = grapheme_boundary_at_or_before(&self.text, byte);
        if self.selection_anchor == Some(self.cursor) {
            self.selection_anchor = None;
        }
    }

    pub fn insert(&mut self, value: &str) {
        if value.is_empty() {
            return;
        }
        self.begin_edit();
        self.delete_selection_without_history();
        self.text.insert_str(self.cursor, value);
        self.cursor = self.cursor.saturating_add(value.len());
        self.selection_anchor = None;
        self.bump_revision();
    }

    /// Insert a complete paste/IME commit as one undoable transaction.
    pub fn insert_paste(&mut self, value: &str) {
        self.insert(value);
    }

    pub fn insert_newline(&mut self) {
        self.insert("\n");
    }

    pub fn backspace(&mut self) -> bool {
        if self.has_selection() {
            self.begin_edit();
            self.delete_selection_without_history();
            self.bump_revision();
            return true;
        }
        if self.cursor == 0 {
            return false;
        }
        self.begin_edit();
        let previous = grapheme_boundary_before(&self.text, self.cursor);
        self.text.replace_range(previous..self.cursor, "");
        self.cursor = previous;
        self.bump_revision();
        true
    }

    pub fn delete_forward(&mut self) -> bool {
        if self.has_selection() {
            self.begin_edit();
            self.delete_selection_without_history();
            self.bump_revision();
            return true;
        }
        if self.cursor >= self.text.len() {
            return false;
        }
        self.begin_edit();
        let next = grapheme_boundary_after(&self.text, self.cursor);
        self.text.replace_range(self.cursor..next, "");
        self.bump_revision();
        true
    }

    pub fn delete_word_backward(&mut self) -> bool {
        if self.has_selection() {
            return self.backspace();
        }
        if self.cursor == 0 {
            return false;
        }
        self.begin_edit();
        let mut start = self.cursor;
        while start > 0
            && grapheme_before(&self.text, start).is_some_and(|grapheme| grapheme.trim().is_empty())
        {
            start = grapheme_boundary_before(&self.text, start);
        }
        while start > 0 && grapheme_before(&self.text, start).is_some_and(|g| !g.trim().is_empty())
        {
            start = grapheme_boundary_before(&self.text, start);
        }
        self.text.replace_range(start..self.cursor, "");
        self.cursor = start;
        self.bump_revision();
        true
    }

    pub fn delete_to_line_start(&mut self) -> bool {
        if self.has_selection() {
            return self.backspace();
        }
        let start = line_start(&self.text, self.cursor);
        if start == self.cursor {
            return false;
        }
        self.begin_edit();
        self.text.replace_range(start..self.cursor, "");
        self.cursor = start;
        self.bump_revision();
        true
    }

    pub fn delete_to_line_end(&mut self) -> bool {
        if self.has_selection() {
            return self.delete_forward();
        }
        let end = line_end(&self.text, self.cursor);
        if end == self.cursor {
            return false;
        }
        self.begin_edit();
        self.text.replace_range(self.cursor..end, "");
        self.bump_revision();
        true
    }

    pub fn move_left(&mut self, extend_selection: bool) {
        let next = grapheme_boundary_before(&self.text, self.cursor);
        self.set_cursor_byte_with_selection(next, extend_selection);
    }

    pub fn move_right(&mut self, extend_selection: bool) {
        let next = grapheme_boundary_after(&self.text, self.cursor);
        self.set_cursor_byte_with_selection(next, extend_selection);
    }

    pub fn move_home(&mut self, extend_selection: bool) {
        self.set_cursor_byte_with_selection(line_start(&self.text, self.cursor), extend_selection);
    }

    pub fn move_end(&mut self, extend_selection: bool) {
        self.set_cursor_byte_with_selection(line_end(&self.text, self.cursor), extend_selection);
    }

    pub fn undo(&mut self) -> bool {
        let Some(snapshot) = self.undo.pop() else {
            return false;
        };
        self.redo.push(self.snapshot());
        self.restore(snapshot);
        true
    }

    pub fn redo(&mut self) -> bool {
        let Some(snapshot) = self.redo.pop() else {
            return false;
        };
        self.undo.push(self.snapshot());
        self.restore(snapshot);
        true
    }

    /// A submit snapshot keeps authored bytes—including spaces, CRLF and
    /// logical newlines—unchanged. Product validation may reject an all
    /// whitespace message, but it must not trim the emitted bytes.
    #[must_use]
    pub fn submit_snapshot(&self) -> Option<String> {
        (!self.text.trim().is_empty()).then(|| self.text.clone())
    }

    fn snapshot(&self) -> ComposerSnapshot {
        ComposerSnapshot {
            text: self.text.clone(),
            cursor: self.cursor,
            selection_anchor: self.selection_anchor,
        }
    }

    fn begin_edit(&mut self) {
        self.undo.push(self.snapshot());
        if self.undo.len() > MAX_EDIT_HISTORY {
            self.undo.remove(0);
        }
        self.redo.clear();
    }

    fn delete_selection_without_history(&mut self) {
        let Some(range) = self.selection_range() else {
            return;
        };
        self.text.replace_range(range.clone(), "");
        self.cursor = range.start;
        self.selection_anchor = None;
    }

    fn restore(&mut self, snapshot: ComposerSnapshot) {
        self.text = snapshot.text;
        self.cursor = grapheme_boundary_at_or_before(&self.text, snapshot.cursor);
        self.selection_anchor = snapshot
            .selection_anchor
            .map(|anchor| grapheme_boundary_at_or_before(&self.text, anchor));
        self.bump_revision();
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
}

#[must_use]
pub(crate) fn grapheme_boundary_at_or_before(text: &str, byte: usize) -> usize {
    UnicodeSegmentation::grapheme_indices(text, true)
        .map(|(index, _)| index)
        .chain(std::iter::once(text.len()))
        .take_while(|boundary| *boundary <= byte.min(text.len()))
        .last()
        .unwrap_or(0)
}

fn grapheme_boundary_before(text: &str, byte: usize) -> usize {
    UnicodeSegmentation::grapheme_indices(text, true)
        .map(|(index, _)| index)
        .take_while(|boundary| *boundary < byte)
        .last()
        .unwrap_or(0)
}

fn grapheme_boundary_after(text: &str, byte: usize) -> usize {
    UnicodeSegmentation::grapheme_indices(text, true)
        .map(|(index, grapheme)| index.saturating_add(grapheme.len()))
        .find(|boundary| *boundary > byte)
        .unwrap_or_else(|| text.len())
}

fn grapheme_before(text: &str, byte: usize) -> Option<&str> {
    let start = grapheme_boundary_before(text, byte);
    (start < byte).then(|| &text[start..byte])
}

fn line_start(text: &str, byte: usize) -> usize {
    text[..byte.min(text.len())]
        .rfind('\n')
        .map_or(0, |index| index.saturating_add(1))
}

fn line_end(text: &str, byte: usize) -> usize {
    text[byte.min(text.len())..]
        .find('\n')
        .map_or(text.len(), |index| byte.saturating_add(index))
}

#[cfg(test)]
mod tests {
    use super::ComposerModel;

    #[test]
    fn cursor_and_delete_never_split_combining_or_zwj_graphemes() {
        let mut model = ComposerModel::new("a e\u{301} 👨‍👩‍👧‍👦");
        assert!(model.backspace());
        assert_eq!(model.text(), "a e\u{301} ");
        assert!(model.backspace());
        assert_eq!(model.text(), "a e\u{301}");
        assert!(model.backspace());
        assert_eq!(model.text(), "a ");
    }

    #[test]
    fn submit_keeps_authored_bytes_but_rejects_all_whitespace() {
        let model = ComposerModel::new("  keep\r\n  every space  ");
        assert_eq!(
            model.submit_snapshot().as_deref(),
            Some("  keep\r\n  every space  ")
        );
        assert_eq!(ComposerModel::new(" \n\t ").submit_snapshot(), None);
    }

    #[test]
    fn selection_paste_and_undo_redo_are_one_grapheme_safe_transaction() {
        let mut model = ComposerModel::new("ab e\u{301} cd");
        model.set_cursor_byte(2);
        model.move_right(true);
        model.move_right(true);
        model.insert_paste("👨‍👩‍👧‍👦\r\n");
        assert_eq!(model.text(), "ab👨‍👩‍👧‍👦\r\n cd");
        assert!(model.undo());
        assert_eq!(model.text(), "ab e\u{301} cd");
        assert!(model.redo());
        assert_eq!(model.text(), "ab👨‍👩‍👧‍👦\r\n cd");
    }

    #[test]
    fn word_and_line_edits_preserve_grapheme_boundaries() {
        let mut model = ComposerModel::new("alpha e\u{301} beta\nnext");
        model.set_cursor_byte(1);
        assert!(model.delete_to_line_end());
        assert_eq!(model.text(), "a\nnext");
        model.set_cursor_byte(model.text().len());
        assert!(model.delete_word_backward());
        assert_eq!(model.text(), "a\n");
    }
}
