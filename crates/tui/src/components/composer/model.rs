//! Canonical, grapheme-safe composer text model.
//!
//! The rendering layer may split this text into any number of visual rows,
//! but never receives authority to rewrite it.  The current textarea adapter
//! can import/export this model while key handling is progressively migrated;
//! all layout and submit invariants are expressed against this type.

use unicode_segmentation::UnicodeSegmentation;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ComposerModel {
    text: String,
    /// Byte offset, always snapped to a Unicode grapheme boundary.
    cursor: usize,
    revision: u64,
}

impl ComposerModel {
    #[must_use]
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            cursor: text.len(),
            text,
            revision: 0,
        }
    }

    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    #[must_use]
    pub const fn cursor_byte(&self) -> usize {
        self.cursor
    }

    #[must_use]
    pub const fn revision(&self) -> u64 {
        self.revision
    }

    pub fn set_cursor_byte(&mut self, byte: usize) {
        self.cursor = grapheme_boundary_at_or_before(&self.text, byte);
    }

    pub fn insert(&mut self, value: &str) {
        self.text.insert_str(self.cursor, value);
        self.cursor += value.len();
        self.revision = self.revision.wrapping_add(1);
    }

    pub fn backspace(&mut self) -> bool {
        if self.cursor == 0 {
            return false;
        }
        let previous = grapheme_boundary_before(&self.text, self.cursor);
        self.text.replace_range(previous..self.cursor, "");
        self.cursor = previous;
        self.revision = self.revision.wrapping_add(1);
        true
    }

    /// A submit snapshot keeps authored bytes—including spaces, CRLF and
    /// logical newlines—unchanged.  Product validation may reject an all
    /// whitespace message, but it must not trim the emitted bytes.
    #[must_use]
    pub fn submit_snapshot(&self) -> Option<String> {
        (!self.text.trim().is_empty()).then(|| self.text.clone())
    }
}

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
}
