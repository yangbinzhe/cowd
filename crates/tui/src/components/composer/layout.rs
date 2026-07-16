//! Pure composer layout: visual wrapping never changes submitted text.

use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

use super::model::ComposerModel;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposerVisualRow {
    pub text: String,
    pub logical_line: usize,
    /// Absolute byte offsets into the canonical composer text.
    pub start_byte: usize,
    pub end_byte: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ComposerCursor {
    pub visual_row: usize,
    pub column: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposerViewport {
    pub start_row: usize,
    pub rows: Vec<ComposerVisualRow>,
    pub cursor: ComposerCursor,
}

/// Immutable visual representation of canonical composer bytes. All row
/// boundaries are Unicode grapheme boundaries. CRLF is rendered as a logical
/// line break while its original bytes remain untouched in the model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposerLayout {
    pub width: u16,
    pub rows: Vec<ComposerVisualRow>,
    pub cursor: ComposerCursor,
}

impl ComposerLayout {
    #[must_use]
    pub fn from_model(model: &ComposerModel, width: u16) -> Self {
        Self::from_text(model.text(), model.cursor_byte(), width)
    }

    #[must_use]
    pub fn from_text(text: &str, cursor_byte: usize, width: u16) -> Self {
        let width = width.max(1);
        let mut rows = Vec::new();
        let mut cursor_position = None;
        let mut line_start = 0usize;

        for (logical_line, line_with_break) in text.split_inclusive('\n').enumerate() {
            let line = line_with_break.strip_suffix('\n').unwrap_or(line_with_break);
            let display_line = line.strip_suffix('\r').unwrap_or(line);
            let first_row = rows.len();
            append_wrapped_rows(
                &mut rows,
                display_line,
                logical_line,
                line_start,
                usize::from(width),
            );
            let line_end = line_start.saturating_add(line.len());
            if cursor_byte >= line_start && cursor_byte <= line_end.saturating_add(1) {
                let display_cursor = cursor_byte
                    .saturating_sub(line_start)
                    .min(display_line.len());
                cursor_position = Some(cursor_for_line(
                    &rows[first_row..],
                    line_start.saturating_add(display_cursor),
                    first_row,
                    width,
                ));
            }
            line_start = line_start.saturating_add(line_with_break.len());
        }

        // `split_inclusive` has no item for an empty string nor for the empty
        // logical line after a trailing newline.
        if text.is_empty() || text.ends_with('\n') {
            let logical_line = text.bytes().filter(|byte| *byte == b'\n').count();
            let first_row = rows.len();
            append_wrapped_rows(&mut rows, "", logical_line, line_start, usize::from(width));
            if cursor_position.is_none() {
                cursor_position = Some(cursor_for_line(
                    &rows[first_row..],
                    cursor_byte.min(text.len()),
                    first_row,
                    width,
                ));
            }
        }

        Self {
            width,
            rows,
            cursor: cursor_position.unwrap_or(ComposerCursor {
                visual_row: 0,
                column: 0,
            }),
        }
    }

    #[must_use]
    pub fn desired_content_height(&self, max_content_height: u16) -> u16 {
        u16::try_from(self.rows.len())
            .unwrap_or(u16::MAX)
            .clamp(1, max_content_height.max(1))
    }

    #[must_use]
    pub fn viewport(&self, content_height: u16) -> ComposerViewport {
        let visible = usize::from(content_height.max(1));
        let start_row = self
            .cursor
            .visual_row
            .saturating_add(1)
            .saturating_sub(visible)
            .min(self.rows.len().saturating_sub(1));
        ComposerViewport {
            start_row,
            rows: self
                .rows
                .iter()
                .skip(start_row)
                .take(visible)
                .cloned()
                .collect(),
            cursor: ComposerCursor {
                visual_row: self.cursor.visual_row.saturating_sub(start_row),
                column: self.cursor.column,
            },
        }
    }

    /// Maps a visual row/column back to an absolute canonical byte offset.
    /// Columns inside a wide grapheme snap to its leading boundary, so callers
    /// can move a cursor without splitting combining text.
    #[must_use]
    pub fn byte_offset_for_visual(&self, visual_row: usize, column: u16) -> Option<usize> {
        let row = self.rows.get(visual_row)?;
        let mut width = 0usize;
        let mut byte = row.start_byte;
        for (offset, grapheme) in row.text.grapheme_indices(true) {
            let next_width = width.saturating_add(UnicodeWidthStr::width(grapheme));
            if next_width > usize::from(column) {
                break;
            }
            width = next_width;
            byte = row
                .start_byte
                .saturating_add(offset)
                .saturating_add(grapheme.len());
        }
        Some(byte)
    }
}

fn append_wrapped_rows(
    rows: &mut Vec<ComposerVisualRow>,
    line: &str,
    logical_line: usize,
    line_start: usize,
    width: usize,
) {
    if line.is_empty() {
        rows.push(ComposerVisualRow {
            text: String::new(),
            logical_line,
            start_byte: line_start,
            end_byte: line_start,
        });
        return;
    }

    let mut start_byte = 0;
    let mut visual_width = 0usize;
    for (byte, grapheme) in line.grapheme_indices(true) {
        let grapheme_width = UnicodeWidthStr::width(grapheme);
        if byte > start_byte && visual_width.saturating_add(grapheme_width) > width {
            rows.push(ComposerVisualRow {
                text: line[start_byte..byte].to_string(),
                logical_line,
                start_byte: line_start.saturating_add(start_byte),
                end_byte: line_start.saturating_add(byte),
            });
            start_byte = byte;
            visual_width = 0;
        }
        visual_width = visual_width.saturating_add(grapheme_width);
    }
    rows.push(ComposerVisualRow {
        text: line[start_byte..].to_string(),
        logical_line,
        start_byte: line_start.saturating_add(start_byte),
        end_byte: line_start.saturating_add(line.len()),
    });
}

fn cursor_for_line(
    rows: &[ComposerVisualRow],
    cursor_byte: usize,
    first_row: usize,
    width: u16,
) -> ComposerCursor {
    let row_index = rows
        .iter()
        .position(|row| cursor_byte >= row.start_byte && cursor_byte <= row.end_byte)
        .unwrap_or_else(|| rows.len().saturating_sub(1));
    let row = &rows[row_index];
    let cursor_byte = cursor_byte.clamp(row.start_byte, row.end_byte);
    let local_cursor = cursor_byte.saturating_sub(row.start_byte);
    let local_cursor = grapheme_boundary_at_or_before(&row.text, local_cursor);
    let column = UnicodeWidthStr::width(&row.text[..local_cursor]);
    ComposerCursor {
        visual_row: first_row.saturating_add(row_index),
        column: u16::try_from(column).unwrap_or(u16::MAX).min(width),
    }
}

fn grapheme_boundary_at_or_before(text: &str, byte: usize) -> usize {
    UnicodeSegmentation::grapheme_indices(text, true)
        .map(|(index, _)| index)
        .chain(std::iter::once(text.len()))
        .take_while(|boundary| *boundary <= byte)
        .last()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cjk_and_emoji_wrap_at_grapheme_boundaries_without_changing_text() {
        let original = "中文🙂e\u{301}👨‍👩‍👧‍👦".to_string();
        let layout = ComposerLayout::from_text(&original, 0, 3);
        assert!(layout.rows.len() > 1);
        assert_eq!(layout.rows.concat_text(), original);
        assert!(layout
            .rows
            .iter()
            .all(|row| UnicodeWidthStr::width(row.text.as_str()) <= 3));
    }

    #[test]
    fn narrow_width_is_bounded_and_cursor_remains_visible() {
        let layout = ComposerLayout::from_text("abcdef", 6, 0);
        assert_eq!(layout.width, 1);
        let viewport = layout.viewport(1);
        assert_eq!(viewport.rows.len(), 1);
        assert_eq!(viewport.cursor.visual_row, 0);
    }

    #[test]
    fn resize_changes_only_visual_rows_not_canonical_bytes() {
        let text = "  keep  spaces\n第二行🙂".to_string();
        let narrow = ComposerLayout::from_text(&text, text.len(), 4);
        let wide = ComposerLayout::from_text(&text, text.len(), 40);
        assert_eq!(narrow.rows.concat_logical_text(), text);
        assert_eq!(wide.rows.concat_logical_text(), text);
    }

    #[test]
    fn cursor_inside_a_combining_sequence_snaps_to_a_grapheme_boundary() {
        let layout = ComposerLayout::from_text("e\u{301}x", 1, 8);
        assert_eq!(layout.cursor.column, 0);
    }

    #[test]
    fn crlf_and_trailing_empty_line_keep_canonical_offsets() {
        let text = "first\r\nsecond\n";
        let model = ComposerModel::new(text);
        let layout = ComposerLayout::from_model(&model, 5);
        assert_eq!(model.text(), text);
        assert_eq!(layout.rows.concat_logical_text(), "first\nsecond\n");
        assert_eq!(layout.rows.last().map(|row| row.text.as_str()), Some(""));
        assert_eq!(
            layout.byte_offset_for_visual(layout.rows.len().saturating_sub(1), 0),
            Some(text.len())
        );
    }

    trait RowText {
        fn concat_text(&self) -> String;
        fn concat_logical_text(&self) -> String;
    }

    impl RowText for [ComposerVisualRow] {
        fn concat_text(&self) -> String {
            self.iter().map(|row| row.text.as_str()).collect()
        }

        fn concat_logical_text(&self) -> String {
            let mut result = String::new();
            let mut previous_line = None;
            for row in self {
                if previous_line.is_some_and(|line| line != row.logical_line) {
                    result.push('\n');
                }
                result.push_str(&row.text);
                previous_line = Some(row.logical_line);
            }
            result
        }
    }

    #[test]
    fn fixtures_preserve_bytes_at_every_supported_width() {
        let fixtures = [
            "ASCII path/without/any/spaces/for/a/long/time",
            "中文 日本語 العربية",
            "🙂👨‍👩‍👧‍👦e\u{301}",
            "  leading  and  repeated spaces  ",
            "first\r\nsecond\n\nlast",
            "\tindent\tand tabs",
        ];
        for fixture in fixtures {
            for width in [0, 1, 2, 5, 10, 20, 40, 80] {
                let model = ComposerModel::new(fixture);
                let layout = ComposerLayout::from_model(&model, width);
                assert_eq!(model.text(), fixture);
                assert_eq!(
                    layout.rows.concat_logical_text(),
                    fixture.replace("\r\n", "\n")
                );
                assert!(!layout.rows.is_empty());
            }
        }
    }

    #[test]
    fn visual_to_logical_mapping_stays_on_grapheme_boundaries() {
        let text = "a e\u{301}🙂".to_string();
        let layout = ComposerLayout::from_text(&text, 0, 2);
        for row in 0..layout.rows.len() {
            for column in 0..=2 {
                let byte = layout.byte_offset_for_visual(row, column).expect("row exists");
                assert!(text.is_char_boundary(byte));
                assert!(UnicodeSegmentation::grapheme_indices(text.as_str(), true)
                    .map(|(index, _)| index)
                    .chain(std::iter::once(text.len()))
                    .any(|boundary| boundary == byte));
            }
        }
    }
}
