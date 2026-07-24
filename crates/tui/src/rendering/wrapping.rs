use ratatui::{
    style::Style,
    text::{Line, Span},
};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

#[derive(Debug, Clone)]
struct StyledGrapheme {
    text: String,
    style: Style,
}

/// Wrap styled terminal lines by their rendered cell width.
///
/// Ratatui wraps after the caller has already selected a logical line slice.
/// That makes the scroll coordinate disagree with what the user can see and
/// can permanently hide the tail of a long line. This helper materializes
/// visual rows first, preserving span styles and treating CJK, emoji,
/// combining marks, whitespace, and unbroken URLs by terminal cell width.
#[must_use]
pub fn wrap_styled_lines(lines: Vec<Line<'static>>, width: usize) -> Vec<Line<'static>> {
    let width = width.max(1);
    lines
        .into_iter()
        .flat_map(|line| wrap_styled_line(line, width))
        .collect()
}

fn wrap_styled_line(line: Line<'static>, width: usize) -> Vec<Line<'static>> {
    let line_style = line.style;
    let alignment = line.alignment;
    let mut graphemes = Vec::new();
    for span in line.spans {
        for grapheme in span.content.graphemes(true) {
            graphemes.push(StyledGrapheme {
                text: grapheme.to_string(),
                style: span.style,
            });
        }
    }
    if graphemes.is_empty() {
        return vec![styled_line(Vec::new(), line_style, alignment)];
    }
    let continuation_indent = hanging_indent_cells(&graphemes, width);
    let indent_style = graphemes
        .first()
        .map_or(Style::default(), |item| item.style);

    let mut rows = Vec::new();
    let mut row = Vec::new();
    let mut row_width = 0usize;
    let mut cursor = 0usize;
    while cursor < graphemes.len() {
        let whitespace = graphemes[cursor].text.chars().all(char::is_whitespace);
        let start = cursor;
        while cursor < graphemes.len()
            && graphemes[cursor].text.chars().all(char::is_whitespace) == whitespace
        {
            cursor += 1;
        }
        let token = &graphemes[start..cursor];
        let token_width = token
            .iter()
            .map(|grapheme| UnicodeWidthStr::width(grapheme.text.as_str()))
            .sum::<usize>();

        if !whitespace && token_width <= width && row_width > 0 && row_width + token_width > width {
            rows.push(styled_line(coalesce(row), line_style, alignment));
            row = continuation_indent_graphemes(continuation_indent, indent_style);
            row_width = continuation_indent;
        }

        for grapheme in token {
            let grapheme_width = UnicodeWidthStr::width(grapheme.text.as_str());
            if row_width > 0 && row_width + grapheme_width > width {
                rows.push(styled_line(coalesce(row), line_style, alignment));
                row = continuation_indent_graphemes(continuation_indent, indent_style);
                row_width = continuation_indent;
            }
            row.push(grapheme.clone());
            row_width += grapheme_width;
        }
    }
    if !row.is_empty() {
        rows.push(styled_line(coalesce(row), line_style, alignment));
    }
    rows
}

fn continuation_indent_graphemes(width: usize, style: Style) -> Vec<StyledGrapheme> {
    (0..width)
        .map(|_| StyledGrapheme {
            text: " ".to_string(),
            style,
        })
        .collect()
}

fn hanging_indent_cells(graphemes: &[StyledGrapheme], width: usize) -> usize {
    if width <= 2 {
        return 0;
    }
    let prefix = graphemes
        .iter()
        .take(32)
        .map(|item| item.text.as_str())
        .collect::<String>();
    let leading_bytes = prefix.len().saturating_sub(prefix.trim_start().len());
    let rest = &prefix[leading_bytes..];
    let marker_bytes = if rest.starts_with("- ")
        || rest.starts_with("* ")
        || rest.starts_with("+ ")
        || rest.starts_with("> ")
    {
        2
    } else {
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits > 0
            && rest
                .get(digits..)
                .is_some_and(|tail| tail.starts_with(". ") || tail.starts_with(") "))
        {
            digits + 2
        } else {
            0
        }
    };
    if marker_bytes == 0 {
        return 0;
    }
    UnicodeWidthStr::width(&prefix[..leading_bytes.saturating_add(marker_bytes)])
        .min(width.saturating_sub(2))
}

fn coalesce(graphemes: Vec<StyledGrapheme>) -> Vec<Span<'static>> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    for grapheme in graphemes {
        if let Some(last) = spans.last_mut() {
            if last.style == grapheme.style {
                last.content.to_mut().push_str(&grapheme.text);
                continue;
            }
        }
        spans.push(Span::styled(grapheme.text, grapheme.style));
    }
    spans
}

fn styled_line(
    spans: Vec<Span<'static>>,
    style: Style,
    alignment: Option<ratatui::layout::Alignment>,
) -> Line<'static> {
    let mut line = Line::from(spans).style(style);
    if let Some(alignment) = alignment {
        line = line.alignment(alignment);
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::style::Color;

    fn text(lines: &[Line<'_>]) -> Vec<String> {
        lines
            .iter()
            .map(|line| {
                line.spans
                    .iter()
                    .map(|span| span.content.as_ref())
                    .collect()
            })
            .collect()
    }

    #[test]
    fn wraps_cjk_by_display_cells_without_losing_content() {
        let input = "中文内容不能丢失";
        let wrapped = wrap_styled_lines(vec![Line::raw(input.to_string())], 6);

        assert_eq!(text(&wrapped), vec!["中文内", "容不能", "丢失"]);
        assert_eq!(text(&wrapped).concat(), input);
    }

    #[test]
    fn hard_wraps_long_urls_and_preserves_styles() {
        let style = Style::default().fg(Color::Cyan);
        let input = Line::from(vec![
            Span::raw("go "),
            Span::styled("https://example.invalid/very/long/path", style),
        ]);
        let wrapped = wrap_styled_lines(vec![input], 10);

        assert_eq!(
            text(&wrapped).concat(),
            "go https://example.invalid/very/long/path"
        );
        assert!(wrapped
            .iter()
            .flat_map(|line| line.spans.iter())
            .any(|span| span.style == style));
    }

    #[test]
    fn keeps_emoji_and_combining_graphemes_intact() {
        let input = "A👨‍👩‍👧‍👦e\u{301}B";
        let wrapped = wrap_styled_lines(vec![Line::raw(input.to_string())], 2);

        assert_eq!(text(&wrapped).concat(), input);
    }

    #[test]
    fn markdown_lists_use_a_hanging_indent_for_continuation_rows() {
        let wrapped = wrap_styled_lines(
            vec![Line::raw(
                "- production delivery must stay readable".to_string(),
            )],
            16,
        );
        let rows = text(&wrapped);
        assert!(rows.len() > 1);
        assert!(rows.iter().skip(1).all(|row| row.starts_with("  ")));
        assert!(rows
            .iter()
            .all(|row| UnicodeWidthStr::width(row.as_str()) <= 16));
    }
}
