use pulldown_cmark::{CodeBlockKind, Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use ratatui::style::{Modifier, Style, Stylize};
use ratatui::text::{Line, Span};
#[cfg(feature = "code-highlight")]
use std::sync::LazyLock;
#[cfg(feature = "code-highlight")]
use syntect::easy::HighlightLines;
#[cfg(feature = "code-highlight")]
use syntect::highlighting::ThemeSet;
#[cfg(feature = "code-highlight")]
use syntect::parsing::SyntaxSet;
#[cfg(feature = "code-highlight")]
use syntect::util::LinesWithEndings;
use unicode_width::UnicodeWidthStr;

use crate::app::Theme;

#[cfg(feature = "code-highlight")]
pub(crate) static SYNTAX_SET: LazyLock<SyntaxSet> =
    LazyLock::new(SyntaxSet::load_defaults_newlines);
#[cfg(feature = "code-highlight")]
pub(crate) static THEME_SET: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

/// Render markdown text into ratatui Lines using theme-aware colors.
/// The `theme` provides all semantic colors (fg, muted, accent, inline-code, etc.)
/// so that every element follows the user-configured theme.
pub fn render_markdown_lines(text: &str, theme: &Theme) -> Vec<Line<'static>> {
    let mut renderer = Renderer::new(theme);
    renderer.render(text)
}

// ---------------------------------------------------------------------------
// Internal state for the event-driven renderer
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum ListKind {
    Unordered,
    Ordered { next: u64 },
}

struct Renderer<'a> {
    theme: &'a Theme,
    lines: Vec<Line<'static>>,
    current_spans: Vec<Span<'static>>,

    // Code blocks
    in_code_block: bool,
    code_language: Option<String>,
    code_content: String,

    // Tables
    in_table: bool,
    table_headers: Vec<String>,
    table_rows: Vec<Vec<String>>,
    table_row_cells: Vec<String>,
    table_cell: String,
    in_table_head: bool,

    // Blockquotes
    in_blockquote: usize,

    // Lists (stack for nesting)
    list_stack: Vec<ListKind>,

    // Links (depth for safety against malformed nesting)
    link_saved_len: usize,
    link_depth: usize,
}

impl<'a> Renderer<'a> {
    fn new(theme: &'a Theme) -> Self {
        Self {
            theme,
            lines: Vec::new(),
            current_spans: Vec::new(),
            in_code_block: false,
            code_language: None,
            code_content: String::new(),
            in_table: false,
            table_headers: Vec::new(),
            table_rows: Vec::new(),
            table_row_cells: Vec::new(),
            table_cell: String::new(),
            in_table_head: false,
            in_blockquote: 0,
            list_stack: Vec::new(),
            link_saved_len: 0,
            link_depth: 0,
        }
    }

    // ── Theme color helpers ─────────────────────────────────────────────

    fn fg_style(&self) -> Style {
        Style::default().fg(self.theme.fg())
    }
    fn muted_style(&self) -> Style {
        Style::default().fg(self.theme.muted_color())
    }
    fn accent_bold(&self) -> Style {
        Style::default()
            .fg(self.theme.accent())
            .add_modifier(Modifier::BOLD)
    }
    fn code_style(&self) -> Style {
        Style::default().fg(self.theme.inline_code_color())
    }
    fn success_style(&self) -> Style {
        Style::default().fg(self.theme.success_color())
    }

    /// Flush `current_spans` into `self.lines` as a single Line.
    /// When inside a blockquote, prepend a dimmed │ prefix.
    fn flush_paragraph(&mut self) {
        if self.current_spans.is_empty() {
            return;
        }
        let mut spans = std::mem::take(&mut self.current_spans);

        if self.in_blockquote > 0 {
            let mut prefixed = Vec::with_capacity(spans.len() + self.in_blockquote);
            for _ in 0..self.in_blockquote {
                prefixed.push(Span::styled("│ ", self.muted_style()));
            }
            // Dim the content inside the quote
            for span in &mut spans {
                span.style = span.style.dim();
            }
            prefixed.extend(spans);
            self.lines.push(Line::from(prefixed));
        } else {
            self.lines.push(Line::from(spans));
        }
    }

    fn render(&mut self, text: &str) -> Vec<Line<'static>> {
        let mut options = Options::empty();
        options.insert(Options::ENABLE_STRIKETHROUGH);
        options.insert(Options::ENABLE_TABLES);
        options.insert(Options::ENABLE_TASKLISTS);
        let parser = Parser::new_ext(text, options);

        for event in parser {
            match event {
                // ---- Code blocks ----
                Event::Start(Tag::CodeBlock(kind)) => {
                    self.flush_paragraph();
                    self.in_code_block = true;
                    self.code_language = match kind {
                        CodeBlockKind::Fenced(lang) if !lang.is_empty() => Some(lang.to_string()),
                        _ => None,
                    };
                }
                Event::End(TagEnd::CodeBlock) => {
                    self.in_code_block = false;
                    self.lines.extend(code_block_lines(
                        &self.code_content,
                        self.code_language.as_deref(),
                        self.theme,
                    ));
                    self.code_content.clear();
                    self.code_language = None;
                }
                Event::Text(t) if self.in_code_block => {
                    self.code_content.push_str(&t);
                }

                // ---- Tables ----
                Event::Start(Tag::Table(..)) => {
                    self.flush_paragraph();
                    self.in_table = true;
                    self.table_headers.clear();
                    self.table_rows.clear();
                    self.table_row_cells.clear();
                    self.table_cell.clear();
                    self.in_table_head = false;
                }
                Event::End(TagEnd::Table) => {
                    self.flush_paragraph();
                    self.render_table();
                    self.in_table = false;
                }
                Event::Start(Tag::TableHead) => {
                    self.in_table_head = true;
                    self.table_row_cells.clear();
                }
                Event::End(TagEnd::TableHead) => {
                    let row = std::mem::take(&mut self.table_row_cells);
                    if !row.is_empty() {
                        self.table_headers = row;
                    }
                    self.in_table_head = false;
                }
                Event::Start(Tag::TableRow) => {
                    self.table_row_cells.clear();
                }
                Event::End(TagEnd::TableRow) => {
                    let row = std::mem::take(&mut self.table_row_cells);
                    self.table_rows.push(row);
                }
                Event::Start(Tag::TableCell) => {
                    self.table_cell.clear();
                }
                Event::End(TagEnd::TableCell) => {
                    let cell = self.table_cell.trim().to_string();
                    self.table_row_cells.push(cell);
                    self.table_cell.clear();
                }
                Event::Text(t) if self.in_table => {
                    self.table_cell.push_str(&t);
                }
                Event::Code(t) if self.in_table => {
                    self.table_cell.push_str(&t);
                }

                // ---- Headings ----
                Event::Start(Tag::Heading { level, .. }) => {
                    self.flush_paragraph();
                    let prefix = match level {
                        HeadingLevel::H1 => "# ",
                        HeadingLevel::H2 => "## ",
                        HeadingLevel::H3 => "### ",
                        HeadingLevel::H4 => "#### ",
                        HeadingLevel::H5 => "##### ",
                        HeadingLevel::H6 => "###### ",
                    };
                    self.current_spans
                        .push(Span::styled(prefix.to_string(), self.accent_bold()));
                }
                Event::End(TagEnd::Heading(_)) => {
                    self.flush_paragraph();
                }

                // ---- Paragraphs ----
                Event::End(TagEnd::Paragraph) => {
                    self.flush_paragraph();
                }

                // ---- Soft / Hard breaks ----
                Event::SoftBreak | Event::HardBreak => {
                    self.flush_paragraph();
                }

                // ---- Horizontal rule ----
                Event::Rule => {
                    self.flush_paragraph();
                    self.lines
                        .push(Line::from(Span::styled("───", self.muted_style())));
                }

                // ---- Lists (ordered / unordered) ----
                Event::Start(Tag::List(start)) => {
                    let kind = match start {
                        Some(n) => ListKind::Ordered { next: n },
                        None => ListKind::Unordered,
                    };
                    self.list_stack.push(kind);
                }
                Event::End(TagEnd::List(..)) => {
                    self.list_stack.pop();
                }

                // ---- List items ----
                Event::Start(Tag::Item) => {
                    self.flush_paragraph();
                    let depth = self.list_stack.len().saturating_sub(1);
                    self.current_spans.push(Span::raw(" ".repeat(depth * 4)));

                    match self.list_stack.last_mut() {
                        Some(ListKind::Ordered { next }) => {
                            let n = *next;
                            *next += 1;
                            self.current_spans
                                .push(Span::styled(format!("{n}. "), self.muted_style()));
                        }
                        _ => {
                            self.current_spans
                                .push(Span::styled("• ".to_string(), self.muted_style()));
                        }
                    }
                }

                // ---- Task list markers ----
                Event::TaskListMarker(checked) => {
                    self.current_spans.pop();
                    if checked {
                        self.current_spans
                            .push(Span::styled("☑ ", self.success_style()));
                    } else {
                        self.current_spans
                            .push(Span::styled("☐ ", self.muted_style()));
                    }
                }

                // ---- Inline code ----
                Event::Code(code) => {
                    self.current_spans
                        .push(Span::styled(code.to_string(), self.code_style()));
                }

                // ---- Plain text ----
                Event::Text(text) => {
                    self.current_spans
                        .push(Span::styled(text.to_string(), self.fg_style()));
                }

                // ---- Links ----
                Event::Start(Tag::Link { .. }) => {
                    if self.link_depth == 0 {
                        self.link_saved_len = self.current_spans.len();
                    }
                    self.link_depth += 1;
                }
                Event::End(TagEnd::Link) => {
                    self.link_depth -= 1;
                    if self.link_depth == 0 {
                        for span in self.current_spans.iter_mut().skip(self.link_saved_len) {
                            span.style = span.style.clone().fg(self.theme.link_color());
                        }
                    }
                }

                // ---- Blockquotes ----
                Event::Start(Tag::BlockQuote(..)) => {
                    self.flush_paragraph();
                    self.in_blockquote += 1;
                }
                Event::End(TagEnd::BlockQuote(..)) => {
                    self.flush_paragraph();
                    self.in_blockquote = self.in_blockquote.saturating_sub(1);
                }

                // ---- Ignore everything else ----
                _ => {}
            }
        }

        self.flush_paragraph();
        std::mem::take(&mut self.lines)
    }

    // -----------------------------------------------------------------------
    // Table rendering helpers
    // -----------------------------------------------------------------------

    fn render_table(&mut self) {
        let mut all_rows: Vec<&[String]> = Vec::new();
        if !self.table_headers.is_empty() {
            all_rows.push(&self.table_headers);
        }
        for row in &self.table_rows {
            all_rows.push(row.as_slice());
        }
        if all_rows.is_empty() {
            return;
        }

        let col_count = all_rows.iter().map(|r| r.len()).max().unwrap_or(0);
        if col_count == 0 {
            return;
        }

        let col_widths: Vec<usize> = (0..col_count)
            .map(|c| {
                all_rows
                    .iter()
                    .filter_map(|row| row.get(c))
                    .map(|cell| UnicodeWidthStr::width(cell.as_str()))
                    .max()
                    .unwrap_or(0)
            })
            .collect();

        let thin = self.muted_style();

        // Build separator line: ├───┼───┤
        let separator: Vec<Span<'static>> = {
            let mut sep = Vec::new();
            sep.push(Span::styled("├", thin));
            for (i, w) in col_widths.iter().enumerate() {
                sep.push(Span::styled("─".repeat(w + 2), thin));
                if i + 1 < col_widths.len() {
                    sep.push(Span::styled("┼", thin));
                }
            }
            sep.push(Span::styled("┤", thin));
            sep
        };

        if !self.table_headers.is_empty() {
            self.lines
                .push(self.render_table_row(&self.table_headers, &col_widths, true));
            self.lines.push(Line::from(separator));
        }

        for row in &self.table_rows {
            self.lines
                .push(self.render_table_row(row, &col_widths, false));
        }
    }

    fn render_table_row(
        &self,
        cells: &[String],
        widths: &[usize],
        is_header: bool,
    ) -> Line<'static> {
        let thin = self.muted_style();
        let mut spans: Vec<Span<'static>> = Vec::new();
        spans.push(Span::styled("│ ", thin));

        for (i, w) in widths.iter().enumerate() {
            let cell_text = cells.get(i).map_or("", |s| s.as_str());
            if is_header {
                spans.push(Span::styled(
                    cell_text.to_string(),
                    self.fg_style().add_modifier(Modifier::BOLD),
                ));
            } else {
                spans.push(Span::styled(cell_text.to_string(), self.fg_style()));
            }
            let pad = w.saturating_sub(UnicodeWidthStr::width(cell_text));
            if pad > 0 {
                spans.push(Span::raw(" ".repeat(pad)));
            }
            if i + 1 < widths.len() {
                spans.push(Span::styled(" │ ", thin));
            } else {
                spans.push(Span::styled(" │", thin));
            }
        }

        Line::from(spans)
    }
}

// ---------------------------------------------------------------------------
// Syntax-highlighted code block rendering (theme-aware)
// ---------------------------------------------------------------------------

fn code_block_lines(content: &str, language: Option<&str>, theme: &Theme) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let bg = theme.code_bg_color();
    let muted = theme.muted_color();
    let fg = theme.fg();

    let lang_label = language.unwrap_or("text");
    lines.push(Line::from(vec![Span::styled(
        format!("   {} ", lang_label),
        Style::default().fg(muted).bg(bg),
    )]));

    #[cfg(feature = "code-highlight")]
    if let Some(syntax) = language.and_then(|l| SYNTAX_SET.find_syntax_by_token(l)) {
        let syntect_theme = &THEME_SET.themes["base16-ocean.dark"];
        let mut highlighter = HighlightLines::new(syntax, syntect_theme);
        for line in LinesWithEndings::from(content) {
            let highlighted = highlighter
                .highlight_line(line, &SYNTAX_SET)
                .unwrap_or_default();
            let spans: Vec<Span> = highlighted
                .into_iter()
                .map(|(style, text)| {
                    let sf = ratatui::style::Color::Rgb(
                        style.foreground.r,
                        style.foreground.g,
                        style.foreground.b,
                    );
                    Span::styled(text.to_string(), Style::default().fg(sf).bg(bg))
                })
                .collect();
            lines.push(Line::from(spans));
        }
    }

    #[cfg(not(feature = "code-highlight"))]
    let _ = language;

    #[cfg(not(feature = "code-highlight"))]
    {
        for line in content.lines() {
            let trimmed = line.trim_end();
            lines.push(Line::from(Span::styled(
                trimmed.to_string(),
                Style::default().fg(fg).bg(bg),
            )));
        }
    }

    #[cfg(feature = "code-highlight")]
    if language
        .and_then(|l| SYNTAX_SET.find_syntax_by_token(l))
        .is_none()
    {
        for line in content.lines() {
            let trimmed = line.trim_end();
            lines.push(Line::from(Span::styled(
                trimmed.to_string(),
                Style::default().fg(fg).bg(bg),
            )));
        }
    }

    lines.push(Line::raw(""));
    lines
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::Theme;

    fn test_theme() -> Theme {
        Theme::Dark
    }

    fn collect_text(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn test_task_list_unchecked_and_checked() {
        let md = "- [ ] todo\n- [x] done";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert!(!lines.is_empty(), "should produce lines");
        let first = collect_text(&lines[0]);
        assert!(
            first.contains('☐'),
            "unchecked item should contain ☐: {first:?}"
        );
        if lines.len() > 1 {
            let second = collect_text(&lines[1]);
            assert!(
                second.contains('☑'),
                "checked item should contain ☑: {second:?}"
            );
        }
    }

    #[test]
    fn test_blockquote_prefix_and_dimmed() {
        let md = "> quoted text";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert!(!lines.is_empty(), "should produce lines");
        let text = collect_text(&lines[0]);
        assert!(
            text.contains('│'),
            "blockquote should contain │ bar: {text:?}"
        );
    }

    #[test]
    fn test_link_renders_text_not_url() {
        let md = "[click here](https://example.com)";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert!(!lines.is_empty(), "should produce a line");
        let text = collect_text(&lines[0]);
        assert!(
            text.contains("click here"),
            "link should show text: {text:?}"
        );
        assert!(
            !text.contains("example.com"),
            "link should NOT show URL inline: {text:?}"
        );
    }

    #[test]
    fn test_nested_list_indentation() {
        let md = "- outer\n  - inner";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert_eq!(lines.len(), 2, "should have 2 lines");
        let inner = collect_text(&lines[1]);
        assert!(
            inner.starts_with("    "),
            "nested item should be indented 4+ spaces: {inner:?}"
        );
    }

    #[test]
    fn test_ordered_list() {
        let md = "1. first\n2. second";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert_eq!(lines.len(), 2, "should have 2 lines");
        let first = collect_text(&lines[0]);
        assert!(
            first.starts_with("1."),
            "first ordered item should start with '1.': {first:?}"
        );
        let second = collect_text(&lines[1]);
        assert!(
            second.starts_with("2."),
            "second ordered item should start with '2.': {second:?}"
        );
    }

    #[test]
    fn test_deeply_nested_lists() {
        let md = "- a\n  - b\n    - c";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert_eq!(lines.len(), 3, "should have 3 lines");
        assert!(
            collect_text(&lines[0]).starts_with('•'),
            "top-level should start with bullet: {:?}",
            collect_text(&lines[0])
        );
        assert!(
            collect_text(&lines[1]).starts_with("    "),
            "nested 1 level should be indented 4 spaces: {:?}",
            collect_text(&lines[1])
        );
        assert!(
            collect_text(&lines[2]).starts_with("        "),
            "nested 2 levels should be indented 8 spaces: {:?}",
            collect_text(&lines[2])
        );
    }

    #[test]
    fn test_table_only_headers() {
        let md = "| A | B |\n|---|---|";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert!(!lines.is_empty(), "should produce at least the header line");
        let header = collect_text(&lines[0]);
        assert!(
            header.starts_with('│'),
            "header line should start with │: {header:?}"
        );
    }

    #[test]
    fn test_table_with_header_and_rows() {
        let md = "| Name | Age |\n|------|-----|\n| Alice | 30 |\n| Bob | 25 |";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert!(lines.len() >= 3, "expected ≥3 lines, got {}", lines.len());
        assert!(
            collect_text(&lines[0]).starts_with('│'),
            "header line should start with │: {:?}",
            collect_text(&lines[0])
        );
        assert!(
            collect_text(&lines[1]).starts_with('├'),
            "separator should start with ├: {:?}",
            collect_text(&lines[1])
        );
        assert!(
            collect_text(&lines[1]).contains('┼'),
            "separator should contain ┼: {:?}",
            collect_text(&lines[1])
        );
        let row1 = collect_text(&lines[2]);
        assert!(row1.contains("Alice"), "row should contain Alice: {row1:?}");
    }

    #[test]
    fn test_all_features_together() {
        let md = r"> A blockquote with a [link](url)

| Col1 | Col2 |
|------|------|
| Data | More |

- [ ] task
- [x] done
1. ordered
2. list";
        let theme = test_theme();
        let lines = render_markdown_lines(md, &theme);
        assert!(!lines.is_empty(), "should produce lines");
        let texts: Vec<String> = lines.iter().map(collect_text).collect();
        assert!(
            texts.iter().any(|t| t.contains('│')),
            "blockquote should have │ bar"
        );
        assert!(
            texts.iter().any(|t| t.starts_with('│')),
            "table header should start with │"
        );
        assert!(
            texts.iter().any(|t| t.contains('☐') || t.contains('☑')),
            "task list should have checkboxes"
        );
        assert!(
            texts.iter().any(|t| t.starts_with("1.")),
            "ordered should start with 1."
        );
    }
}
