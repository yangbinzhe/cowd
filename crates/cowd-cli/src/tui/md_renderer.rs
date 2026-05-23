use pulldown_cmark::{Event, Parser, Tag, TagEnd, CodeBlockKind, HeadingLevel, Options};
use ratatui::text::{Line, Span};
use ratatui::style::{Color, Style, Stylize};
use std::sync::LazyLock;
use syntect::easy::HighlightLines;
use syntect::highlighting::ThemeSet;
use syntect::parsing::SyntaxSet;
use syntect::util::LinesWithEndings;

static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);
static THEME_SET: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

pub fn render_markdown_lines(text: &str, base_color: Color) -> Vec<Line<'static>> {
    let mut options = Options::empty();
    options.insert(Options::ENABLE_STRIKETHROUGH);
    let parser = Parser::new_ext(text, options);
    let mut lines: Vec<Line<'static>> = Vec::new();
    let mut current_spans: Vec<Span<'static>> = Vec::new();
    let mut in_code_block = false;
    let mut code_language: Option<String> = None;
    let mut code_content = String::new();

    for event in parser {
        match event {
            Event::Start(Tag::CodeBlock(kind)) => {
                if !current_spans.is_empty() {
                    lines.push(Line::from(std::mem::take(&mut current_spans)));
                }
                in_code_block = true;
                code_language = match kind {
                    CodeBlockKind::Fenced(lang) if !lang.is_empty() => Some(lang.to_string()),
                    _ => None,
                };
            }
            Event::End(TagEnd::CodeBlock) => {
                in_code_block = false;
                lines.extend(code_block_lines(&code_content, code_language.as_deref()));
                code_content.clear();
                code_language = None;
            }
            Event::Text(text) if in_code_block => {
                code_content.push_str(&text);
            }
            Event::Start(Tag::Heading { level, .. }) => {
                if !current_spans.is_empty() {
                    lines.push(Line::from(std::mem::take(&mut current_spans)));
                }
                let heading_prefix = match level {
                    HeadingLevel::H1 => "# ",
                    HeadingLevel::H2 => "## ",
                    HeadingLevel::H3 => "### ",
                    HeadingLevel::H4 => "#### ",
                    HeadingLevel::H5 => "##### ",
                    HeadingLevel::H6 => "###### ",
                };
                current_spans.push(Span::styled(heading_prefix.to_string(), Style::default().fg(Color::Cyan).bold()));
            }
            Event::End(TagEnd::Heading(_level)) => {
                lines.push(Line::from(std::mem::take(&mut current_spans)));
            }
            Event::End(TagEnd::Paragraph) => {
                if !current_spans.is_empty() {
                    lines.push(Line::from(std::mem::take(&mut current_spans)));
                }
            }
            Event::Start(Tag::Item) => {
                if !current_spans.is_empty() {
                    lines.push(Line::from(std::mem::take(&mut current_spans)));
                }
                current_spans.push(Span::styled("  • ".to_string(), Style::default().fg(Color::DarkGray)));
            }
            Event::Code(code) => {
                current_spans.push(Span::styled(code.to_string(), Style::default().fg(Color::Yellow)));
            }
            Event::Text(text) => {
                current_spans.push(Span::styled(text.to_string(), Style::default().fg(base_color)));
            }
            Event::SoftBreak | Event::HardBreak => {
                lines.push(Line::from(std::mem::take(&mut current_spans)));
            }
            Event::Rule => {
                if !current_spans.is_empty() {
                    lines.push(Line::from(std::mem::take(&mut current_spans)));
                }
                lines.push(Line::from(Span::styled("───".to_string(), Style::default().fg(Color::DarkGray))));
            }
            _ => {}
        }
    }

    if !current_spans.is_empty() {
        lines.push(Line::from(current_spans));
    }

    lines
}

fn code_block_lines(content: &str, language: Option<&str>) -> Vec<Line<'static>> {
    let mut lines: Vec<Line<'static>> = Vec::new();
    let bg = Color::Rgb(40, 40, 40);

    let lang_label = language.unwrap_or("text");
    lines.push(Line::from(vec![
        Span::styled(format!("   {} ", lang_label), Style::default().fg(Color::DarkGray).bg(bg)),
    ]));

    if let Some(syntax) = language.and_then(|l| SYNTAX_SET.find_syntax_by_token(l)) {
        let theme = &THEME_SET.themes["base16-ocean.dark"];
        let mut highlighter = HighlightLines::new(syntax, theme);
        for line in LinesWithEndings::from(content) {
            let highlighted = highlighter.highlight_line(line, &SYNTAX_SET).unwrap_or_default();
            let spans: Vec<Span> = highlighted
                .into_iter()
                .map(|(style, text)| {
                    let fg = Color::Rgb(style.foreground.r, style.foreground.g, style.foreground.b);
                    Span::styled(text.to_string(), Style::default().fg(fg).bg(bg))
                })
                .collect();
            lines.push(Line::from(spans));
        }
    } else {
        for line in content.lines() {
            let trimmed = line.trim_end();
            lines.push(Line::from(Span::styled(trimmed.to_string(), Style::default().fg(Color::White).bg(bg))));
        }
    }

    lines.push(Line::raw(""));
    lines
}
