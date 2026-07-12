// ── Question Multi-Step Form Component ───────────────────────────────
// Complete Question prompt system, exact match to opencode question.tsx (511 lines).
// Full Tab navigation, 1-9 shortcuts, single/multi select, custom text input,
// Confirm review page.
//
// Architecture:
//   - Standalone component (NOT a DialogKind variant — avoids conflicts)
//   - Self-contained state: tab, answers, custom, selected, editing
//   - handle_key() returns bool (consumed) for integration with event loop
//   - render() draws full-screen overlay: tabs, question, options, footer
//   - take_answers() / is_rejected() / is_confirmed() for result extraction
// -----------------------------------------------------------------

#![allow(dead_code)]

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, Paragraph},
};

use crate::components::base::terminal_len;
use crate::components::RenderContext;

// ── Data Types ────────────────────────────────────────────────────────

/// A single option within a question.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuestionOption {
    pub label: String,
    pub description: String,
}

/// Definition of a single question in the form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuestionDef {
    pub header: String,
    pub question: String,
    pub options: Vec<QuestionOption>,
    pub multiple: bool,
    pub custom: bool,
}

// ── QuestionForm State ────────────────────────────────────────────────

/// Complete multi-step question form with keyboard navigation, option selection,
/// custom text input, and a Confirm review page.
///
/// # State
/// - `tab`: current question index (0-based). `questions.len()` = Confirm tab.
/// - `answers`: one `Vec<String>` per question (multi-select accumulates).
/// - `custom`: custom text input per question (at question index).
/// - `selected`: highlighted option index within current question.
/// - `editing`: whether custom text input is active.
/// - `edit_buffer`: current text in the custom input textarea.
/// - `rejected`: set to `true` when user presses Esc to reject.
/// - `confirmed`: set to `true` when user submits on Confirm tab.
///
/// # Key bindings
/// | Key(s) | Context | Action |
/// |--------|---------|--------|
/// | h / ← | tab bar | Previous question |
/// | l / → | tab bar | Next question |
/// | Tab | tab bar | Next question (Shift+Tab = prev) |
/// | j / ↓ | options | Next option |
/// | k / ↑ | options | Previous option |
/// | 1-9   | options | Pick option by index + submit |
/// | Enter | options | Select current option |
/// | Space | multi | Toggle option |
/// | Enter | confirm tab | Submit all answers |
/// | Esc   | any | Reject entire form |
/// | Enter | custom edit | Submit custom text |
/// | Esc   | custom edit | Cancel editing |
pub struct QuestionForm {
    questions: Vec<QuestionDef>,
    /// Current tab index: 0..questions.len() = question, questions.len() = Confirm.
    tab: usize,
    /// One entry per question: the list of selected/picked answers.
    answers: Vec<Vec<String>>,
    /// Custom input text per question.
    custom: Vec<String>,
    /// Currently highlighted option index (0-based, within current question options + custom).
    selected: usize,
    /// Whether the custom text input textarea is active.
    editing: bool,
    /// Text buffer for custom input editing.
    edit_buffer: String,
    /// Set when the user rejects the form (Esc).
    rejected: bool,
    /// Set when the user confirms the form (Enter on Confirm tab).
    confirmed: bool,
}

impl QuestionForm {
    // ── Construction ───────────────────────────────────────────────

    /// Create a new `QuestionForm` from a list of question definitions.
    ///
    /// Initialises answers and custom vectors to match the number of questions.
    ///
    /// # Arguments
    /// * `questions` - The question definitions. Must not be empty.
    ///
    /// # Panics
    /// Panics if `questions` is empty.
    #[must_use]
    pub fn new(questions: Vec<QuestionDef>) -> Self {
        assert!(
            !questions.is_empty(),
            "QuestionForm requires at least one question"
        );
        let count = questions.len();
        Self {
            questions,
            tab: 0,
            answers: vec![Vec::new(); count],
            custom: vec![String::new(); count],
            selected: 0,
            editing: false,
            edit_buffer: String::new(),
            rejected: false,
            confirmed: false,
        }
    }

    // ── Derived State Accessors ────────────────────────────────────

    /// Returns `true` if there is only one question and it is NOT multi-select.
    /// In single mode: pick immediately submits (no Confirm tab, no auto-advance).
    #[inline]
    fn is_single(&self) -> bool {
        self.questions.len() == 1 && !self.questions[0].multiple
    }

    /// Number of tabs: if single → 1; otherwise → questions.len() + 1 (Confirm).
    #[inline]
    fn tab_count(&self) -> usize {
        if self.is_single() {
            1
        } else {
            self.questions.len() + 1
        }
    }

    /// Returns `true` when the current tab is the Confirm review tab.
    #[inline]
    fn on_confirm(&self) -> bool {
        !self.is_single() && self.tab >= self.questions.len()
    }

    /// Reference to the current question, if not on Confirm tab.
    #[inline]
    fn current_question(&self) -> Option<&QuestionDef> {
        if self.on_confirm() {
            None
        } else {
            Some(&self.questions[self.tab])
        }
    }

    /// Options for the current question.
    fn current_options(&self) -> &[QuestionOption] {
        self.current_question().map_or(&[], |q| &q.options)
    }

    /// Whether the current question allows custom input.
    fn current_custom_enabled(&self) -> bool {
        self.current_question().map_or(false, |q| q.custom)
    }

    /// Whether the current question is multi-select.
    fn current_is_multi(&self) -> bool {
        self.current_question().map_or(false, |q| q.multiple)
    }

    /// Total number of selectable items (options + custom if enabled).
    fn total_options(&self) -> usize {
        let base = self.current_options().len();
        if self.current_custom_enabled() {
            base + 1
        } else {
            base
        }
    }

    /// Returns `true` if the custom option is currently selected.
    fn custom_selected(&self) -> bool {
        self.current_custom_enabled() && self.selected >= self.current_options().len()
    }

    /// Custom input text for the current question.
    fn current_custom_input(&self) -> &str {
        self.custom.get(self.tab).map_or("", |s| s.as_str())
    }

    /// Whether the custom input for the current question has been picked.
    fn custom_picked(&self) -> bool {
        let val = self.current_custom_input();
        if val.is_empty() {
            return false;
        }
        self.answers
            .get(self.tab)
            .map_or(false, |a| a.iter().any(|x| x == val))
    }

    // ── Public Result Access ───────────────────────────────────────

    /// Take the answers out of the form. Each inner `Vec<String>` contains the
    /// selected/picked answers for that question (empty if unanswered).
    ///
    /// This consumes the answers and leaves empty vectors behind.
    #[must_use]
    pub fn take_answers(&mut self) -> Vec<Vec<String>> {
        let mut result = Vec::with_capacity(self.questions.len());
        for a in &mut self.answers {
            result.push(std::mem::take(a));
        }
        result
    }

    /// Returns `true` if the user rejected (dismissed via Esc) the form.
    #[must_use]
    #[inline]
    pub fn is_rejected(&self) -> bool {
        self.rejected
    }

    /// Returns `true` if the user confirmed (submitted on Confirm tab) the form.
    #[must_use]
    #[inline]
    pub fn is_confirmed(&self) -> bool {
        self.confirmed
    }

    /// Returns `true` if the form is still active (not rejected nor confirmed).
    #[must_use]
    #[inline]
    pub fn is_active(&self) -> bool {
        !self.rejected && !self.confirmed
    }

    // ── State Mutation Helpers ─────────────────────────────────────

    /// Select an option label for the current question.
    /// In single mode, this submits immediately.
    /// In multi-question mode, this auto-advances to the next tab.
    fn pick(&mut self, answer: String, custom: bool) {
        // Always answer the question(s) before moving on
        if self.is_single() {
            self.answers[self.tab] = vec![answer.clone()];
            if custom {
                self.custom[self.tab] = answer;
            }
            self.confirmed = true;
            return;
        }
        self.answers[self.tab] = vec![answer.clone()];
        if custom {
            self.custom[self.tab] = answer;
        }
        self.tab += 1;
        self.selected = 0;
    }

    /// Toggle an option for the current question (multi-select).
    fn toggle(&mut self, answer: &str) {
        let existing = &mut self.answers[self.tab];
        if let Some(pos) = existing.iter().position(|a| a == answer) {
            existing.remove(pos);
        } else {
            existing.push(answer.to_string());
        }
    }

    /// Move selection to a specific option index.
    fn move_to(&mut self, index: usize) {
        self.selected = index;
    }

    /// Navigate to a specific tab.
    fn select_tab(&mut self, index: usize) {
        self.tab = index;
        self.selected = 0;
    }

    /// Execute the "select option" action:
    /// - If custom is selected: enter editing mode (or toggle if multi + already picked)
    /// - If multi-select: toggle
    /// - If single-select: pick
    fn select_option(&mut self) {
        if self.custom_selected() {
            if !self.current_is_multi() {
                // Enter editing mode for custom single-select
                let input = self.current_custom_input().to_string();
                self.edit_buffer = input;
                self.editing = true;
                return;
            }
            // Multi + custom selected
            let val = self.current_custom_input().to_string();
            if !val.is_empty() && self.custom_picked() {
                self.toggle(&val);
                return;
            }
            // Enter editing mode
            let input = self.current_custom_input().to_string();
            self.edit_buffer = input;
            self.editing = true;
            return;
        }
        // Regular option
        let label = {
            let opts = self.current_options();
            if self.selected >= opts.len() {
                return;
            }
            opts[self.selected].label.clone()
        };
        if self.current_is_multi() {
            self.toggle(&label);
        } else {
            self.pick(label, false);
        }
    }

    /// Submit custom text input: commit the edit buffer as the custom answer.
    fn submit_custom_edit(&mut self) {
        let text = self.edit_buffer.trim().to_string();
        let prev = self.custom.get(self.tab).cloned().unwrap_or_default();

        if text.is_empty() {
            // Clear any previous custom answer for this question
            if !prev.is_empty() {
                self.custom[self.tab] = String::new();
                // Remove prev from answers
                if let Some(answers) = self.answers.get_mut(self.tab) {
                    answers.retain(|x| x != &prev);
                }
            }
            self.editing = false;
            return;
        }

        if self.current_is_multi() {
            // Multi-select custom: replace old custom, update answers
            self.custom[self.tab] = text.clone();
            let answers = &mut self.answers[self.tab];
            // Remove old custom value
            if !prev.is_empty() {
                answers.retain(|x| x != &prev);
            }
            // Add new custom value
            if !answers.iter().any(|x| x == &text) {
                answers.push(text);
            }
            self.editing = false;
        } else {
            // Single-select custom: pick the text
            self.pick(text, true);
            self.editing = false;
        }
    }

    /// Reject the entire form.
    fn reject(&mut self) {
        self.rejected = true;
    }

    /// Submit all answers (called from Confirm tab).
    fn submit(&mut self) {
        self.confirmed = true;
    }

    // ── Key Handling ───────────────────────────────────────────────

    /// Process a key event. Returns `true` if the event was consumed.
    ///
    /// Key handling follows the opencode question.tsx keybinding model:
    /// - Editing mode (custom textarea): Enter submits, Esc cancels, Backspace/char edits.
    /// - Confirm tab: Enter submits, Esc rejects.
    /// - Question tab: j/k navigate, 1-9 pick, Enter select, Esc reject.
    /// - Tab bar: h/l, ←/→, Tab navigate between questions.
    pub fn handle_key(&mut self, event: &KeyEvent) -> bool {
        // ── Editing mode ──────────────────────────────────────────
        if self.editing && !self.on_confirm() {
            match event.code {
                KeyCode::Esc => {
                    self.editing = false;
                    self.edit_buffer.clear();
                    return true;
                }
                KeyCode::Enter => {
                    self.submit_custom_edit();
                    return true;
                }
                KeyCode::Char(c) => {
                    self.edit_buffer.push(c);
                    return true;
                }
                KeyCode::Backspace => {
                    self.edit_buffer.pop();
                    return true;
                }
                _ => return false,
            }
        }

        // ── Confirm tab mode ──────────────────────────────────────
        if self.on_confirm() {
            match event.code {
                KeyCode::Enter => {
                    self.submit();
                    return true;
                }
                KeyCode::Esc => {
                    self.reject();
                    return true;
                }
                // Tab navigation still works on Confirm tab
                KeyCode::Left | KeyCode::Char('h') => {
                    self.select_tab((self.tab + self.tab_count() - 1) % self.tab_count());
                    return true;
                }
                KeyCode::Right | KeyCode::Char('l') => {
                    self.select_tab((self.tab + 1) % self.tab_count());
                    return true;
                }
                KeyCode::Tab => {
                    let next = (self.tab + 1) % self.tab_count();
                    self.select_tab(next);
                    return true;
                }
                KeyCode::BackTab => {
                    let prev = (self.tab + self.tab_count() - 1) % self.tab_count();
                    self.select_tab(prev);
                    return true;
                }
                _ => return false,
            }
        }

        // ── Question tab mode ─────────────────────────────────────
        let total = self.total_options();

        match event.code {
            // ── Tab navigation ────────────────────────────────
            KeyCode::Left | KeyCode::Char('h') => {
                if !self.is_single() {
                    let prev_tab = (self.tab + self.tab_count() - 1) % self.tab_count();
                    self.select_tab(prev_tab);
                }
                return true;
            }
            KeyCode::Right | KeyCode::Char('l') => {
                if !self.is_single() {
                    let next_tab = (self.tab + 1) % self.tab_count();
                    self.select_tab(next_tab);
                }
                return true;
            }
            KeyCode::Tab => {
                if !self.is_single() {
                    let next = (self.tab + 1) % self.tab_count();
                    self.select_tab(next);
                }
                return true;
            }
            KeyCode::BackTab => {
                if !self.is_single() {
                    let prev = (self.tab + self.tab_count() - 1) % self.tab_count();
                    self.select_tab(prev);
                }
                return true;
            }

            // ── Option navigation ─────────────────────────────
            KeyCode::Up | KeyCode::Char('k') => {
                if total > 0 {
                    self.selected = (self.selected + total - 1) % total;
                }
                return true;
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if total > 0 {
                    self.selected = (self.selected + 1) % total;
                }
                return true;
            }

            // ── Number shortcuts: 1-9 → pick option by index ──
            KeyCode::Char(c) if c.is_ascii_digit() && c != '0' => {
                let idx = (c as usize) - ('1' as usize);
                if idx < total && total <= 9 {
                    self.move_to(idx);
                    self.select_option();
                }
                return true;
            }

            // ── Enter / Space: select / toggle ────────────────
            KeyCode::Enter => {
                self.select_option();
                return true;
            }
            KeyCode::Char(' ') => {
                // Space toggles in multi-select mode, otherwise same as Enter
                if self.current_is_multi() && !self.custom_selected() {
                    let label = {
                        let opts = self.current_options();
                        if self.selected < opts.len() {
                            opts[self.selected].label.clone()
                        } else {
                            String::new()
                        }
                    };
                    if !label.is_empty() {
                        self.toggle(&label);
                    }
                } else {
                    self.select_option();
                }
                return true;
            }

            // ── Esc: reject ───────────────────────────────────
            KeyCode::Esc => {
                self.reject();
                return true;
            }

            _ => false,
        }
    }

    // ── Rendering ──────────────────────────────────────────────────

    /// Render the full Question form as a full-screen overlay.
    ///
    /// Draws (in order):
    /// 1. Dimmed backdrop covering the full area
    /// 2. Centered dialog box with:
    ///    - Tab bar (question headers + Confirm)
    ///    - Question content (text, options, custom input) or Confirm review
    ///    - Footer key hints
    ///
    /// # Arguments
    /// * `ctx` — Render context providing frame and theme access.
    /// * `area` — The full screen area (used for backdrop + centering).
    pub fn render(&self, ctx: &mut RenderContext, area: Rect) {
        let accent = ctx.theme().accent_color();
        let fg = ctx.theme().fg_color();
        let frame = ctx.frame_mut();

        // 1. Backdrop: Clear + dimmed overlay
        frame.render_widget(Clear, area);
        let dim_bg = Style::default().bg(Color::Rgb(20, 20, 20));
        frame.render_widget(Paragraph::new("").style(dim_bg), area);

        // 2. Compute dialog size: 80% of screen width, adaptive height
        let max_w = ((area.width as f32) * 0.8) as u16;
        let w = max_w.min(80).max(40);
        let h = Self::compute_height(self, w).min(area.height);
        let x = area.x + (area.width.saturating_sub(w)) / 2;
        let y = area.y + (area.height.saturating_sub(h)) / 2;
        let dialog_rect = Rect::new(x, y, w, h);

        // Clear dialog area
        frame.render_widget(Clear, dialog_rect);

        // 3. Dialog border + title
        let block = Block::default()
            .borders(Borders::ALL)
            .title(" Question ")
            .fg(accent);
        let inner = block.inner(dialog_rect);
        frame.render_widget(block, dialog_rect);

        // 4. Render content inside dialog
        self.render_content(frame, inner, accent, fg);
    }

    /// Compute adaptive dialog height based on content.
    fn compute_height(&self, w: u16) -> u16 {
        // Base: border(2) + title line + footer line = 4
        let mut h: u16 = 4;

        // Tab bar (only for multi-question)
        if !self.is_single() {
            h += 1;
        }

        if self.on_confirm() {
            // Confirm tab: "Review" header + one line per question
            h = h
                .saturating_add(1)
                .saturating_add(terminal_len(self.questions.len()));
        } else {
            // Question text line + blank
            let q_text = self.current_question().map_or("", |q| q.question.as_str());
            let q_wrapped = Self::wrap_lines(q_text, w.saturating_sub(6));
            h += q_wrapped + 1;

            // Options: one per option + optional description line + custom option
            let opts = self.current_options();
            for opt in opts {
                h += 1; // option line
                if !opt.description.is_empty() {
                    let desc_wrapped = Self::wrap_lines(&opt.description, w.saturating_sub(10));
                    h += desc_wrapped;
                }
            }

            // Custom option line
            if self.current_custom_enabled() {
                h += 1;
                // Editing textarea area
                if self.editing {
                    // Show up to 3 lines of textarea
                    let buf_lines = terminal_len(self.edit_buffer.lines().count().max(1));
                    h += buf_lines.min(3);
                } else if !self.current_custom_input().is_empty() {
                    // Show current custom input preview
                    h += 1;
                }
            }
        }

        // Clamp between 8 and screen height * 0.9
        h.max(8)
    }

    /// Wrap text into lines for a given width. Returns number of lines.
    fn wrap_lines(text: &str, max_width: u16) -> u16 {
        if max_width < 1 {
            return 1;
        }
        let w = max_width as usize;
        let mut lines: u16 = 0;
        let mut current: usize = 0;
        for word in text.split_whitespace() {
            if current + word.len() > w && current > 0 {
                lines += 1;
                current = 0;
            }
            if current > 0 {
                current += 1; // space
            }
            current += word.len();
        }
        if current > 0 || lines == 0 {
            lines += 1;
        }
        lines
    }

    /// Render the inner content of the dialog.
    fn render_content(&self, frame: &mut ratatui::Frame, area: Rect, accent: Color, fg: Color) {
        let mut y_offset = area.y;

        // ── Tab bar ────────────────────────────────────────────
        if !self.is_single() {
            self.render_tab_bar(frame, area, &mut y_offset, accent, fg);
        }

        // ── Content area ───────────────────────────────────────
        if self.on_confirm() {
            self.render_confirm(frame, area, &mut y_offset, accent, fg);
        } else {
            self.render_question(frame, area, &mut y_offset, accent, fg);
        }

        // ── Footer ─────────────────────────────────────────────
        self.render_footer(frame, area, y_offset.max(area.y), accent, fg);
    }

    /// Render the tab bar: question headers + "Confirm" tab.
    fn render_tab_bar(
        &self,
        frame: &mut ratatui::Frame,
        area: Rect,
        y_offset: &mut u16,
        accent: Color,
        fg: Color,
    ) {
        let mut spans: Vec<Span> = Vec::new();

        // Question tabs
        for (i, q) in self.questions.iter().enumerate() {
            let is_active = i == self.tab;
            let is_answered = !self.answers.get(i).map_or(true, |a| a.is_empty());

            let style = if is_active {
                Style::default().fg(Color::Black).bg(accent)
            } else if is_answered {
                Style::default().fg(fg)
            } else {
                Style::default().fg(Color::DarkGray)
            };

            spans.push(Span::styled(format!(" {} ", q.header), style));
            spans.push(Span::raw(" "));
        }

        // Confirm tab
        let is_confirm = self.on_confirm();
        let confirm_style = if is_confirm {
            Style::default().fg(Color::Black).bg(accent)
        } else {
            Style::default().fg(Color::DarkGray)
        };
        spans.push(Span::styled(" Confirm ", confirm_style));

        let line = Line::from(spans);
        let text = Text::from(vec![line]);

        let rect = Rect::new(area.x + 2, *y_offset, area.width.saturating_sub(4), 1);
        frame.render_widget(Paragraph::new(text), rect);
        *y_offset += 1;
    }

    /// Render the current question content: question text, options, custom input.
    fn render_question(
        &self,
        frame: &mut ratatui::Frame,
        area: Rect,
        y_offset: &mut u16,
        accent: Color,
        fg: Color,
    ) {
        let q = match self.current_question() {
            Some(q) => q,
            None => return,
        };
        let is_multi = q.multiple;
        let opts = self.current_options();
        let inner_w = area.width.saturating_sub(6); // 2 borders + 4 padding
        let x = area.x + 2;

        // Question text
        let mut q_text = q.question.clone();
        if is_multi {
            q_text.push_str(" (select all that apply)");
        }
        let q_lines = Self::wrap_lines(&q_text, inner_w);
        let q_rect = Rect::new(x, *y_offset, inner_w, q_lines);
        frame.render_widget(
            Paragraph::new(Text::from(q_text.as_str())).style(Style::default().fg(fg)),
            q_rect,
        );
        *y_offset += q_lines;

        // Blank line
        *y_offset += 1;

        // Options list
        let total_opts = opts.len();
        let custom_enabled = q.custom;

        for (i, opt) in opts.iter().enumerate() {
            let is_selected = i == self.selected;
            let is_picked = self
                .answers
                .get(self.tab)
                .map_or(false, |a| a.iter().any(|x| x == &opt.label));

            // Build option line
            let mut option_spans: Vec<Span> = Vec::new();

            // Number
            let num_style = if is_selected {
                Style::default().fg(accent).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            option_spans.push(Span::styled(format!("{}. ", i + 1), num_style));

            // Label (with multi-select checkbox)
            if is_multi {
                let checkbox = if is_picked { "[✓]" } else { "[ ]" };
                let label_style = if is_picked {
                    Style::default().fg(Color::Green)
                } else if is_selected {
                    Style::default().fg(accent)
                } else {
                    Style::default().fg(fg)
                };
                option_spans.push(Span::styled(
                    format!("{} {}", checkbox, opt.label),
                    label_style,
                ));
            } else {
                let label_style = if is_picked {
                    Style::default().fg(Color::Green)
                } else if is_selected {
                    Style::default().fg(accent)
                } else {
                    Style::default().fg(fg)
                };
                option_spans.push(Span::styled(&opt.label, label_style));
                if is_picked {
                    option_spans.push(Span::styled(" ✓", Style::default().fg(Color::Green)));
                }
            }

            // Selection highlight background
            let option_line = Line::from(option_spans);
            let bg_style = if is_selected {
                Style::default().bg(Color::Rgb(40, 40, 40))
            } else {
                Style::default()
            };

            let opt_rect = Rect::new(x + 2, *y_offset, inner_w.saturating_sub(2), 1);
            frame.render_widget(
                Paragraph::new(Text::from(vec![option_line])).style(bg_style),
                opt_rect,
            );
            *y_offset += 1;

            // Description line
            if !opt.description.is_empty() {
                let desc_rect = Rect::new(x + 5, *y_offset, inner_w.saturating_sub(5), 1);
                frame.render_widget(
                    Paragraph::new(Text::from(Span::styled(
                        &opt.description,
                        Style::default().fg(Color::DarkGray),
                    ))),
                    desc_rect,
                );
                *y_offset += 1;
            }
        }

        // Custom "Type your own answer" option
        if custom_enabled {
            let custom_idx = total_opts;
            let is_selected = self.custom_selected();
            let is_picked = self.custom_picked();

            let mut spans: Vec<Span> = Vec::new();

            // Number
            let num_style = if is_selected {
                Style::default().fg(accent).add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(Color::DarkGray)
            };
            spans.push(Span::styled(format!("{}. ", custom_idx + 1), num_style));

            // Custom label
            let label = if is_multi {
                let checkbox = if is_picked { "[✓]" } else { "[ ]" };
                format!("{} Type your own answer", checkbox)
            } else {
                "Type your own answer".to_string()
            };

            let label_style = if is_picked {
                Style::default().fg(Color::Green)
            } else if is_selected {
                Style::default().fg(accent)
            } else {
                Style::default().fg(fg)
            };
            spans.push(Span::styled(&label, label_style));
            if !is_multi && is_picked {
                spans.push(Span::styled(" ✓", Style::default().fg(Color::Green)));
            }

            let bg_style = if is_selected {
                Style::default().bg(Color::Rgb(40, 40, 40))
            } else {
                Style::default()
            };

            let opt_rect = Rect::new(x + 2, *y_offset, inner_w.saturating_sub(2), 1);
            frame.render_widget(
                Paragraph::new(Text::from(vec![Line::from(spans)])).style(bg_style),
                opt_rect,
            );
            *y_offset += 1;

            // Custom input textarea / preview
            if self.editing {
                // Show editing textarea
                let edit_text = if self.edit_buffer.is_empty() {
                    format!(" Type your own answer...")
                } else {
                    format!(" {}▊", self.edit_buffer)
                };
                let edit_style = if self.edit_buffer.is_empty() {
                    Style::default().fg(Color::DarkGray)
                } else {
                    Style::default().fg(fg)
                };
                let edit_rect = Rect::new(x + 4, *y_offset, inner_w.saturating_sub(4), 1);
                frame.render_widget(
                    Paragraph::new(Text::from(Span::styled(edit_text, edit_style))),
                    edit_rect,
                );
                *y_offset += 1;
            } else if !self.current_custom_input().is_empty() {
                // Show current custom input as preview
                let preview = format!(" {}", self.current_custom_input());
                let preview_rect = Rect::new(x + 4, *y_offset, inner_w.saturating_sub(4), 1);
                frame.render_widget(
                    Paragraph::new(Text::from(Span::styled(
                        preview,
                        Style::default().fg(Color::DarkGray),
                    ))),
                    preview_rect,
                );
                *y_offset += 1;
            }
        }
    }

    /// Render the Confirm review page: list all questions with answers.
    fn render_confirm(
        &self,
        frame: &mut ratatui::Frame,
        area: Rect,
        y_offset: &mut u16,
        _accent: Color,
        fg: Color,
    ) {
        let x = area.x + 2;
        let inner_w = area.width.saturating_sub(6);

        // "Review" header
        let header_rect = Rect::new(x, *y_offset, inner_w, 1);
        frame.render_widget(
            Paragraph::new(Text::from(Span::styled(
                "Review",
                Style::default().fg(fg).add_modifier(Modifier::BOLD),
            ))),
            header_rect,
        );
        *y_offset += 1;

        // Each question with its answer
        for (i, q) in self.questions.iter().enumerate() {
            let answer = self.answers.get(i).cloned().unwrap_or_default();
            let answer_str = if answer.is_empty() {
                "(not answered)".to_string()
            } else {
                answer.join(", ")
            };
            let answered = !answer.is_empty();

            let mut spans: Vec<Span> = Vec::new();
            spans.push(Span::styled(
                format!("{}: ", q.header),
                Style::default().fg(Color::DarkGray),
            ));
            spans.push(Span::styled(
                &answer_str,
                if answered {
                    Style::default().fg(fg)
                } else {
                    Style::default().fg(Color::Red)
                },
            ));

            let line = Line::from(spans);
            let answer_rect = Rect::new(x + 2, *y_offset, inner_w.saturating_sub(2), 1);
            frame.render_widget(Paragraph::new(Text::from(vec![line])), answer_rect);
            *y_offset += 1;
        }
    }

    /// Render the footer with keybinding hints.
    fn render_footer(
        &self,
        frame: &mut ratatui::Frame,
        area: Rect,
        y: u16,
        accent: Color,
        _fg: Color,
    ) {
        let inner_w = area.width.saturating_sub(6);
        let x = area.x + 2;

        let enter_label = if self.on_confirm() {
            "submit"
        } else if self.current_is_multi() {
            "toggle"
        } else if self.is_single() {
            "submit"
        } else {
            "confirm"
        };

        let mut spans: Vec<Span> = Vec::new();

        if !self.is_single() {
            spans.push(Span::styled("←→ ", Style::default().fg(accent)));
            spans.push(Span::styled("tab  ", Style::default().fg(Color::DarkGray)));
        }

        if !self.on_confirm() {
            spans.push(Span::styled("↑↓ ", Style::default().fg(accent)));
            spans.push(Span::styled(
                "select  ",
                Style::default().fg(Color::DarkGray),
            ));
        }

        spans.push(Span::styled("enter ", Style::default().fg(accent)));
        spans.push(Span::styled(
            format!("{}  ", enter_label),
            Style::default().fg(Color::DarkGray),
        ));

        spans.push(Span::styled("esc ", Style::default().fg(accent)));
        spans.push(Span::styled(
            "dismiss",
            Style::default().fg(Color::DarkGray),
        ));

        let line = Line::from(spans);
        let footer_rect = Rect::new(x, y, inner_w, 1);
        frame.render_widget(Paragraph::new(Text::from(vec![line])), footer_rect);
    }
}

// ── Tests ────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn char_key(c: char) -> KeyEvent {
        KeyEvent::new(KeyCode::Char(c), KeyModifiers::NONE)
    }

    // ── Test helpers ──────────────────────────────────────────────

    fn make_question(
        header: &str,
        question: &str,
        options: &[(&str, &str)],
        multiple: bool,
        custom: bool,
    ) -> QuestionDef {
        QuestionDef {
            header: header.to_string(),
            question: question.to_string(),
            options: options
                .iter()
                .map(|(l, d)| QuestionOption {
                    label: l.to_string(),
                    description: d.to_string(),
                })
                .collect(),
            multiple,
            custom,
        }
    }

    fn make_single_q() -> QuestionForm {
        QuestionForm::new(vec![make_question(
            "Language",
            "What language?",
            &[
                ("Rust", "Fast and safe"),
                ("Python", "Simple"),
                ("Go", "Concurrent"),
            ],
            false,
            false,
        )])
    }

    fn make_multi_step_q() -> QuestionForm {
        QuestionForm::new(vec![
            make_question(
                "Language",
                "What language?",
                &[("Rust", ""), ("Python", ""), ("Go", "")],
                false,
                false,
            ),
            make_question(
                "Features",
                "Select features",
                &[("Toast", ""), ("Export", ""), ("Fork", "")],
                true,
                false,
            ),
        ])
    }

    // ── single_select_picks ───────────────────────────────────────

    #[test]
    fn single_select_picks_option() {
        let mut form = make_single_q();
        assert!(form.is_active());
        assert!(form.is_single());

        // Select first option with Enter
        form.move_to(0);
        form.handle_key(&key(KeyCode::Enter));

        assert!(form.is_confirmed());
        assert!(!form.is_rejected());

        let answers = form.take_answers();
        assert_eq!(answers.len(), 1);
        assert_eq!(answers[0], vec!["Rust"]);
    }

    #[test]
    fn single_select_picks_second_option_number_shortcut() {
        let mut form = make_single_q();

        // Press '2' to pick Python
        form.handle_key(&char_key('2'));

        assert!(form.is_confirmed());
        let answers = form.take_answers();
        assert_eq!(answers[0], vec!["Python"]);
    }

    #[test]
    fn single_select_reject() {
        let mut form = make_single_q();
        form.handle_key(&key(KeyCode::Esc));

        assert!(form.is_rejected());
        assert!(!form.is_confirmed());
    }

    // ── multi_toggles ─────────────────────────────────────────────

    #[test]
    fn multi_toggles_options() {
        let mut form = make_multi_step_q();
        // First question (single select, advance to Features)
        form.handle_key(&char_key('1')); // Pick Rust → auto-advance to Features (tab 1)

        assert_eq!(form.tab, 1);

        // Now on Features (multi-select)
        assert!(form.current_is_multi());

        // Toggle "Toast" with Enter
        form.move_to(0);
        form.handle_key(&key(KeyCode::Enter));

        // Toggle "Export" with Space
        form.move_to(1);
        form.handle_key(&char_key(' '));

        let answers = &form.answers[1];
        assert!(answers.contains(&"Toast".to_string()));
        assert!(answers.contains(&"Export".to_string()));
        assert!(!answers.contains(&"Fork".to_string()));

        // Toggle "Toast" off
        form.move_to(0);
        form.handle_key(&key(KeyCode::Enter));
        let answers = &form.answers[1];
        assert!(!answers.contains(&"Toast".to_string()));
        assert!(answers.contains(&"Export".to_string()));
    }

    #[test]
    fn multi_submit_via_confirm() {
        let mut form = make_multi_step_q();
        // Answer first question
        form.handle_key(&char_key('1')); // Pick Rust → advance to Features

        // Toggle some features
        form.move_to(0);
        form.handle_key(&key(KeyCode::Enter)); // Toast
        form.move_to(2);
        form.handle_key(&key(KeyCode::Enter)); // Fork

        // Navigate to Confirm tab
        form.select_tab(2); // questions.len() = 2, so Confirm is at index 2
        form.handle_key(&key(KeyCode::Enter)); // Submit

        assert!(form.is_confirmed());
        let answers = form.take_answers();
        assert_eq!(answers[0], vec!["Rust"]);
        assert!(answers[1].contains(&"Toast".to_string()));
        assert!(answers[1].contains(&"Fork".to_string()));
    }

    // ── tab_navigation ────────────────────────────────────────────

    #[test]
    fn tab_navigation_h_l() {
        let mut form = make_multi_step_q();
        assert_eq!(form.tab, 0);

        // l → next tab (Features = 1)
        form.handle_key(&char_key('l'));
        assert_eq!(form.tab, 1);

        // l → Confirm (2)
        form.handle_key(&char_key('l'));
        assert_eq!(form.tab, 2);

        // h → back to Features
        form.handle_key(&char_key('h'));
        assert_eq!(form.tab, 1);

        // h → back to Language
        form.handle_key(&char_key('h'));
        assert_eq!(form.tab, 0);
    }

    #[test]
    fn tab_navigation_left_right_arrows() {
        let mut form = make_multi_step_q();
        assert_eq!(form.tab, 0);

        form.handle_key(&key(KeyCode::Right));
        assert_eq!(form.tab, 1);

        form.handle_key(&key(KeyCode::Left));
        assert_eq!(form.tab, 0);
    }

    #[test]
    fn tab_navigation_tab_and_backtab() {
        let mut form = make_multi_step_q();

        form.handle_key(&key(KeyCode::Tab));
        assert_eq!(form.tab, 1);

        form.handle_key(&key(KeyCode::BackTab));
        assert_eq!(form.tab, 0);
    }

    #[test]
    fn tab_wraps_around() {
        let mut form = make_multi_step_q();

        // Go to Confirm (tab 2)
        form.handle_key(&char_key('l'));
        form.handle_key(&char_key('l'));
        assert_eq!(form.tab, 2);

        // Wrap from Confirm to Language (tab 0)
        form.handle_key(&char_key('l'));
        assert_eq!(form.tab, 0);

        // Wrap from Language to Confirm via h
        form.handle_key(&char_key('h'));
        assert_eq!(form.tab, 2);
    }

    // ── custom_input ──────────────────────────────────────────────

    #[test]
    fn custom_input_enters_editing_mode() {
        let mut form = QuestionForm::new(vec![make_question(
            "Tool",
            "Which tool?",
            &[("bash", "Shell commands")],
            false,
            true, // custom enabled
        )]);

        // Select the custom option (index 1 = options.len())
        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter));

        assert!(form.editing);
    }

    #[test]
    fn custom_input_submit_text() {
        let mut form = QuestionForm::new(vec![make_question(
            "Tool",
            "Which tool?",
            &[("bash", "Shell commands")],
            false,
            true,
        )]);

        // Select custom option and enter editing
        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter));
        assert!(form.editing);

        // Type some text
        form.handle_key(&char_key('v'));
        form.handle_key(&char_key('i'));
        form.handle_key(&char_key('m'));

        assert_eq!(form.edit_buffer, "vim");

        // Submit
        form.handle_key(&key(KeyCode::Enter));
        assert!(form.is_single()); // single question → confirmed immediately
        assert!(form.is_confirmed());

        let answers = form.take_answers();
        assert_eq!(answers[0], vec!["vim"]);
    }

    #[test]
    fn custom_input_escape_cancels() {
        let mut form = QuestionForm::new(vec![make_question(
            "Tool",
            "Which tool?",
            &[("bash", "")],
            false,
            true,
        )]);

        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter));
        assert!(form.editing);

        // Type something then cancel
        form.handle_key(&char_key('x'));
        form.handle_key(&key(KeyCode::Esc));

        assert!(!form.editing);
        assert!(form.edit_buffer.is_empty());
    }

    #[test]
    fn custom_input_multi_select_toggle() {
        let mut form = make_multi_step_q();

        // Answer first question
        form.handle_key(&char_key('1')); // Pick Rust → advance to Features

        // Features is multi-select with no custom. Let's test with a custom-enabled multi.
        // We'll create a new form specifically for this.

        let mut form2 = QuestionForm::new(vec![make_question(
            "Features",
            "Select features",
            &[("Toast", ""), ("Export", "")],
            true,
            true, // multi + custom
        )]);

        // Single question, multi → still shows confirm
        assert!(!form2.is_single()); // multi → not single

        // Select custom option (index 2)
        form2.move_to(2);
        form2.handle_key(&key(KeyCode::Enter)); // Enters editing mode (multi + custom, not yet picked)

        assert!(form2.editing);

        // Type custom text
        form2.handle_key(&char_key('l'));
        form2.handle_key(&char_key('i'));
        form2.handle_key(&char_key('n'));
        form2.handle_key(&char_key('t'));
        form2.handle_key(&key(KeyCode::Enter)); // Submit custom

        assert!(!form2.editing);
        // custom text should be in answers
        assert!(form2.answers[0].contains(&"lint".to_string()));

        // Selecting custom again should toggle it off
        form2.move_to(2);
        form2.handle_key(&key(KeyCode::Enter)); // custom picked → toggle off
        assert!(!form2.answers[0].contains(&"lint".to_string()));
    }

    // ── confirm_review ────────────────────────────────────────────

    #[test]
    fn confirm_review_shows_answers() {
        let mut form = make_multi_step_q();

        // Answer first question
        form.handle_key(&char_key('1')); // Pick Rust → advance

        // Answer second question (multi toggle)
        form.move_to(0);
        form.handle_key(&key(KeyCode::Enter)); // Toast

        // Go to Confirm
        form.select_tab(2);
        assert!(form.on_confirm());

        // Verify answers before submit
        assert_eq!(form.answers[0], vec!["Rust"]);
        assert!(form.answers[1].contains(&"Toast".to_string()));
    }

    #[test]
    fn confirm_not_answered_shows_empty() {
        let mut form = make_multi_step_q();

        // Skip first question, go directly to Confirm
        form.select_tab(2);
        assert!(form.on_confirm());

        // First question should have no answer
        assert!(form.answers[0].is_empty());
    }

    // ── number_shortcuts ──────────────────────────────────────────

    #[test]
    fn number_shortcuts_direct_pick() {
        let mut form = make_single_q();

        // Press '3' → pick "Go"
        form.handle_key(&char_key('3'));

        assert!(form.is_confirmed());
        let answers = form.take_answers();
        assert_eq!(answers[0], vec!["Go"]);
    }

    #[test]
    fn number_shortcuts_navigate_and_pick() {
        let mut form = make_multi_step_q();

        // Press '2' → pick Python, advance to Features
        form.handle_key(&char_key('2'));
        assert_eq!(form.tab, 1);
        assert_eq!(form.answers[0], vec!["Python"]);

        // Press '1' → toggle Toast (multi-select, toggles)
        form.handle_key(&char_key('1'));
        assert!(form.answers[1].contains(&"Toast".to_string()));
    }

    #[test]
    fn number_shortcut_out_of_range_ignored() {
        let mut form = make_single_q();

        // Only 3 options, press '9' → should do nothing to confirmed state
        form.handle_key(&char_key('9'));

        // Should still be active (no crash)
        assert!(form.is_active());
    }

    // ── reject ─────────────────────────────────────────────────────

    #[test]
    fn reject_from_question_tab() {
        let mut form = make_single_q();
        form.handle_key(&key(KeyCode::Esc));

        assert!(form.is_rejected());
        assert!(!form.is_confirmed());
    }

    #[test]
    fn reject_from_confirm_tab() {
        let mut form = make_multi_step_q();

        // Go to Confirm
        form.select_tab(2);

        // Reject
        form.handle_key(&key(KeyCode::Esc));
        assert!(form.is_rejected());
    }

    #[test]
    fn reject_from_editing_cancels_edit_not_form() {
        let mut form = QuestionForm::new(vec![make_question(
            "Tool",
            "Which tool?",
            &[("bash", "")],
            false,
            true,
        )]);

        // Enter editing
        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter));
        assert!(form.editing);

        // Esc in editing → cancel editing, not reject form
        form.handle_key(&key(KeyCode::Esc));
        assert!(!form.editing);
        assert!(!form.is_rejected());
        assert!(form.is_active());
    }

    // ── option_navigation ──────────────────────────────────────────

    #[test]
    fn j_k_navigate_options() {
        let mut form = make_single_q();
        assert_eq!(form.selected, 0);

        form.handle_key(&char_key('j'));
        assert_eq!(form.selected, 1);

        form.handle_key(&char_key('j'));
        assert_eq!(form.selected, 2);

        form.handle_key(&char_key('k'));
        assert_eq!(form.selected, 1);
    }

    #[test]
    fn j_k_wraps_around() {
        let mut form = make_single_q();
        assert_eq!(form.selected, 0);

        // k wraps to last
        form.handle_key(&char_key('k'));
        assert_eq!(form.selected, 2); // 3 options → last = 2

        // j wraps to first
        form.handle_key(&char_key('j'));
        assert_eq!(form.selected, 0);
    }

    // ── take_answers ───────────────────────────────────────────────

    #[test]
    fn take_answers_returns_all() {
        let mut form = make_multi_step_q();

        form.handle_key(&char_key('1')); // Pick Rust → advance
        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter)); // Toggle Export

        let answers = form.take_answers();
        assert_eq!(answers.len(), 2);
        assert_eq!(answers[0], vec!["Rust"]);
        assert!(answers[1].contains(&"Export".to_string()));
    }

    #[test]
    fn take_answers_leaves_empty() {
        let mut form = make_single_q();
        form.handle_key(&char_key('1'));
        let _ = form.take_answers();

        // Second take should return empty
        let answers2 = form.take_answers();
        assert_eq!(answers2[0], Vec::<String>::new());
    }

    // ── Render tests ──────────────────────────────────────────────

    fn test_terminal() -> ratatui::Terminal<ratatui::backend::TestBackend> {
        let backend = ratatui::backend::TestBackend::new(80, 24);
        ratatui::Terminal::new(backend).expect("TestBackend terminal creation never fails")
    }

    fn test_theme() -> crate::skin::SkinConfig {
        crate::skin::SkinConfig::default()
    }

    #[test]
    fn render_question_shows_options() {
        let mut terminal = test_terminal();
        let form = make_single_q();
        let theme = test_theme();

        terminal
            .draw(|f| {
                let area = f.area();
                let mut ctx = RenderContext::new(f, &theme);
                form.render(&mut ctx, area);
            })
            .expect("draw failed");

        let buffer = terminal.backend().buffer();
        let mut lines: Vec<String> = Vec::new();
        for y in 0..buffer.area().height {
            let mut line = String::new();
            for x in 0..buffer.area().width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }

        let joined = lines.join("\n");
        assert!(joined.contains("Rust"), "Should show Rust option: {joined}");
        assert!(
            joined.contains("Python"),
            "Should show Python option: {joined}"
        );
        assert!(joined.contains("Go"), "Should show Go option: {joined}");
        assert!(
            joined.contains("What language?"),
            "Should show question text: {joined}"
        );
        assert!(joined.contains("esc"), "Should show footer: {joined}");
    }

    #[test]
    fn render_multi_question_shows_tab_bar() {
        let mut terminal = test_terminal();
        let form = make_multi_step_q();
        let theme = test_theme();

        terminal
            .draw(|f| {
                let area = f.area();
                let mut ctx = RenderContext::new(f, &theme);
                form.render(&mut ctx, area);
            })
            .expect("draw failed");

        let buffer = terminal.backend().buffer();
        let mut lines: Vec<String> = Vec::new();
        for y in 0..buffer.area().height {
            let mut line = String::new();
            for x in 0..buffer.area().width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }

        let joined = lines.join("\n");
        assert!(
            joined.contains("Language"),
            "Should show Language tab: {joined}"
        );
        assert!(
            joined.contains("Features"),
            "Should show Features tab: {joined}"
        );
        assert!(
            joined.contains("Confirm"),
            "Should show Confirm tab: {joined}"
        );
        assert!(joined.contains("tab"), "Should show footer: {joined}");
    }

    #[test]
    fn render_confirm_shows_review() {
        let mut terminal = test_terminal();
        let mut form = make_multi_step_q();

        // Answer some questions
        form.handle_key(&char_key('1')); // Pick Rust → advance
        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter)); // Toggle Export
        form.select_tab(2); // Confirm tab

        let theme = test_theme();

        terminal
            .draw(|f| {
                let area = f.area();
                let mut ctx = RenderContext::new(f, &theme);
                form.render(&mut ctx, area);
            })
            .expect("draw failed");

        let buffer = terminal.backend().buffer();
        let mut lines: Vec<String> = Vec::new();
        for y in 0..buffer.area().height {
            let mut line = String::new();
            for x in 0..buffer.area().width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }

        let joined = lines.join("\n");
        assert!(
            joined.contains("Review"),
            "Should show Review header: {joined}"
        );
        assert!(joined.contains("Rust"), "Should show Rust answer: {joined}");
        assert!(
            joined.contains("Export"),
            "Should show Export answer: {joined}"
        );
    }

    #[test]
    fn render_confirm_not_answered_in_red() {
        let mut terminal = test_terminal();
        let mut form = make_multi_step_q();
        // Don't answer any question, just go to Confirm
        form.select_tab(2);

        let theme = test_theme();

        terminal
            .draw(|f| {
                let area = f.area();
                let mut ctx = RenderContext::new(f, &theme);
                form.render(&mut ctx, area);
            })
            .expect("draw failed");

        let buffer = terminal.backend().buffer();
        let mut lines: Vec<String> = Vec::new();
        for y in 0..buffer.area().height {
            let mut line = String::new();
            for x in 0..buffer.area().width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }

        let joined = lines.join("\n");
        assert!(
            joined.contains("(not answered)"),
            "Should show not answered: {joined}"
        );
    }

    #[test]
    fn render_custom_input_textarea() {
        let mut terminal = test_terminal();
        let mut form = QuestionForm::new(vec![make_question(
            "Tool",
            "Which tool?",
            &[("bash", "Shell")],
            false,
            true,
        )]);

        // Enter custom editing
        form.move_to(1);
        form.handle_key(&key(KeyCode::Enter));
        form.handle_key(&char_key('v'));
        form.handle_key(&char_key('i'));
        form.handle_key(&char_key('m'));

        let theme = test_theme();

        terminal
            .draw(|f| {
                let area = f.area();
                let mut ctx = RenderContext::new(f, &theme);
                form.render(&mut ctx, area);
            })
            .expect("draw failed");

        let buffer = terminal.backend().buffer();
        let mut lines: Vec<String> = Vec::new();
        for y in 0..buffer.area().height {
            let mut line = String::new();
            for x in 0..buffer.area().width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }

        let joined = lines.join("\n");
        assert!(
            joined.contains("Type your own answer"),
            "Should show custom option: {joined}"
        );
        assert!(joined.contains("vim"), "Should show typed text: {joined}");
    }

    #[test]
    fn render_multi_select_checkbox() {
        let mut terminal = test_terminal();
        let form = QuestionForm::new(vec![make_question(
            "Features",
            "Select features",
            &[("Toast", "")],
            true, // multi
            false,
        )]);

        let theme = test_theme();

        terminal
            .draw(|f| {
                let area = f.area();
                let mut ctx = RenderContext::new(f, &theme);
                form.render(&mut ctx, area);
            })
            .expect("draw failed");

        let buffer = terminal.backend().buffer();
        let mut lines: Vec<String> = Vec::new();
        for y in 0..buffer.area().height {
            let mut line = String::new();
            for x in 0..buffer.area().width {
                line.push_str(buffer[(x, y)].symbol());
            }
            lines.push(line.trim_end().to_string());
        }

        let joined = lines.join("\n");
        assert!(
            joined.contains("[ ]"),
            "Should show empty checkbox: {joined}"
        );
        assert!(
            joined.contains("select all that apply"),
            "Should show multi hint: {joined}"
        );
    }

    // ── is_active ──────────────────────────────────────────────────

    #[test]
    fn is_active_returns_false_after_confirm() {
        let mut form = make_single_q();
        form.handle_key(&char_key('1'));
        assert!(!form.is_active());
    }

    #[test]
    fn is_active_returns_false_after_reject() {
        let mut form = make_single_q();
        form.handle_key(&key(KeyCode::Esc));
        assert!(!form.is_active());
    }

    #[test]
    fn is_active_returns_true_while_editing() {
        let form = make_single_q();
        assert!(form.is_active());
    }
}
