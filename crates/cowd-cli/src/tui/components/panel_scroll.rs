#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PanelScrollState {
    pub offset: usize,
    pub content_len: usize,
    pub viewport_len: usize,
}

impl Default for PanelScrollState {
    fn default() -> Self {
        Self {
            offset: 0,
            content_len: 0,
            viewport_len: 1,
        }
    }
}

impl PanelScrollState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sync(&mut self, content_len: usize, viewport_len: usize) {
        self.content_len = content_len;
        self.viewport_len = viewport_len.max(1);
        self.clamp();
    }

    pub fn max_offset(&self) -> usize {
        self.content_len.saturating_sub(self.viewport_len.max(1))
    }

    pub fn clamp(&mut self) {
        self.offset = self.offset.min(self.max_offset());
    }

    pub fn line_down(&mut self) {
        self.offset = (self.offset + 1).min(self.max_offset());
    }

    pub fn line_up(&mut self) {
        self.offset = self.offset.saturating_sub(1);
    }

    pub fn page_down(&mut self) {
        let amount = self.viewport_len.saturating_sub(1).max(1);
        self.offset = (self.offset + amount).min(self.max_offset());
    }

    pub fn page_up(&mut self) {
        let amount = self.viewport_len.saturating_sub(1).max(1);
        self.offset = self.offset.saturating_sub(amount);
    }

    pub fn top(&mut self) {
        self.offset = 0;
    }

    pub fn bottom(&mut self) {
        self.offset = self.max_offset();
    }

    pub fn ensure_visible(&mut self, index: usize) {
        if index < self.offset {
            self.offset = index;
        } else if index >= self.offset + self.viewport_len {
            self.offset = index.saturating_sub(self.viewport_len.saturating_sub(1));
        }
        self.clamp();
    }
}

pub fn clamp_u16_offset(offset: &mut u16, content_len: usize, viewport_len: usize) {
    let mut state = PanelScrollState {
        offset: *offset as usize,
        content_len,
        viewport_len: viewport_len.max(1),
    };
    state.clamp();
    *offset = offset_to_u16(state.offset);
}

pub fn offset_to_u16(offset: usize) -> u16 {
    offset.min(u16::MAX as usize) as u16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_to_content_bounds() {
        let mut state = PanelScrollState {
            offset: 50,
            content_len: 20,
            viewport_len: 5,
        };

        state.clamp();

        assert_eq!(state.offset, 15);
    }

    #[test]
    fn bottom_uses_last_full_viewport() {
        let mut state = PanelScrollState::new();
        state.sync(30, 10);

        state.bottom();

        assert_eq!(state.offset, 20);
    }

    #[test]
    fn ensure_visible_moves_minimally() {
        let mut state = PanelScrollState::new();
        state.sync(100, 10);
        state.offset = 20;

        state.ensure_visible(35);

        assert_eq!(state.offset, 26);
        state.ensure_visible(24);
        assert_eq!(state.offset, 24);
    }

    #[test]
    fn u16_offset_conversion_saturates() {
        assert_eq!(offset_to_u16(usize::MAX), u16::MAX);
    }
}
