# Cowd TUI 滚动修复方案 — 借鉴 ratatui-kit

## 根因分析

当前 cowd `chat_view.rs` 滚动计算链:
```
entry_line_counts → total_lines() → ScrollbarState::new(total_lines) → auto_scroll
```

问题:
1. `total_lines()` 使用预计算的 `entry_line_counts`，可能与实际渲染行数不匹配
2. `scroll_offset` 基于 `total_lines - viewport_h`，当 `total_lines` 偏大时产生虚位
3. 折叠/展开条目时，`entry_line_counts` 未及时更新

## ratatui-kit 的 ScrollViewState 设计 (参考源码)

```rust
// /datas/workspace/agents/ratatui-kit/packages/ratatui-kit/src/components/scroll_view/state.rs

pub struct ScrollViewState {
    offset: Position,      // 当前滚动偏移 (x, y)
    size: Option<Size>,    // 内容总尺寸 (在首次渲染时设置)
    page_size: Option<Size>, // 可视区域尺寸
}

// 滚动时基于 page_size，而非预计算值
pub fn scroll_page_down(&mut self) {
    let page_size = self.page_size.map_or(1, |size| size.height);
    self.offset.y = self.offset.y.saturating_add(page_size).saturating_sub(1);
}
```

## 修复计划

### FIX 1: 滚动边界使用实际渲染尺寸
- 在 `chat_view.render()` 后更新 `scroll_offset` 上限为 `max(0, actual_rendered_lines - viewport_h)`
- 不再依赖 `total_lines()` 作为滚动的绝对上限

### FIX 2: 自动回滚修正
- 当 `scroll_offset > max_scroll` 时自动修正（折叠条目后内容变少）

### FIX 3: 鼠标滚轮增量改为 page-based
- `ScrollDown` 使用 `viewport_height.saturating_sub(1)` 作为步长
- `ScrollUp` 同理

实施位置: `crates/cowd-cli/src/tui/components/chat_view.rs` 和 `src/tui/state.rs`
