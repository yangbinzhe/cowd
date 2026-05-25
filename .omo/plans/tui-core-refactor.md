# Cowd TUI 核心重构方案 — 借鉴 ratatui-kit

## 参考源码

`/datas/workspace/agents/ratatui-kit/packages/ratatui-kit/src/components/scroll_view/state.rs` (146行)

## TDD 执行计划

### Phase 1: 滚动系统重写 (借鉴 ScrollViewState)

- [ ] 1.1 新增 `ScrollState` 结构—统一键盘+鼠标事件处理

  **RED**: `test_scroll_state_up`, `test_scroll_state_page_down`, `test_scroll_state_mouse`, `test_scroll_state_handle_event`
  
  **GREEN**: 创建 `crates/cowd-cli/src/tui/scroll_state.rs`
  ```rust
  // 参考 ratatui-kit state.rs:16-146
  pub struct ScrollState {
      offset: u16,           // 当前滚动位置
      content_height: u16,   // 内容总高度 (渲染时设置)
      viewport_height: u16,  // 可视区域高度
  }
  impl ScrollState {
      // 统一事件处理: 键盘 + 鼠标
      pub fn handle_event(&mut self, event: &Event) -> bool;
      // 滚动操作
      pub fn scroll_up(&mut self);
      pub fn scroll_down(&mut self);
      pub fn scroll_page_up(&mut self);
      pub fn scroll_page_down(&mut self);
      pub fn scroll_to_top(&mut self);
      pub fn scroll_to_bottom(&mut self);
      // 渲染后钳制
      pub fn clamp_after_render(&mut self);
  }
  ```

- [ ] 1.2 替换 chat_view 中的分散滚动逻辑

  **RED**: `test_chat_view_uses_scroll_state`, `test_scroll_clamp_prevents_ghost`
  
  **GREEN**: chat_view 使用新 `ScrollState`，移除 `scroll_offset`/`auto_scroll`/`viewport_height` 分散字段

- [ ] 1.3 替换 state.rs 中的滚动同步

  **RED**: `test_scroll_sync_from_app`
  
  **GREEN**: `sync_from_app` 直接同步 `ScrollState`，移除手动计算

### Phase 2: 事件系统统一 (借鉴 handle_event 模式)

- [ ] 2.1 所有组件实现统一的 `handle_event(&Event) -> bool`

  **RED**: `test_unified_handle_event`
  
  **GREEN**: 扩展 `Component` trait — `handle_event` 接受 `crossterm::Event` (含键盘+鼠标)，返回 `bool`

### Phase 3: 渲染稳定性

- [ ] 3.1 渲染后钳制所有滚动状态

  **RED**: `test_scroll_clamp_after_collapse`
  
  **GREEN**: 折叠条目后自动修正滚动位置

### Phase FINAL

- [ ] F1. 全量测试 — `cargo test -p cowd-cli -- tui` 全绿
- [ ] F2. 崩溃回归 — 确认 prompt.rs UTF-8 和 main.rs Tokio 不再崩溃
