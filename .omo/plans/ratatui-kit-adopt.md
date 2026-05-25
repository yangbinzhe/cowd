# Cowd TUI 借鉴 ratatui-kit — 可立即采用的模式

## 1. 统一事件处理 (handle_event)
ratatui-kit `ScrollViewState::handle_event(&Event)` 同时处理键盘和鼠标。

**cowd 当前**: 键盘在 `process_raw_key()`, 鼠标在 main.rs 手动处理 — 分散两处。

**采用方案**: 统一到 `ScrollState::handle_event(&Event) -> bool`

## 2. 渲染时设置尺寸 (size/page_size)
ratatui-kit 在渲染时设置 `size` 和 `page_size`，不提前计算。

**cowd 当前**: `total_lines()` 提前计算，与实际渲染可能不匹配。

**采用方案**: 渲染后回调 `set_content_size(actual_lines)`, 滚动边界基于实际值。

## 3. ComponentDrawer (渲染抽象)
ratatui-kit 的 `ComponentDrawer` 封装 frame + area + scroll_buffer。

**cowd 当前**: `RenderContext` 封装 frame + skin。

**采用方案**: 简化 `RenderContext`，减少每帧创建次数 (当前 ~15次)。

---

## 立即执行

所有改动在 cowd (master) worktree，零破坏现有功能。
