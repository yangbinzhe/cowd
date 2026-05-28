# 修复 7 个失败测试

## 问题分类

### 1. 逻辑错误 (2个)
- `load_session_reference_rejects_workspace_mismatch` — 应该拒绝但实际接受了
- `category_cycle` — 断言失败，期望 `None` 但得到 `Some(0)`

### 2. 测试隔离问题 (2个)
- `managed_sessions_default_to_jsonl_and_resolve_legacy_json` — PoisonError，mutex被其他测试污染
- `latest_session_alias_resolves_most_recent_managed_session` — 同上

### 3. 错误消息格式不匹配 (3个)
- `context_window_preflight_errors_render_recovery_steps`
- `provider_context_window_errors_are_reframed_with_same_guidance`
- `retry_wrapped_context_window_errors_keep_recovery_guidance`

---

## 修复方案

### 修复 1: `load_session_reference_rejects_workspace_mismatch`

**文件**: `crates/cowd-cli/src/main.rs`  
**函数**: `load_session_reference` (line 5291)

**问题**: 函数没有检查 workspace 是否匹配。测试创建了一个 session 在 workspace_a，然后切换到 workspace_b，期望函数拒绝加载。

**修复**: 在 `load_session_reference` 中添加 workspace 检查：

```rust
fn load_session_reference(
    reference: &str,
) -> Result<(SessionHandle, Session), Box<dyn std::error::Error>> {
    let handle = resolve_session_reference(reference)?;
    let session = Session::load_from_path(&handle.path)?;
    
    // Check workspace mismatch: if session has a workspace_root and it doesn't match current dir
    if let Some(ref session_workspace) = session.workspace_root {
        let current_dir = std::env::current_dir()?;
        if *session_workspace != current_dir {
            return Err(format!(
                "session workspace mismatch: session was created in '{}' but current workspace is '{}'",
                session_workspace.display(),
                current_dir.display()
            ).into());
        }
    }
    
    Ok((handle, session))
}
```

---

### 修复 2: `category_cycle`

**文件**: `crates/cowd-cli/src/tui/components/skills_panel.rs`  
**函数**: `category_cycle` test (line 760)

**问题**: 测试期望循环结束后 `active_category` 为 `None`，但实际得到 `Some(0)`。

**分析**: 需要查看 `next_category` 和 `prev_category` 的实现逻辑，确认循环边界条件是否正确。

**修复**: 检查 `next_category` 函数，确保当从最后一个 category 循环时，正确返回到 `None`。

---

### 修复 3-4: PoisonError 测试隔离问题

**文件**: `crates/cowd-cli/src/main.rs`  
**测试**: 
- `managed_sessions_default_to_jsonl_and_resolve_legacy_json` (line 10902)
- `latest_session_alias_resolves_most_recent_managed_session` (line 10937)

**问题**: 这两个测试使用 `cwd_lock()` mutex，但其他测试可能在 panic 时污染了 mutex。

**修复**: 
1. 使用 `lock().unwrap_or_else(|e| e.into_inner())` 替代 `lock().expect()`
2. 或者使用 `std::sync::Mutex::new()` 创建独立的 mutex 实例

**代码示例**:
```rust
let _guard = cwd_lock().lock().unwrap_or_else(|e| {
    eprintln!("Warning: mutex poisoned, recovering");
    e.into_inner()
});
```

---

### 修复 5-7: 错误消息格式不匹配

**文件**: `crates/cowd-cli/src/main.rs`  
**测试**:
- `context_window_preflight_errors_render_recovery_steps` (line 8898)
- `provider_context_window_errors_are_reframed_with_same_guidance` (line 8945)
- `retry_wrapped_context_window_errors_keep_recovery_guidance` (line 8978)

**问题**: 测试期望特定的错误消息格式，但实际错误消息格式已改变。

**修复**: 
1. 查看测试期望的错误消息格式
2. 更新错误消息生成代码，使其匹配测试期望
3. 或者更新测试以匹配当前的错误消息格式

**建议**: 优先更新错误消息生成代码，因为测试代表了期望的行为。

---

## 执行顺序

1. **修复 1**: `load_session_reference` workspace 检查 (简单，独立)
2. **修复 2**: `category_cycle` 边界条件 (简单，独立)
3. **修复 3-4**: PoisonError 隔离 (简单，独立)
4. **修复 5-7**: 错误消息格式 (需要分析测试期望)

---

## 验证

修复完成后运行：
```bash
cargo test -p cowd-cli --lib
```

期望结果：所有测试通过，0 失败。
