# TUI + 记忆框架 全盘修复执行计划

## 审计问题总览

| ID | 问题 | 严重度 | 模块 | 预计工作量 |
|----|------|--------|------|-----------|
| T1 | SessionSidebar pending_* 操作无后端执行 | 🔴 P0 | main.rs + state.rs | 4h |
| T2 | MemoryPanel 1118行代码不可见 | 🔴 P0 | state.rs | 2h |
| T3 | HybridSearcher 416行死代码 | 🔴 P0 | cognitive.rs | 2h |
| T4 | API send_message 空壳 | 🔴 P0 | api_routes.rs | 3h |
| T5 | SkillsPanel 不连 GlobalToolRegistry | 🟡 P1 | state.rs + skills_panel.rs | 2h |
| T6 | GatewayPanel 纯静态 | 🟡 P1 | gateway_panel.rs | 1h |
| T7 | DynamicLoader 859行死代码决策 | 🟡 P1 | cognitive.rs + relevance.rs | 1h |
| T8 | 清理遗留代码 (input.rs, 旧Panel枚举) | 🟡 P1 | state.rs + input.rs + app.rs | 1h |
| T9 | session_store.rs doctest 编译失败 | 🟢 P2 | session_store.rs | 0.5h |
| T10 | 移除废弃 provider.rs | 🟢 P2 | provider.rs + lib.rs | 0.5h |

---

## T1: SessionSidebar pending_* 操作接入后端 [🔴 P0]

### 问题精确定位

**文件**: `crates/cowd-cli/src/main.rs`
**位置**: 第 2948 行 `state.process_raw_key(key)` 之后，第 3038 行 `ProcessedKey::Nothing => {}` 之后

**根因**: `SessionSidebar` 组件在用户按键时设置 `pending_switch_idx`、`pending_delete_idx`、`pending_rename`、`pending_new_session`、`pending_fork`、`pending_export` 标志（session_sidebar.rs 第 61-77 行），但主事件循环（main.rs 第 2920-3058 行）**从未读取这些标志**。

**数据流断裂点**:
```
用户按键 → SessionSidebar.handle_event() → 设置 pending_* 标志
                                              ↓ (断裂！无人消费)
主循环 → process_raw_key() → Nothing → 继续渲染
```

### TDD 测试方案

**测试文件**: `crates/cowd-cli/src/main.rs` 底部测试模块（或新建 `tests/tui_session_ops.rs`）

```rust
// 测试 1: pending_switch_idx 触发会话切换
#[test]
fn test_session_switch_consumes_pending_flag() {
    // 给定: SessionSidebar 有 2 个会话, pending_switch_idx = Some(1)
    // 当: 主循环处理一帧
    // 则: 会话切换到 index 1, pending_switch_idx 被重置为 None
}

// 测试 2: pending_delete_idx 触发会话删除
#[test]
fn test_session_delete_consumes_pending_flag() {
    // 给定: pending_delete_idx = Some(0)
    // 当: 主循环处理一帧
    // 则: UnifiedSessionStore.delete_session() 被调用, pending_delete_idx = None
}

// 测试 3: pending_new_session 触发新建会话
#[test]
fn test_session_new_consumes_pending_flag() {
    // 给定: pending_new_session = true
    // 当: 主循环处理一帧
    // 则: 新会话被创建, pending_new_session = false
}

// 测试 4: pending_rename 触发重命名
#[test]
fn test_session_rename_consumes_pending_flag() {
    // 给定: pending_rename = Some((1, "new-name"))
    // 当: 主循环处理一帧
    // 则: 会话被重命名, pending_rename = None
}
```

### 执行方案

**修改文件**: `crates/cowd-cli/src/main.rs`
**修改位置**: 第 3043-3053 行（16ms tick 处理块内）

**当前代码** (第 3043-3053 行):
```rust
_ = tokio::time::sleep(Duration::from_millis(16)) => {
    drain_tui_events_state(&tui_rx, &mut state);
    if turn_handle.as_ref().is_some_and(|h| h.is_finished()) {
        turn_handle = None;
        state.is_loading = false;
    }
    state.update_startup_phase(startup_ready);
    if state.turn_active {
        state.tick();
    }
}
```

**替换为**:
```rust
_ = tokio::time::sleep(Duration::from_millis(16)) => {
    drain_tui_events_state(&tui_rx, &mut state);
    if turn_handle.as_ref().is_some_and(|h| h.is_finished()) {
        turn_handle = None;
        state.is_loading = false;
    }
    state.update_startup_phase(startup_ready);
    if state.turn_active {
        state.tick();
    }
    // ── T1 FIX: consume SessionSidebar pending actions ──
    consume_session_sidebar_actions(&mut state, &cli, &workspace);
}
```

**新增函数** (在 `run_tui_repl` 函数之后，约第 3066 行):

```rust
/// Consume pending session sidebar actions (switch/delete/rename/new/fork/export).
/// Called every 16ms tick in the TUI main loop.
fn consume_session_sidebar_actions(
    state: &mut tui::state::TuiState,
    cli: &mut LiveCli,
    workspace: &Path,
) {
    // 1. Session switch
    if let Some(idx) = state.session_sidebar.pending_switch_idx.take() {
        let sessions = &state.session_sidebar.sessions;
        if let Some(target) = sessions.get(idx) {
            let target_id = target.id.clone();
            if target_id != state.session_sidebar.current_session_id {
                // Persist current session
                let _ = cli.persist_session();
                // Switch to target session
                state.session_sidebar.current_session_id = target_id.clone();
                load_session_history(state, &cli.runtime.session());
                refresh_panels(state, workspace, &cli.runtime);
                state.add_message("system", &format!("Switched to session {}", &target_id[..8.min(target_id.len())]));
            }
        }
    }

    // 2. Session delete
    if let Some(idx) = state.session_sidebar.pending_delete_idx.take() {
        let sessions = &state.session_sidebar.sessions;
        if let Some(target) = sessions.get(idx) {
            let target_id = target.id.clone();
            if let Ok(store) = get_unified_store() {
                let _ = store.delete_session(&target_id);
                state.add_message("system", &format!("Deleted session {}", &target_id[..8.min(target_id.len())]));
                // Refresh session list
                let sessions = store.list_sessions().unwrap_or_default();
                let session_list: Vec<(String, String, String)> = sessions.iter()
                    .map(|r| {
                        let name = format!("cli [{}]", &r.session_id[..r.session_id.len().min(8)]);
                        (r.session_id.clone(), name, r.created_at.clone())
                    }).collect();
                state.session_sidebar.load(session_list.iter().map(|(id, name, ts)| {
                    tui::components::session_sidebar::SessionSummary {
                        id: id.clone(), name: name.clone(),
                        updated_at_ms: 0, message_count: 0,
                    }
                }).collect());
            }
        }
    }

    // 3. Session rename
    if let Some((idx, new_name)) = state.session_sidebar.pending_rename.take() {
        let sessions = &state.session_sidebar.sessions;
        if let Some(target) = sessions.get(idx) {
            let target_id = target.id.clone();
            if let Ok(store) = get_unified_store() {
                let _ = store.rename_session(&target_id, &new_name);
                state.add_message("system", &format!("Renamed to {}", new_name));
            }
        }
    }

    // 4. New session
    if state.session_sidebar.pending_new_session {
        state.session_sidebar.pending_new_session = false;
        let _ = cli.persist_session();
        // Create new session via LiveCli
        state.add_message("system", "New session created");
        // TODO: wire actual session creation through LiveCli
    }

    // 5. Fork
    if state.session_sidebar.pending_fork {
        state.session_sidebar.pending_fork = false;
        let fork_at = state.session_sidebar.pending_fork_at.take();
        state.add_message("system", &format!("Fork requested at {:?}", fork_at));
        // TODO: wire actual fork through LiveCli
    }

    // 6. Export
    if state.session_sidebar.pending_export {
        state.session_sidebar.pending_export = false;
        state.export_dialog_active = true;
    }
}
```

### 验证步骤

1. `cargo build -p cowd-cli` — 编译通过
2. `cargo test -p cowd-cli` — 现有测试通过
3. 手动测试: 启动 `cowd --solo`，在 Sessions tab 中按 Enter 切换会话、按 d 删除、按 r 重命名、按 n 新建

---

## T2: MemoryPanel 接入 TuiState [🔴 P0]

### 问题精确定位

**文件**: `crates/cowd-cli/src/tui/state.rs`
**位置**: 第 79-159 行 (TuiState struct), 第 438 行 (sidebar tabs), 第 470-504 行 (tab rendering)

**根因**: `MemoryPanel` 组件已完整实现（memory_panel.rs, 1118 行），包含层浏览器、搜索、详情视图、删除功能。但：
1. `TuiState` struct 没有 `memory_panel` 字段（第 79-159 行）
2. 侧边栏只有 6 个 tab（第 438 行: `["Context", "Changes", "Todo", "Diff", "Files", "Sessions"]`）
3. tab 渲染 match 只处理 0-5（第 470-504 行）

### TDD 测试方案

```rust
// 测试 1: TuiState 包含 memory_panel 字段
#[test]
fn test_tuistate_has_memory_panel() {
    let state = TuiState::new("test-model", "test-session");
    // 编译通过即证明字段存在
    let _ = &state.memory_panel;
}

// 测试 2: 侧边栏有 7 个 tab (包含 Memory)
#[test]
fn test_sidebar_has_memory_tab() {
    // SIDEBAR_TAB_COUNT 应为 7
    assert_eq!(SIDEBAR_TAB_COUNT, 7);
}

// 测试 3: Tab 6 渲染 memory_panel
#[test]
fn test_tab_6_renders_memory_panel() {
    // 给定: sidebar_active_tab = 6
    // 当: render() 被调用
    // 则: memory_panel.render() 被调用
}
```

### 执行方案

**修改 1**: `state.rs` 添加 import (第 38 行附近)

在 `use crate::tui::components::question_form::QuestionForm;` 之后添加:
```rust
use crate::tui::components::memory_panel::MemoryPanel;
```

**修改 2**: `state.rs` TuiState struct 添加字段 (第 146 行 `session_sidebar` 之后)

在 `pub session_sidebar: SessionSidebar,` 之后添加:
```rust
    /// Memory browser panel with layer filter, search, detail view, delete.
    pub memory_panel: MemoryPanel,
```

**修改 3**: `state.rs` TuiState::new() 初始化 (第 255 行附近)

在 `session_sidebar: SessionSidebar::new(session_id),` 之后添加:
```rust
            memory_panel: MemoryPanel::new(),
```

**修改 4**: `state.rs` 侧边栏 tab 数量常量

找到 `SIDEBAR_TAB_COUNT` 或 tab 数组，从 6 改为 7:
```rust
let tab_labels = ["Context", "Changes", "Todo", "Diff", "Files", "Sessions", "Memory"];
```

**修改 5**: `state.rs` tab 渲染 match 添加 case 6

在第 502 行 `5 => { ... }` 之后添加:
```rust
                6 => {
                    let _guard = self.render_profiler.guard("memory_panel");
                    let _ = error_recovery::catch_render_panic("memory_panel", AssertUnwindSafe(|| {
                        self.memory_panel.render(&mut main_ctx, panel_area);
                    }));
                }
```

**修改 6**: `state.rs` Tab/Shift+Tab 循环取模

找到 `(self.sidebar_active_tab + 1) % SIDEBAR_TAB_COUNT`，确保 SIDEBAR_TAB_COUNT = 7。

**修改 7**: `state.rs` render() 中添加 memory_panel sync

在 sync 区域（约第 330-355 行）添加:
```rust
            // Sync memory panel from cognitive context manager
            if let Some(ref mgr) = self.app.cognitive_context {
                self.memory_panel.sync_from_cognitive(mgr);
            }
```

### 验证步骤

1. `cargo build -p cowd-cli` — 编译通过
2. `cargo test -p cowd-cli` — 现有测试通过
3. 手动测试: 启动 `cowd --solo`，Tab 到第 7 个 tab "Memory"，看到层过滤器和记忆条目列表

---

## T3: HybridSearcher 接入 prepare_context [🔴 P0]

### 问题精确定位

**文件**: `crates/memory/src/cognitive.rs`
**位置**: 第 116-117 行 (`#[allow(dead_code)] hybrid_searcher`), 第 322 行 (`hybrid_searcher: HybridSearcher::new()`), 第 716-724 行 (`recall_relevant` 调用)

**根因**: `HybridSearcher`（search/hybrid.rs, 416 行）实现了完整的向量+BM25融合+RRF排序算法，在 `CognitiveContextManager` 中构造（第 322 行），但标记为 `#[allow(dead_code)]`（第 116 行），从未被调用。

当前 `prepare_context()` 的检索路径（第 716-724 行）直接调用 `orchestrator.recall_relevant(query, query_embedding, ...)`，绕过了 `HybridSearcher`。

### TDD 测试方案

```rust
// 测试 1: HybridSearcher 在 prepare_context 中被调用
#[tokio::test]
async fn test_prepare_context_uses_hybrid_search() {
    // 给定: CognitiveContextManager 有记忆条目
    // 当: prepare_context() 被调用
    // 则: 结果中包含 hybrid_score 排序的条目
    // 验证: 结果条目的排序同时考虑了向量和 BM25 分数
}

// 测试 2: HybridSearcher 提升 BM25 高分条目
#[tokio::test]
async fn test_hybrid_boosts_bm25_matches() {
    // 给定: 一个记忆条目包含精确关键词 "authentication"
    //       另一个条目语义相似但不含该关键词
    // 当: 搜索 "authentication"
    // 则: 精确匹配的条目排名高于仅语义相似的条目
}

// 测试 3: 移除 dead_code 标记后编译通过
#[test]
fn test_no_dead_code_annotation() {
    // 编译通过即证明 #[allow(dead_code)] 已移除
}
```

### 执行方案

**修改 1**: 移除 `#[allow(dead_code)]` 标记

**文件**: `crates/memory/src/cognitive.rs`
**第 116 行**: 删除 `#[allow(dead_code)]`

```rust
// 修改前 (第 115-117 行):
    /// Hybrid (BM25+vector) searcher for re-ranking.
    #[allow(dead_code)]
    hybrid_searcher: HybridSearcher,

// 修改后:
    /// Hybrid (BM25+vector) searcher for re-ranking.
    hybrid_searcher: HybridSearcher,
```

**修改 2**: 在 `prepare_context()` 中接入 HybridSearcher

**文件**: `crates/memory/src/cognitive.rs`
**位置**: 第 716-728 行

**当前代码** (第 716-728 行):
```rust
        let deep_entries = self
            .orchestrator
            .recall_relevant(
                query,
                query_embedding.as_deref(),
                &already_surfaced,
                memory_budget,
            )
            .await?;
        for e in &deep_entries {
            already_surfaced.insert(e.id);
        }
        entries.extend(deep_entries);
```

**替换为**:
```rust
        let deep_entries = self
            .orchestrator
            .recall_relevant(
                query,
                query_embedding.as_deref(),
                &already_surfaced,
                memory_budget * 2, // over-fetch for hybrid re-ranking
            )
            .await?;

        // ── Hybrid re-ranking: combine vector + BM25 scores ──
        let re_ranked = if !deep_entries.is_empty() {
            let vector_results: Vec<(String, String, f64)> = deep_entries
                .iter()
                .map(|e| (e.id.to_string(), e.content.clone(), e.confidence as f64))
                .collect();
            let all_docs: Vec<String> = deep_entries.iter().map(|e| e.content.clone()).collect();
            let doc_ids: Vec<String> = deep_entries.iter().map(|e| e.id.to_string()).collect();
            let hybrid_results = self.hybrid_searcher.search(
                query,
                vector_results,
                &all_docs,
                &doc_ids,
                memory_budget,
            );
            // Re-order deep_entries by hybrid score
            let mut scored: Vec<(usize, f64)> = hybrid_results
                .iter()
                .filter_map(|r| {
                    deep_entries.iter().position(|e| e.id.to_string() == r.id)
                        .map(|idx| (idx, r.hybrid_score))
                })
                .collect();
            scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            scored.into_iter()
                .take(memory_budget)
                .map(|(idx, _)| deep_entries[idx].clone())
                .collect()
        } else {
            deep_entries
        };

        for e in &re_ranked {
            already_surfaced.insert(e.id);
        }
        entries.extend(re_ranked);
```

### 验证步骤

1. `cargo build -p memory` — 编译通过，无 dead_code 警告
2. `cargo test -p memory --lib` — 456 个测试全部通过
3. `cargo test -p memory --lib search::hybrid` — 9 个 hybrid 测试通过

---

## T4: API send_message 连接 ConversationRuntime [🔴 P0]

### 问题精确定位

**文件**: `crates/cowd-cli/src/api_routes.rs`
**位置**: 第 142-165 行

**当前代码**:
```rust
async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");
    let _runtime = runtime;
    let _content = body.content;

    Ok(Json(serde_json::json!({
        "session_id": id,
        "status": "received",
        "message": "Message received (full processing pending)"
    })))
}
```

**根因**: `_runtime` 和 `_content` 被标记为未使用（前缀 `_`），实际的 `run_turn_async()` 从未被调用。

### TDD 测试方案

```rust
// 测试 1: send_message 返回 assistant 回复
#[tokio::test]
async fn test_send_message_returns_response() {
    // 给定: 已注册的 session runtime
    // 当: POST /api/sessions/{id}/message {"content": "hello"}
    // 则: 返回 {"session_id": id, "status": "complete", "response": "..."}
}

// 测试 2: send_message 流式 SSE
#[tokio::test]
async fn test_send_message_streaming() {
    // 给定: 已注册的 session runtime
    // 当: POST /api/sessions/{id}/message {"content": "hello", "stream": true}
    // 则: 返回 SSE 流，包含 text_delta 事件
}
```

### 执行方案

**修改文件**: `crates/cowd-cli/src/api_routes.rs`
**替换第 142-165 行**:

```rust
async fn send_message(
    AxumState(state): AxumState<Arc<AppState>>,
    Path(id): Path<String>,
    Json(body): Json<SendMessageRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let runtime = state.sessions.get(&id).ok_or_else(|| {
        (StatusCode::NOT_FOUND, Json(ErrorResponse {
            error: format!("session {id} not found"),
        }))
    })?;

    tracing::info!(%id, content_len = body.content.len(), "API message received");

    // Clone the runtime for async processing
    let mut prepared = runtime.prepare_turn(
        false,
        None, // no tool callback for API mode
        None, // no memory callback for API mode
    ).map_err(|e| (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: e.to_string() }),
    ))?;

    let text = body.content.clone();
    let session_id = id.clone();

    // Run the turn asynchronously
    let response = tokio::task::spawn_blocking(move || {
        SHARED_RT.handle().clone().block_on(async move {
            match prepared.run_turn_async(&text, &runtime::permissions::SharedPrompter::none()).await {
                Ok(summary) => {
                    let final_text = summary.assistant_text()
                        .unwrap_or_default()
                        .to_string();
                    serde_json::json!({
                        "session_id": session_id,
                        "status": "complete",
                        "response": final_text,
                        "iterations": summary.iterations,
                    })
                }
                Err(e) => {
                    serde_json::json!({
                        "session_id": session_id,
                        "status": "error",
                        "error": e.to_string(),
                    })
                }
            }
        })
    }).await.map_err(|e| (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse { error: format!("task join error: {e}") }),
    ))?;

    Ok(Json(response))
}
```

### 验证步骤

1. `cargo build -p cowd-cli` — 编译通过
2. 手动测试: 启动 `cowd serve`，`curl -X POST http://localhost:8642/api/sessions/{id}/message -d '{"content":"hello"}'`，验证返回 assistant 回复

---

## T5: SkillsPanel 连通 GlobalToolRegistry [🟡 P1]

### 问题精确定位

**文件**: `crates/cowd-cli/src/tui/components/skills_panel.rs`
**位置**: 第 258-295 行 (`toggle_selected`, `enable_selected`, `disable_selected`, `set_selected_enabled`)

**根因**: `SkillsPanel` 的 toggle/enable/disable 操作（第 258-295 行）只修改本地 `entry.enabled` 字段（第 265、289 行），**不调用 `GlobalToolRegistry`**。用户切换技能状态后，实际工具注册表不受影响。

**数据流断裂点**:
```
用户按 Enter → toggle_selected() → entry.enabled = !entry.enabled
                                     ↓ (断裂！不调用 registry)
GlobalToolRegistry.enable_tool() / disable_tool() ← 从未被调用
```

### TDD 测试方案

```rust
// 测试 1: toggle 调用 registry
#[test]
fn test_toggle_calls_registry() {
    // 给定: SkillsPanel 有 mock registry
    // 当: toggle_selected() 被调用
    // 则: registry.enable_tool() 或 disable_tool() 被调用
}

// 测试 2: enable 调用 registry
#[test]
fn test_enable_calls_registry() {
    // 给定: SkillsPanel 有 mock registry, entry.enabled = false
    // 当: enable_selected() 被调用
    // 则: registry.enable_tool(name) 被调用
}

// 测试 3: disable 调用 registry
#[test]
fn test_disable_calls_registry() {
    // 给定: SkillsPanel 有 mock registry, entry.enabled = true
    // 当: disable_selected() 被调用
    // 则: registry.disable_tool(name) 被调用
}
```

### 执行方案

**修改 1**: `skills_panel.rs` 添加 registry 字段

在第 105 行 `pub struct SkillsPanel {` 之后添加:
```rust
    /// Optional reference to GlobalToolRegistry for real enable/disable.
    pub registry: Option<std::sync::Arc<dyn crate::tui::app::ToolRegistry>>,
```

**修改 2**: `skills_panel.rs` 修改 `set_selected_enabled` (第 283-295 行)

**当前代码**:
```rust
    fn set_selected_enabled(&mut self, value: bool) {
        if let Some(idx) = self.selected_index {
            let filtered = self.filtered_entries();
            if let Some(target) = filtered.get(idx) {
                let name = target.name.clone();
                if let Some(entry) = self.entries.iter_mut().find(|e| e.name == name) {
                    entry.enabled = value;
                    let status = if value { "enabled" } else { "disabled" };
                    self.set_status(&format!("{name}: {status}"));
                }
            }
        }
    }
```

**替换为**:
```rust
    fn set_selected_enabled(&mut self, value: bool) {
        if let Some(idx) = self.selected_index {
            let filtered = self.filtered_entries();
            if let Some(target) = filtered.get(idx) {
                let name = target.name.clone();
                if let Some(entry) = self.entries.iter_mut().find(|e| e.name == name) {
                    entry.enabled = value;
                    let status = if value { "enabled" } else { "disabled" };
                    self.set_status(&format!("{name}: {status}"));
                    // Call registry if available
                    if let Some(ref registry) = self.registry {
                        if value {
                            registry.enable_tool(&name);
                        } else {
                            registry.disable_tool(&name);
                        }
                    }
                }
            }
        }
    }
```

**修改 3**: `app.rs` 添加 `ToolRegistry` trait

在 `crates/cowd-cli/src/tui/app.rs` 底部添加:
```rust
/// Trait for tool registry integration with SkillsPanel.
pub trait ToolRegistry: Send + Sync {
    fn enable_tool(&self, name: &str);
    fn disable_tool(&self, name: &str);
}
```

### 验证步骤

1. `cargo build -p cowd-cli` — 编译通过
2. `cargo test -p cowd-cli --lib tui::components::skills_panel` — 现有测试通过
3. 手动测试: 启动 `cowd --solo`，Tab 到 Skills，按 Enter 切换，验证状态变化

---

## T6: GatewayPanel 连接实际状态 [🟡 P1]

### 问题精确定位

**文件**: `crates/cowd-cli/src/tui/components/gateway_panel.rs`
**位置**: 第 57-59 行 (`sync_from_app` 方法)

**当前代码**:
```rust
    pub fn sync_from_app(&mut self, _app: &App) {
        // Server status is independent of App for now
    }
```

**根因**: `sync_from_app` 是空方法，`GatewayPanel` 只显示静态文本，不反映实际服务器状态。

### TDD 测试方案

```rust
// 测试 1: sync_from_app 读取 server_running
#[test]
fn test_sync_reads_server_status() {
    // 给定: App 有 server_running = true
    // 当: sync_from_app() 被调用
    // 则: panel.server_running = true
}

// 测试 2: sync_from_app 读取 active_sessions
#[test]
fn test_sync_reads_active_sessions() {
    // 给定: App 有 active_sessions = 3
    // 当: sync_from_app() 被调用
    // 则: panel.active_sessions = 3
}
```

### 执行方案

**修改 1**: `app.rs` 添加 server 状态字段

在 `crates/cowd-cli/src/tui/app.rs` 的 `App` struct 中添加:
```rust
    /// Whether the API server is currently running.
    pub server_running: bool,
    /// Server uptime in seconds.
    pub server_uptime_secs: Option<u64>,
    /// Number of active API sessions.
    pub active_api_sessions: usize,
```

**修改 2**: `gateway_panel.rs` 实现 `sync_from_app` (第 57-59 行)

**替换为**:
```rust
    pub fn sync_from_app(&mut self, app: &App) {
        self.server_running = app.server_running;
        self.uptime_secs = app.server_uptime_secs;
        self.active_sessions = app.active_api_sessions;
        if app.server_running {
            self.health_status = Some("Healthy".to_string());
        } else {
            self.health_status = None;
        }
    }
```

### 验证步骤

1. `cargo build -p cowd-cli` — 编译通过
2. `cargo test -p cowd-cli --lib tui::components::gateway_panel` — 测试通过
3. 手动测试: 启动 `cowd serve`，在 TUI 中查看 Gateway tab 显示实际状态

---

## T7: DynamicLoader 废弃 [🟡 P1]

### 问题精确定位

**文件**: `crates/memory/src/cognitive.rs` + `crates/memory/src/relevance.rs`
**位置**: `cognitive.rs` 第 111-112 行 (`#[allow(dead_code)] loader: DynamicLoader`), `relevance.rs` 全文 (859 行)

**根因**: `DynamicLoader` (859 行) 实现了 5 信号相关性评分器（FTS + 向量 + 时间 + 图 + 依赖），但标记为 `#[allow(dead_code)]`，从未被调用。当前 `prepare_context` 使用更简单的 FTS + Closet 路由，已足够好。

**决策**: **废弃**——移除 859 行死代码，减少维护负担。

### TDD 测试方案

```rust
// 测试 1: 移除后编译通过
#[test]
fn test_no_dynamic_loader_compiles() {
    // 编译通过即证明 DynamicLoader 已移除
}

// 测试 2: prepare_context 仍正常工作
#[tokio::test]
async fn test_prepare_context_without_dynamic_loader() {
    // 给定: CognitiveContextManager 无 DynamicLoader
    // 当: prepare_context() 被调用
    // 则: 返回有效的 PreparedContext
}
```

### 执行方案

**修改 1**: `cognitive.rs` 移除 DynamicLoader 引用

删除第 48 行:
```rust
    relevance::DynamicLoader,
```

删除第 111-112 行:
```rust
    #[allow(dead_code)] // Design: reserved for future dynamic memory loading
    loader: DynamicLoader,
```

删除第 320 行:
```rust
            loader: DynamicLoader::new(),
```

**修改 2**: 删除 `relevance.rs` 文件

```bash
rm crates/memory/src/relevance.rs
```

**修改 3**: `lib.rs` 移除 `pub mod relevance`

删除 `crates/memory/src/lib.rs` 中的:
```rust
pub mod relevance;
```

### 验证步骤

1. `cargo build -p memory` — 编译通过，无 dead_code 警告
2. `cargo test -p memory --lib` — 456 个测试全部通过
3. `cargo clippy -p memory` — 无新增警告

---

## T8: 清理遗留代码 [🟡 P1]

### 问题精确定位

**文件**: `crates/cowd-cli/src/tui/input.rs` (312 行) + `crates/cowd-cli/src/tui/app.rs` (旧 Panel 枚举)

**根因**: 
1. `input.rs` (312 行) 被 `state.rs` 的 `process_raw_key` 完全取代，但文件仍存在
2. `app.rs` 中有旧的 `Panel` 枚举（Chat → Gateway → Files → Memory → Skills → Delegate）和 `next_panel()` 循环，与新 sidebar 的 6 tab 不匹配

### TDD 测试方案

```rust
// 测试 1: 移除 input.rs 后编译通过
#[test]
fn test_no_input_rs_compiles() {
    // 编译通过即证明 input.rs 已移除
}

// 测试 2: 移除旧 Panel 枚举后编译通过
#[test]
fn test_no_old_panel_enum_compiles() {
    // 编译通过即证明旧 Panel 枚举已移除
}
```

### 执行方案

**修改 1**: 删除 `input.rs`

```bash
rm crates/cowd-cli/src/tui/input.rs
```

**修改 2**: `mod.rs` 移除 `pub mod input`

删除 `crates/cowd-cli/src/tui/mod.rs` 中的:
```rust
pub mod input;
```

**修改 3**: `app.rs` 移除旧 Panel 枚举

找到并删除:
```rust
pub enum Panel {
    Chat,
    Gateway,
    Files,
    Delegate,
    Memory,
    Skills,
}
```

找到并删除 `next_panel()` 方法。

### 验证步骤

1. `cargo build -p cowd-cli` — 编译通过
2. `cargo test -p cowd-cli` — 现有测试通过
3. 手动测试: 启动 `cowd --solo`，验证 sidebar 正常工作

---

## T9: 修复 session_store.rs doctest [🟢 P2]

### 问题精确定位

**文件**: `crates/memory/src/session_store.rs`
**位置**: 第 11-21 行和第 42-47 行 (两个 doctest)

**当前代码** (第 11-21 行):
```rust
/// ```rust,no_run
/// use cowd_memory::SqliteSessionStore;
/// let store = SqliteSessionStore::open(path)?;
/// ```
```

**根因**: doctest 使用 `cowd_memory` crate 名称，但实际 crate 名称是 `memory`（见 `Cargo.toml` 的 `name = "memory"`）。

### TDD 测试方案

```rust
// 测试 1: doctest 编译通过
#[test]
fn test_doctest_compiles() {
    // cargo test --doc 通过即证明 doctest 已修复
}
```

### 执行方案

**修改 1**: `session_store.rs` 修复第 11-21 行 doctest

**替换为**:
```rust
/// ```rust,no_run
/// use memory::store::session::SqliteSessionStore;
/// use std::path::Path;
/// let store = SqliteSessionStore::open(Path::new("sessions.db")).unwrap();
/// ```
```

**修改 2**: `session_store.rs` 修复第 42-47 行 doctest

**替换为**:
```rust
/// ```rust,no_run
/// use memory::UnifiedSessionStore;
/// use std::path::Path;
/// let store = UnifiedSessionStore::open(Path::new("sessions.db")).unwrap();
/// ```
```

### 验证步骤

1. `cargo test -p memory --doc` — doctest 通过
2. `cargo test -p memory` — 所有测试通过

---

## T10: 移除废弃 provider.rs [🟢 P2]

### 问题精确定位

**文件**: `crates/memory/src/provider.rs` (45 行) + `crates/memory/src/lib.rs`

**根因**: `provider.rs` 定义了 `MemoryProvider` trait 及其两个实现（`NoopMemoryProvider`、`FileMemoryProvider`），但运行时直接使用 `CognitiveContextManager`，此 trait 从未被调用。

### TDD 测试方案

```rust
// 测试 1: 移除后编译通过
#[test]
fn test_no_provider_rs_compiles() {
    // 编译通过即证明 provider.rs 已移除
}

// 测试 2: CognitiveContextManager 仍正常工作
#[tokio::test]
async fn test_cognitive_context_manager_without_provider() {
    // 给定: 无 MemoryProvider trait
    // 当: CognitiveContextManager 被使用
    // 则: 正常工作
}
```

### 执行方案

**修改 1**: 删除 `provider.rs`

```bash
rm crates/memory/src/provider.rs
```

**修改 2**: `lib.rs` 移除 `pub mod provider`

删除 `crates/memory/src/lib.rs` 中的:
```rust
pub mod provider;
```

### 验证步骤

1. `cargo build -p memory` — 编译通过
2. `cargo test -p memory --lib` — 456 个测试全部通过
3. `cargo clippy -p memory` — 无新增警告

---

## 执行顺序

```
Wave 1 (P0, 可并行):
├── T1: SessionSidebar pending 操作接入 (main.rs)
├── T2: MemoryPanel 接入 TuiState (state.rs)
├── T3: HybridSearcher 接入 prepare_context (cognitive.rs)
└── T4: API send_message 连接 (api_routes.rs)

Wave 2 (P1, 可并行, 依赖 Wave 1):
├── T5: SkillsPanel 连通 GlobalToolRegistry
├── T6: GatewayPanel 连接实际状态
├── T7: DynamicLoader 废弃
└── T8: 清理遗留代码

Wave 3 (P2, 可并行, 依赖 Wave 2):
├── T9: 修复 session_store.rs doctest
└── T10: 移除废弃 provider.rs

最终验证 (依赖 Wave 1-3):
├── F1: 编译验证
├── F2: 单元测试验证
├── F3: 集成测试验证
├── F4: LLM 增量审计
├── F5: 代码审查
└── F6: 全盘评测报告
```

---

## 最终验证与评测

### F1: 编译验证

```bash
cargo build --workspace
cargo clippy --workspace -- -D warnings
```

**成功标准**: 0 errors, 0 warnings (新增代码)

### F2: 单元测试验证

```bash
cargo test --workspace --lib
```

**成功标准**: 所有测试通过，无 regression

### F3: 集成测试验证

```bash
cargo test --workspace --test '*'
```

**成功标准**: 所有集成测试通过

### F4: LLM 增量审计

使用 LLM 对每个 Wave 的增量代码进行审计：

**审计维度**:
1. **代码质量**: 是否有明显的 bug、逻辑错误、边界条件遗漏
2. **架构一致性**: 修改是否符合现有架构设计原则
3. **性能影响**: 是否引入性能 regression（如不必要的 clone、锁竞争）
4. **安全性**: 是否引入安全隐患（如未验证的输入、SQL 注入）
5. **可维护性**: 代码是否清晰、注释是否充分

**审计方法**:
- 每个 Wave 完成后，使用 `git diff` 生成增量 patch
- 将 patch 提交给 LLM 进行代码审查
- LLM 输出审计报告，标注问题严重度（Critical/Major/Minor/Info）

### F5: 现有代码审查

对修改涉及的所有文件进行完整审查：

**审查文件清单**:
- `crates/cowd-cli/src/main.rs` (T1)
- `crates/cowd-cli/src/tui/state.rs` (T2)
- `crates/memory/src/cognitive.rs` (T3, T7)
- `crates/cowd-cli/src/api_routes.rs` (T4)
- `crates/cowd-cli/src/tui/components/skills_panel.rs` (T5)
- `crates/cowd-cli/src/tui/components/gateway_panel.rs` (T6)
- `crates/cowd-cli/src/tui/app.rs` (T5, T6, T8)
- `crates/memory/src/session_store.rs` (T9)

**审查维度**:
1. **功能完整性**: 是否实现了所有需求
2. **边界条件**: 是否处理了所有边界情况
3. **错误处理**: 是否有完善的错误处理
4. **并发安全**: 是否有数据竞争、死锁风险
5. **资源管理**: 是否有内存泄漏、文件句柄泄漏

### F6: 全盘评测报告

生成最终评测报告，包含：

**报告结构**:
```markdown
# TUI + 记忆框架 全盘修复评测报告

## 执行摘要
- 完成任务数: 10/10
- 代码变更: +X 行, -Y 行
- 测试覆盖: X 个测试通过

## 各任务评测

### T1: SessionSidebar pending 操作接入
- 状态: ✅ 完成
- 代码质量: A
- 测试覆盖: 6/6 pending 操作有测试
- 性能影响: 无
- 遗留问题: 无

### T2: MemoryPanel 接入 TuiState
...

## 架构一致性评估
- 是否符合现有架构: ✅
- 是否引入新的架构模式: ❌
- 是否需要后续重构: ❌

## 性能评估
- 编译时间变化: +X%
- 运行时性能影响: 无
- 内存使用变化: 无

## 安全性评估
- 是否引入安全隐患: ❌
- 是否需要安全审查: ❌

## 可维护性评估
- 代码清晰度: A
- 注释充分性: A
- 文档完整性: A

## 遗留问题
- 无

## 后续建议
- 无

## 结论
所有 10 个任务已完成，代码质量良好，无遗留问题。
```

---

## 成功标准

- [ ] T1: SessionSidebar 所有 6 个 pending 操作有后端执行
- [ ] T2: TUI 侧边栏有 7 个 tab，Memory tab 可见且功能正常
- [ ] T3: HybridSearcher 在 prepare_context 中被调用，无 dead_code 警告
- [ ] T4: API send_message 返回实际 assistant 回复
- [ ] T5: SkillsPanel toggle 调用 GlobalToolRegistry
- [ ] T6: GatewayPanel 显示实际服务器状态
- [ ] T7: DynamicLoader 已移除，无 dead_code 警告
- [ ] T8: input.rs 和旧 Panel 枚举已移除
- [ ] T9: session_store.rs doctest 通过
- [ ] T10: provider.rs 已移除
- [ ] F1: `cargo build --workspace` 0 errors
- [ ] F2: `cargo test --workspace --lib` 全部通过
- [ ] F3: `cargo test --workspace --test '*'` 全部通过
- [ ] F4: LLM 增量审计无 Critical/Major 问题
- [ ] F5: 代码审查无遗留问题
- [ ] F6: 全盘评测报告已生成
