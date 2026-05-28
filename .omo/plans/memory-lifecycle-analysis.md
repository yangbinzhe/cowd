# 记忆框架核心机制与生命周期分析

## 完整生命周期管道

```
用户输入
    │
    ▼
1. prepare_context() ← 注入 (INJECTION)
    ├── Step 1: L0 Identity  (跨会话不变身份)
    ├── Step 2: L1 Essential (15条热记忆 + 代码热符号)
    ├── Step 3: L2 Project  (项目上下文 + 代码图谱符号)
    ├── Step 4: Session Resume BM25  (最近5条L3)
    ├── Step 5: State Rebuilder  (从持久化状态重建)
    ├── Step 6: Knowledge Graph  (KG实体查询)
    ├── Step 7: L4 Peer Context (多Agent感知)
    ├── Step 8: Code Symbols  (tree-sitter符号注入)
    ├── Step 9: L3 Deep Recall (FTS5 + BM25 + 向量混合)
    ├── Step 10: Fresh Context (新鲜度加权)
    ├── Step 11: Knowledge Graph Deep (时序图谱)
    ├── Step 12: Seeds (触发式种子记忆)
    └── Step 13: Code Context (代码关联注入)

→ LLM 调用 → 工具执行 →
    │
    ▼
2. on_turn_end() ← 保存 (PERSISTENCE)
    ├── Delegation observation → L4
    ├── Background Extractor → 实体+关系+事实提取
    ├── Tool Sandbox → 工具输出索引
    ├── Embedding generation (异步)
    ├── AAAK Compression (符号化压缩)
    └── Closet update (指针索引)

后台 (BACKGROUND):
    ├── DriftDetector.tick() ← 过期 (EXPIRATION)
    │   ├── staleness_decay_per_day (默认0.1/天)
    │   ├── review_threshold → 标记待审查
    │   └── prune_threshold → 自动裁剪
    ├── ContextRotMonitor.tick() ← 轮换 (ROTATION)
    │   ├── 5-call debounce
    │   └── severity upgrade
    ├── HotReload ← 配置变更
    └── FileWatcher ← 文件变更 → 重新索引
```

## 各环节自动化程度

| 环节 | 自动化 | 触发方式 | 完善度 |
|------|--------|----------|--------|
| **保存 (SAVE)** | ✅ 全自动 | on_turn_end() 轮次结束时 | ✅ 完整 |
| **提取 (EXTRACT)** | ✅ 全自动 | BackgroundExtractor 异步运行 | ✅ 实体+关系+事实 |
| **注入 (INJECT)** | ✅ 全自动 | prepare_context() 13步管道 | ✅ 完整 |
| **校正 (CORRECT)** | ✅ 部分自动 | FactChecker + Coherence 检查 | ⚠️ 无自动修正(仅检测) |
| **过期 (EXPIRE)** | ✅ 全自动 | Drift + ContextRot 衰减 | ✅ 阈值可配置 |
| **重建 (REBUILD)** | ✅ 全自动 | StateRebuilder 会话恢复时 | ✅ |

## 关键结论

记忆框架的**保存、注入、过期、重建**四个环节已达到**完全智能化自动化**。**校正环节**仅完成检测（FactChecker/Coherence），缺少自动修正路径。这是唯一需要人工干预的环节。
