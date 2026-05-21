# Claw CLI STDIO 守护进程协议

## 概述

Claw CLI 支持 `--daemon --stdio` 模式，作为守护进程运行，通过 STDIN/STDOUT 接收和发送 JSON 消息。

## 启动命令

```bash
claw --daemon --stdio --workspace /path/to/workspace
```

## 协议格式

### 请求格式（Gateway → CLI，通过 stdin）

```json
{
  "type": "request",
  "id": "req_001",
  "method": "chat.completion",
  "params": {
    "session_id": "session_abc123",
    "messages": [
      {
        "role": "user",
        "content": "Hello, what can you do?"
      }
    ],
    "model": "claude-sonnet-4-20250514",
    "stream": true,
    "tools": [],
    "tool_choice": "auto"
  }
}
```

### 响应格式（CLI → Gateway，通过 stdout）

#### 流式响应

```json
{
  "type": "response",
  "id": "req_001",
  "event": "delta",
  "data": {
    "delta": {
      "content": "I can help you with..."
    }
  }
}
```

```json
{
  "type": "response",
  "id": "req_001",
  "event": "tool_use",
  "data": {
    "tool_calls": [
      {
        "id": "call_001",
        "type": "function",
        "function": {
          "name": "bash",
          "arguments": "{\"command\": \"ls -la\"}"
        }
      }
    ]
  }
}
```

```json
{
  "type": "response",
  "id": "req_001",
  "event": "done",
  "data": {
    "finish_reason": "stop",
    "usage": {
      "prompt_tokens": 1000,
      "completion_tokens": 500,
      "total_tokens": 1500
    }
  }
}
```

#### 工具结果（Gateway → CLI）

当 CLI 返回 `tool_use` 后，Gateway 执行工具并将结果发送回 CLI：

```json
{
  "type": "request",
  "id": "req_002",
  "method": "chat.completion.continue",
  "params": {
    "session_id": "session_abc123",
    "tool_results": [
      {
        "tool_call_id": "call_001",
        "output": "total 123\n-rw-r--r-- 1 user staff 1234 Apr 15 10:00 file.txt"
      }
    ]
  }
}
```

### 错误格式

```json
{
  "type": "error",
  "id": "req_001",
  "error": {
    "code": "internal_error",
    "message": "Failed to execute tool: bash"
  }
}
```

## 会话管理

### 创建会话

```json
{
  "type": "request",
  "id": "req_003",
  "method": "session.create",
  "params": {
    "session_id": "session_new",
    "workspace": "/path/to/workspace"
  }
}
```

### 列出会话

```json
{
  "type": "request",
  "id": "req_004",
  "method": "session.list",
  "params": {}
}
```

## 记忆管理

### 记忆状态

```json
{
  "type": "request",
  "id": "req_005",
  "method": "memory.status",
  "params": {
    "session_id": "session_abc123"
  }
}
```

### 记忆搜索

```json
{
  "type": "request",
  "id": "req_006",
  "method": "memory.search",
  "params": {
    "query": "user preferences",
    "limit": 5
  }
}
```

## 退出

```json
{
  "type": "request",
  "id": "req_999",
  "method": "shutdown",
  "params": {}
}
```

CLI 响应：

```json
{
  "type": "response",
  "id": "req_999",
  "event": "shutdown",
  "data": {}
}
```

然后 CLI 进程退出。

## 实现要点

1. **JSON Lines 协议**：每行一个完整的 JSON 对象
2. **UTF-8 编码**：所有消息使用 UTF-8 编码
3. **异步处理**：CLI 可以同时处理多个请求（通过不同的 id）
4. **错误恢复**：如果 CLI 崩溃，Gateway 可以重启它
5. **日志输出**：CLI 的日志输出到 stderr，不干扰 stdout 的 JSON 协议
