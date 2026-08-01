#![allow(
    clippy::doc_markdown,
    clippy::match_same_arms,
    clippy::must_use_candidate,
    clippy::uninlined_format_args,
    clippy::unnested_or_patterns
)]
//! Bridge between MCP tool surface (ListMcpResources, ReadMcpResource, McpAuth, MCP)
//! and the existing McpServerManager runtime.
//!
//! Provides a stateful client registry that tool handlers can use to
//! connect to MCP servers and invoke their capabilities.

use std::collections::HashMap;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;

use crate::mcp::mcp_tool_name;
use crate::mcp_stdio::{
    McpListResourcesResult, McpReadResourceResult, McpServerManager, McpToolDiscoveryReport,
};
use serde::{Deserialize, Serialize};

/// Status of a managed MCP server connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum McpConnectionStatus {
    Disconnected,
    Connecting,
    Connected,
    AuthRequired,
    Error,
}

impl std::fmt::Display for McpConnectionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connecting => write!(f, "connecting"),
            Self::Connected => write!(f, "connected"),
            Self::AuthRequired => write!(f, "auth_required"),
            Self::Error => write!(f, "error"),
        }
    }
}

/// Metadata about an MCP resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResourceInfo {
    pub uri: String,
    pub name: String,
    pub description: Option<String>,
    pub mime_type: Option<String>,
}

/// Metadata about an MCP tool exposed by a server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpToolInfo {
    pub name: String,
    pub description: Option<String>,
    pub input_schema: Option<serde_json::Value>,
}

/// Tracked state of an MCP server connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpServerState {
    pub server_name: String,
    pub status: McpConnectionStatus,
    pub tools: Vec<McpToolInfo>,
    pub resources: Vec<McpResourceInfo>,
    pub server_info: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Clone, Default)]
pub struct McpToolRegistry {
    inner: Arc<RwLock<HashMap<String, McpServerState>>>,
    workers: Arc<McpWorkerPool>,
}

enum McpWorkerCommand {
    Discover {
        reply: Sender<Result<McpToolDiscoveryReport, String>>,
    },
    Call {
        qualified_tool_name: String,
        arguments: Option<serde_json::Value>,
        reply: Sender<Result<serde_json::Value, String>>,
    },
    ListResources {
        server_name: String,
        reply: Sender<Result<McpListResourcesResult, String>>,
    },
    ReadResource {
        server_name: String,
        uri: String,
        reply: Sender<Result<McpReadResourceResult, String>>,
    },
    Shutdown {
        reply: Sender<Result<(), String>>,
    },
}

struct McpServerWorker {
    sender: Sender<McpWorkerCommand>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl McpServerWorker {
    fn spawn(server_name: &str, manager: McpServerManager) -> Result<Arc<Self>, String> {
        let (sender, receiver) = mpsc::channel();
        let thread_name = format!("mcp-server-worker-{server_name}");
        let join = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || Self::run(manager, receiver))
            .map_err(|error| format!("failed to start MCP server worker: {error}"))?;
        Ok(Arc::new(Self {
            sender,
            join: Mutex::new(Some(join)),
        }))
    }

    fn run(mut manager: McpServerManager, receiver: Receiver<McpWorkerCommand>) {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                tracing::error!(error = %error, "failed to create MCP server worker runtime");
                return;
            }
        };

        while let Ok(command) = receiver.recv() {
            match command {
                McpWorkerCommand::Discover { reply } => {
                    let report = runtime.block_on(manager.discover_tools_best_effort());
                    let _ = reply.send(Ok(report));
                }
                McpWorkerCommand::Call {
                    qualified_tool_name,
                    arguments,
                    reply,
                } => {
                    let result = runtime
                        .block_on(manager.call_tool(&qualified_tool_name, arguments))
                        .map_err(|error| error.to_string())
                        .and_then(|response| {
                            if let Some(error) = response.error {
                                return Err(format!(
                                    "MCP server returned JSON-RPC error for tools/call: {} ({})",
                                    error.message, error.code
                                ));
                            }
                            let result = response.result.ok_or_else(|| {
                                "MCP server returned no result for tools/call".to_string()
                            })?;
                            serde_json::to_value(result).map_err(|error| {
                                format!("failed to serialize MCP tool result: {error}")
                            })
                        });
                    let _ = reply.send(result);
                }
                McpWorkerCommand::ListResources { server_name, reply } => {
                    let result = runtime
                        .block_on(manager.list_resources(&server_name))
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                McpWorkerCommand::ReadResource {
                    server_name,
                    uri,
                    reply,
                } => {
                    let result = runtime
                        .block_on(manager.read_resource(&server_name, &uri))
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                }
                McpWorkerCommand::Shutdown { reply } => {
                    let result = runtime
                        .block_on(manager.shutdown())
                        .map_err(|error| error.to_string());
                    let _ = reply.send(result);
                    return;
                }
            }
        }

        if let Err(error) = runtime.block_on(manager.shutdown()) {
            tracing::warn!(error = %error, "failed to shut down MCP server after worker disconnect");
        }
    }

    fn call_tool(
        &self,
        qualified_tool_name: String,
        arguments: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let (reply_sender, reply_receiver) = mpsc::channel();
        self.sender
            .send(McpWorkerCommand::Call {
                qualified_tool_name,
                arguments,
                reply: reply_sender,
            })
            .map_err(|_| "MCP server worker is not running".to_string())?;
        reply_receiver
            .recv()
            .map_err(|_| "MCP server worker stopped before returning a result".to_string())?
    }

    fn discover_tools(&self) -> Result<McpToolDiscoveryReport, String> {
        let (reply_sender, reply_receiver) = mpsc::channel();
        self.sender
            .send(McpWorkerCommand::Discover {
                reply: reply_sender,
            })
            .map_err(|_| "MCP server worker is not running".to_string())?;
        reply_receiver
            .recv()
            .map_err(|_| "MCP server worker stopped during tool discovery".to_string())?
    }

    fn list_resources(&self, server_name: &str) -> Result<McpListResourcesResult, String> {
        let (reply_sender, reply_receiver) = mpsc::channel();
        self.sender
            .send(McpWorkerCommand::ListResources {
                server_name: server_name.to_string(),
                reply: reply_sender,
            })
            .map_err(|_| "MCP server worker is not running".to_string())?;
        reply_receiver
            .recv()
            .map_err(|_| "MCP server worker stopped while listing resources".to_string())?
    }

    fn read_resource(&self, server_name: &str, uri: &str) -> Result<McpReadResourceResult, String> {
        let (reply_sender, reply_receiver) = mpsc::channel();
        self.sender
            .send(McpWorkerCommand::ReadResource {
                server_name: server_name.to_string(),
                uri: uri.to_string(),
                reply: reply_sender,
            })
            .map_err(|_| "MCP server worker is not running".to_string())?;
        reply_receiver
            .recv()
            .map_err(|_| "MCP server worker stopped while reading a resource".to_string())?
    }

    fn shutdown(&self) -> Result<(), String> {
        let (reply_sender, reply_receiver) = mpsc::channel();
        let command_result = self.sender.send(McpWorkerCommand::Shutdown {
            reply: reply_sender,
        });
        let shutdown_result = match command_result {
            Ok(()) => reply_receiver
                .recv()
                .map_err(|_| "MCP server worker stopped during shutdown".to_string())?,
            Err(_) => Ok(()),
        };

        let join = self
            .join
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(join) = join {
            join.join()
                .map_err(|_| "MCP server worker panicked during shutdown".to_string())?;
        }
        shutdown_result
    }
}

#[derive(Default)]
struct McpWorkerPool {
    workers: RwLock<HashMap<String, Arc<McpServerWorker>>>,
}

impl McpWorkerPool {
    fn install(
        &self,
        server_name: &str,
        manager: McpServerManager,
    ) -> Result<Arc<McpServerWorker>, String> {
        let worker = McpServerWorker::spawn(server_name, manager)?;
        let previous = self
            .workers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(server_name.to_string(), Arc::clone(&worker));
        if let Some(previous) = previous {
            previous.shutdown()?;
        }
        Ok(worker)
    }

    fn get(&self, server_name: &str) -> Option<Arc<McpServerWorker>> {
        self.workers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(server_name)
            .cloned()
    }

    fn remove(&self, server_name: &str) -> Option<Arc<McpServerWorker>> {
        self.workers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(server_name)
    }

    fn shutdown_all(&self) -> Result<(), String> {
        let workers = {
            let mut guard = self
                .workers
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.drain().map(|(_, worker)| worker).collect::<Vec<_>>()
        };
        let errors = workers
            .into_iter()
            .filter_map(|worker| worker.shutdown().err())
            .collect::<Vec<_>>();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

impl Drop for McpWorkerPool {
    fn drop(&mut self) {
        if let Err(error) = self.shutdown_all() {
            tracing::warn!(error = %error, "failed to shut down MCP worker pool");
        }
    }
}

impl McpToolRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a server-specific long-lived worker.
    ///
    /// A worker owns one `McpServerManager` and its Tokio runtime. This keeps
    /// calls for one stdio server ordered while allowing separate servers to
    /// execute in parallel without recreating their child process per call.
    pub fn install_server_manager(
        &self,
        server_name: &str,
        manager: McpServerManager,
    ) -> Result<McpToolDiscoveryReport, String> {
        let worker = self.workers.install(server_name, manager)?;
        worker.discover_tools()
    }

    pub fn register_server(
        &self,
        server_name: &str,
        status: McpConnectionStatus,
        tools: Vec<McpToolInfo>,
        resources: Vec<McpResourceInfo>,
        server_info: Option<String>,
    ) {
        let mut inner = self.inner.write().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.insert(
            server_name.to_owned(),
            McpServerState {
                server_name: server_name.to_owned(),
                status,
                tools,
                resources,
                server_info,
                error_message: None,
            },
        );
    }

    pub fn get_server(&self, server_name: &str) -> Option<McpServerState> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.get(server_name).cloned()
    }

    pub fn list_servers(&self) -> Vec<McpServerState> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.values().cloned().collect()
    }

    pub fn list_resources(&self, server_name: &str) -> Result<Vec<McpResourceInfo>, String> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let state = inner
            .get(server_name)
            .ok_or_else(|| format!("server '{}' not found", server_name))?;
        if state.status != McpConnectionStatus::Connected {
            return Err(format!(
                "server '{}' is not connected (status: {})",
                server_name, state.status
            ));
        }
        let cached = state.resources.clone();
        drop(inner);

        let Some(worker) = self.workers.get(server_name) else {
            return Ok(cached);
        };
        worker.list_resources(server_name).map(|result| {
            result
                .resources
                .into_iter()
                .map(|resource| McpResourceInfo {
                    name: resource.name.unwrap_or_else(|| resource.uri.clone()),
                    uri: resource.uri,
                    description: resource.description,
                    mime_type: resource.mime_type,
                })
                .collect()
        })
    }

    pub fn read_resource(&self, server_name: &str, uri: &str) -> Result<McpResourceInfo, String> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let state = inner
            .get(server_name)
            .ok_or_else(|| format!("server '{}' not found", server_name))?;

        if state.status != McpConnectionStatus::Connected {
            return Err(format!(
                "server '{}' is not connected (status: {})",
                server_name, state.status
            ));
        }

        state
            .resources
            .iter()
            .find(|r| r.uri == uri)
            .cloned()
            .ok_or_else(|| format!("resource '{}' not found on server '{}'", uri, server_name))
    }

    pub fn read_resource_contents(
        &self,
        server_name: &str,
        uri: &str,
    ) -> Result<McpReadResourceResult, String> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let state = inner
            .get(server_name)
            .ok_or_else(|| format!("server '{}' not found", server_name))?;
        if state.status != McpConnectionStatus::Connected {
            return Err(format!(
                "server '{}' is not connected (status: {})",
                server_name, state.status
            ));
        }
        drop(inner);

        self.workers
            .get(server_name)
            .ok_or_else(|| format!("MCP server '{}' manager is not configured", server_name))?
            .read_resource(server_name, uri)
    }

    pub fn list_tools(&self, server_name: &str) -> Result<Vec<McpToolInfo>, String> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        match inner.get(server_name) {
            Some(state) => {
                if state.status != McpConnectionStatus::Connected {
                    return Err(format!(
                        "server '{}' is not connected (status: {})",
                        server_name, state.status
                    ));
                }
                Ok(state.tools.clone())
            }
            None => Err(format!("server '{}' not found", server_name)),
        }
    }

    pub fn call_tool(
        &self,
        server_name: &str,
        tool_name: &str,
        arguments: &serde_json::Value,
    ) -> Result<serde_json::Value, String> {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let state = inner
            .get(server_name)
            .ok_or_else(|| format!("server '{}' not found", server_name))?;

        if state.status != McpConnectionStatus::Connected {
            return Err(format!(
                "server '{}' is not connected (status: {})",
                server_name, state.status
            ));
        }

        if !state.tools.iter().any(|t| t.name == tool_name) {
            return Err(format!(
                "tool '{}' not found on server '{}'",
                tool_name, server_name
            ));
        }

        drop(inner);

        let worker = self
            .workers
            .get(server_name)
            .ok_or_else(|| format!("MCP server '{}' manager is not configured", server_name))?;

        worker.call_tool(
            mcp_tool_name(server_name, tool_name),
            (!arguments.is_null()).then(|| arguments.clone()),
        )
    }

    /// Set auth status for a server.
    pub fn set_auth_status(
        &self,
        server_name: &str,
        status: McpConnectionStatus,
    ) -> Result<(), String> {
        let mut inner = self.inner.write().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let state = inner
            .get_mut(server_name)
            .ok_or_else(|| format!("server '{}' not found", server_name))?;
        state.status = status;
        Ok(())
    }

    /// Disconnect / remove a server.
    pub fn disconnect(&self, server_name: &str) -> Option<McpServerState> {
        let mut inner = self.inner.write().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let removed = inner.remove(server_name);
        drop(inner);
        if let Some(worker) = self.workers.remove(server_name) {
            if let Err(error) = worker.shutdown() {
                tracing::warn!(server = server_name, error = %error, "failed to shut down MCP server worker");
            }
        }
        removed
    }

    /// Number of registered servers.
    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap_or_else(|poisoned| {
            tracing::warn!("mcp tool bridge registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn shutdown_all(&self) -> Result<(), String> {
        self.workers.shutdown_all()
    }

    pub fn remove_server_manager(&self, server_name: &str) -> Result<(), String> {
        self.workers
            .remove(server_name)
            .map_or(Ok(()), |worker| worker.shutdown())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::config::{
        ConfigSource, McpServerConfig, McpStdioServerConfig, ScopedMcpServerConfig,
    };

    fn temp_dir() -> PathBuf {
        static NEXT_TEMP_DIR_ID: AtomicU64 = AtomicU64::new(0);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_else(|e| {
                tracing::warn!("system time error: {}, using 0 as fallback", e);
                std::time::Duration::from_secs(0)
            })
            .as_nanos();
        let unique_id = NEXT_TEMP_DIR_ID.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("runtime-mcp-tool-bridge-{nanos}-{unique_id}"))
    }

    fn cleanup_script(script_path: &Path) {
        if let Some(root) = script_path.parent() {
            let _ = fs::remove_dir_all(root);
        }
    }

    fn write_bridge_mcp_server_script() -> PathBuf {
        let root = temp_dir();
        fs::create_dir_all(&root).expect("temp dir");
        let script_path = root.join("bridge-mcp-server.py");
        let script = [
            "#!/usr/bin/env python3",
            "import json, os, sys, time",
            "LABEL = os.environ.get('MCP_SERVER_LABEL', 'server')",
            "LOG_PATH = os.environ.get('MCP_LOG_PATH')",
            "CALL_DELAY_MS = int(os.environ.get('MCP_CALL_DELAY_MS', '0'))",
            "PID_PATH = os.environ.get('MCP_PID_PATH')",
            "if PID_PATH:",
            "    with open(PID_PATH, 'w', encoding='utf-8') as handle:",
            "        handle.write(str(os.getpid()))",
            "",
            "def log(method):",
            "    if LOG_PATH:",
            "        with open(LOG_PATH, 'a', encoding='utf-8') as handle:",
            "            handle.write(f'{method}\\n')",
            "",
            "def read_message():",
            "    header = b''",
            r"    while not header.endswith(b'\r\n\r\n'):",
            "        chunk = sys.stdin.buffer.read(1)",
            "        if not chunk:",
            "            return None",
            "        header += chunk",
            "    length = 0",
            r"    for line in header.decode().split('\r\n'):",
            r"        if line.lower().startswith('content-length:'):",
            r"            length = int(line.split(':', 1)[1].strip())",
            "    payload = sys.stdin.buffer.read(length)",
            "    return json.loads(payload.decode())",
            "",
            "def send_message(message):",
            "    payload = json.dumps(message).encode()",
            r"    sys.stdout.buffer.write(f'Content-Length: {len(payload)}\r\n\r\n'.encode() + payload)",
            "    sys.stdout.buffer.flush()",
            "",
            "while True:",
            "    request = read_message()",
            "    if request is None:",
            "        break",
            "    method = request['method']",
            "    log(method)",
            "    if method == 'initialize':",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'protocolVersion': request['params']['protocolVersion'],",
            "                'capabilities': {'tools': {}},",
            "                'serverInfo': {'name': LABEL, 'version': '1.0.0'}",
            "            }",
            "        })",
            "    elif method == 'tools/list':",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'tools': [",
            "                    {",
            "                        'name': 'echo',",
            "                        'description': f'Echo tool for {LABEL}',",
            "                        'inputSchema': {",
            "                            'type': 'object',",
            "                            'properties': {'text': {'type': 'string'}},",
            "                            'required': ['text']",
            "                        }",
            "                    }",
            "                ]",
            "            }",
            "        })",
            "    elif method == 'tools/call':",
            "        args = request['params'].get('arguments') or {}",
            "        text = args.get('text', '')",
            "        if CALL_DELAY_MS:",
            "            time.sleep(CALL_DELAY_MS / 1000)",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'result': {",
            "                'content': [{'type': 'text', 'text': f'{LABEL}:{text}'}],",
            "                'structuredContent': {'server': LABEL, 'echoed': text},",
            "                'isError': False",
            "            }",
            "        })",
            "    else:",
            "        send_message({",
            "            'jsonrpc': '2.0',",
            "            'id': request['id'],",
            "            'error': {'code': -32601, 'message': f'unknown method: {method}'},",
            "        })",
            "",
        ]
        .join("\n");
        fs::write(&script_path, script).expect("write script");
        let mut permissions = fs::metadata(&script_path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&script_path, permissions).expect("chmod");
        script_path
    }

    fn manager_server_config(
        script_path: &Path,
        server_name: &str,
        log_path: &Path,
    ) -> ScopedMcpServerConfig {
        manager_server_config_with_delay(script_path, server_name, log_path, None)
    }

    fn manager_server_config_with_delay(
        script_path: &Path,
        server_name: &str,
        log_path: &Path,
        call_delay_ms: Option<u64>,
    ) -> ScopedMcpServerConfig {
        let mut env = BTreeMap::from([
            ("MCP_SERVER_LABEL".to_string(), server_name.to_string()),
            (
                "MCP_LOG_PATH".to_string(),
                log_path.to_string_lossy().into_owned(),
            ),
        ]);
        if let Some(call_delay_ms) = call_delay_ms {
            env.insert("MCP_CALL_DELAY_MS".to_string(), call_delay_ms.to_string());
        }
        ScopedMcpServerConfig {
            scope: ConfigSource::Local,
            config: McpServerConfig::Stdio(McpStdioServerConfig {
                command: "python3".to_string(),
                args: vec![script_path.to_string_lossy().into_owned()],
                env,
                tool_call_timeout_ms: Some(1_000),
            }),
        }
    }

    fn manager_server_config_with_pid_file(
        script_path: &Path,
        server_name: &str,
        log_path: &Path,
        pid_path: &Path,
    ) -> ScopedMcpServerConfig {
        let mut config = manager_server_config(script_path, server_name, log_path);
        let McpServerConfig::Stdio(stdio) = &mut config.config else {
            unreachable!("bridge test config is always stdio");
        };
        stdio.env.insert(
            "MCP_PID_PATH".to_string(),
            pid_path.to_string_lossy().into_owned(),
        );
        config
    }

    fn manager_for_worker(servers: &BTreeMap<String, ScopedMcpServerConfig>) -> McpServerManager {
        McpServerManager::from_servers(servers)
    }

    #[test]
    fn registers_and_retrieves_server() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "test-server",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "greet".into(),
                description: Some("Greet someone".into()),
                input_schema: None,
            }],
            vec![McpResourceInfo {
                uri: "res://data".into(),
                name: "Data".into(),
                description: None,
                mime_type: Some("application/json".into()),
            }],
            Some("TestServer v1.0".into()),
        );

        let server = registry.get_server("test-server").expect("should exist");
        assert_eq!(server.status, McpConnectionStatus::Connected);
        assert_eq!(server.tools.len(), 1);
        assert_eq!(server.resources.len(), 1);
    }

    #[test]
    fn lists_resources_from_connected_server() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::Connected,
            vec![],
            vec![McpResourceInfo {
                uri: "res://alpha".into(),
                name: "Alpha".into(),
                description: None,
                mime_type: None,
            }],
            None,
        );

        let resources = registry.list_resources("srv").expect("should succeed");
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].uri, "res://alpha");
    }

    #[test]
    fn rejects_resource_listing_for_disconnected_server() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::Disconnected,
            vec![],
            vec![],
            None,
        );
        assert!(registry.list_resources("srv").is_err());
    }

    #[test]
    fn reads_specific_resource() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::Connected,
            vec![],
            vec![McpResourceInfo {
                uri: "res://data".into(),
                name: "Data".into(),
                description: Some("Test data".into()),
                mime_type: Some("text/plain".into()),
            }],
            None,
        );

        let resource = registry
            .read_resource("srv", "res://data")
            .expect("should find");
        assert_eq!(resource.name, "Data");

        assert!(registry.read_resource("srv", "res://missing").is_err());
    }

    #[test]
    fn given_connected_server_without_manager_when_calling_tool_then_it_errors() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "greet".into(),
                description: None,
                input_schema: None,
            }],
            vec![],
            None,
        );

        let error = registry
            .call_tool("srv", "greet", &serde_json::json!({"name": "world"}))
            .expect_err("should require a configured manager");
        assert!(error.contains("manager is not configured"));

        // Unknown tool should fail
        assert!(registry
            .call_tool("srv", "missing", &serde_json::json!({}))
            .is_err());
    }

    #[test]
    fn given_connected_server_with_manager_when_calling_tool_then_it_returns_live_result() {
        let script_path = write_bridge_mcp_server_script();
        let root = script_path.parent().expect("script parent");
        let log_path = root.join("bridge.log");
        let servers = BTreeMap::from([(
            "alpha".to_string(),
            manager_server_config(&script_path, "alpha", &log_path),
        )]);
        let manager = manager_for_worker(&servers);

        let registry = McpToolRegistry::new();
        registry.register_server(
            "alpha",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "echo".into(),
                description: Some("Echo tool for alpha".into()),
                input_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"]
                })),
            }],
            vec![],
            Some("bridge test server".into()),
        );
        registry
            .install_server_manager("alpha", manager)
            .expect("install MCP worker");

        let result = registry
            .call_tool("alpha", "echo", &serde_json::json!({"text": "hello"}))
            .expect("should return live MCP result");
        let second_result = registry
            .call_tool("alpha", "echo", &serde_json::json!({"text": "again"}))
            .expect("second call should reuse the live MCP server");

        assert_eq!(
            result["structuredContent"]["server"],
            serde_json::json!("alpha")
        );
        assert_eq!(
            result["structuredContent"]["echoed"],
            serde_json::json!("hello")
        );
        assert_eq!(
            result["content"][0]["text"],
            serde_json::json!("alpha:hello")
        );
        assert_eq!(
            second_result["content"][0]["text"],
            serde_json::json!("alpha:again")
        );

        let log = fs::read_to_string(&log_path).expect("read log");
        assert_eq!(
            log.lines().collect::<Vec<_>>(),
            vec!["initialize", "tools/list", "tools/call", "tools/call"]
        );

        drop(registry);
        cleanup_script(&script_path);
    }

    #[test]
    fn rejects_tool_call_on_disconnected_server() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::AuthRequired,
            vec![McpToolInfo {
                name: "greet".into(),
                description: None,
                input_schema: None,
            }],
            vec![],
            None,
        );

        assert!(registry
            .call_tool("srv", "greet", &serde_json::json!({}))
            .is_err());
    }

    #[test]
    fn sets_auth_and_disconnects() {
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::AuthRequired,
            vec![],
            vec![],
            None,
        );

        registry
            .set_auth_status("srv", McpConnectionStatus::Connected)
            .expect("should succeed");
        let state = registry.get_server("srv").unwrap();
        assert_eq!(state.status, McpConnectionStatus::Connected);

        let removed = registry.disconnect("srv");
        assert!(removed.is_some());
        assert!(registry.is_empty());
    }

    #[test]
    fn rejects_operations_on_missing_server() {
        let registry = McpToolRegistry::new();
        assert!(registry.list_resources("missing").is_err());
        assert!(registry.read_resource("missing", "uri").is_err());
        assert!(registry.list_tools("missing").is_err());
        assert!(registry
            .call_tool("missing", "tool", &serde_json::json!({}))
            .is_err());
        assert!(registry
            .set_auth_status("missing", McpConnectionStatus::Connected)
            .is_err());
    }

    #[test]
    fn mcp_connection_status_display_all_variants() {
        // given
        let cases = [
            (McpConnectionStatus::Disconnected, "disconnected"),
            (McpConnectionStatus::Connecting, "connecting"),
            (McpConnectionStatus::Connected, "connected"),
            (McpConnectionStatus::AuthRequired, "auth_required"),
            (McpConnectionStatus::Error, "error"),
        ];

        // when
        let rendered: Vec<_> = cases
            .into_iter()
            .map(|(status, expected)| (status.to_string(), expected))
            .collect();

        // then
        assert_eq!(
            rendered,
            vec![
                ("disconnected".to_string(), "disconnected"),
                ("connecting".to_string(), "connecting"),
                ("connected".to_string(), "connected"),
                ("auth_required".to_string(), "auth_required"),
                ("error".to_string(), "error"),
            ]
        );
    }

    #[test]
    fn list_servers_returns_all_registered() {
        // given
        let registry = McpToolRegistry::new();
        registry.register_server(
            "alpha",
            McpConnectionStatus::Connected,
            vec![],
            vec![],
            None,
        );
        registry.register_server(
            "beta",
            McpConnectionStatus::Connecting,
            vec![],
            vec![],
            None,
        );

        // when
        let servers = registry.list_servers();

        // then
        assert_eq!(servers.len(), 2);
        assert!(servers.iter().any(|server| server.server_name == "alpha"));
        assert!(servers.iter().any(|server| server.server_name == "beta"));
    }

    #[test]
    fn list_tools_from_connected_server() {
        // given
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "inspect".into(),
                description: Some("Inspect data".into()),
                input_schema: Some(serde_json::json!({"type": "object"})),
            }],
            vec![],
            None,
        );

        // when
        let tools = registry.list_tools("srv").expect("tools should list");

        // then
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "inspect");
    }

    #[test]
    fn list_tools_rejects_disconnected_server() {
        // given
        let registry = McpToolRegistry::new();
        registry.register_server(
            "srv",
            McpConnectionStatus::AuthRequired,
            vec![],
            vec![],
            None,
        );

        // when
        let result = registry.list_tools("srv");

        // then
        let error = result.expect_err("non-connected server should fail");
        assert!(error.contains("not connected"));
        assert!(error.contains("auth_required"));
    }

    #[test]
    fn list_tools_rejects_missing_server() {
        // given
        let registry = McpToolRegistry::new();

        // when
        let result = registry.list_tools("missing");

        // then
        assert_eq!(
            result.expect_err("missing server should fail"),
            "server 'missing' not found"
        );
    }

    #[test]
    fn get_server_returns_none_for_missing() {
        // given
        let registry = McpToolRegistry::new();

        // when
        let server = registry.get_server("missing");

        // then
        assert!(server.is_none());
    }

    #[test]
    fn call_tool_payload_structure() {
        let script_path = write_bridge_mcp_server_script();
        let root = script_path.parent().expect("script parent");
        let log_path = root.join("payload.log");
        let servers = BTreeMap::from([(
            "srv".to_string(),
            manager_server_config(&script_path, "srv", &log_path),
        )]);
        let registry = McpToolRegistry::new();
        let arguments = serde_json::json!({"text": "world"});
        registry.register_server(
            "srv",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "echo".into(),
                description: Some("Echo tool for srv".into()),
                input_schema: Some(serde_json::json!({
                    "type": "object",
                    "properties": {"text": {"type": "string"}},
                    "required": ["text"]
                })),
            }],
            vec![],
            None,
        );
        registry
            .install_server_manager("srv", manager_for_worker(&servers))
            .expect("install MCP worker");

        let result = registry
            .call_tool("srv", "echo", &arguments)
            .expect("tool should return live payload");

        assert_eq!(result["structuredContent"]["server"], "srv");
        assert_eq!(result["structuredContent"]["echoed"], "world");
        assert_eq!(result["content"][0]["text"], "srv:world");

        drop(registry);
        cleanup_script(&script_path);
    }

    #[test]
    fn different_mcp_servers_execute_calls_in_parallel() {
        let script_path = write_bridge_mcp_server_script();
        let root = script_path.parent().expect("script parent");
        let alpha_log = root.join("alpha.log");
        let beta_log = root.join("beta.log");
        let alpha_servers = BTreeMap::from([(
            "alpha".to_string(),
            manager_server_config_with_delay(&script_path, "alpha", &alpha_log, Some(250)),
        )]);
        let beta_servers = BTreeMap::from([(
            "beta".to_string(),
            manager_server_config_with_delay(&script_path, "beta", &beta_log, Some(250)),
        )]);
        let registry = McpToolRegistry::new();
        for (server_name, manager) in [
            ("alpha", manager_for_worker(&alpha_servers)),
            ("beta", manager_for_worker(&beta_servers)),
        ] {
            registry.register_server(
                server_name,
                McpConnectionStatus::Connected,
                vec![McpToolInfo {
                    name: "echo".into(),
                    description: None,
                    input_schema: None,
                }],
                vec![],
                None,
            );
            registry
                .install_server_manager(server_name, manager)
                .expect("install MCP worker");
        }

        let started = Instant::now();
        let alpha = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                registry.call_tool("alpha", "echo", &serde_json::json!({"text": "one"}))
            })
        };
        let beta = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                registry.call_tool("beta", "echo", &serde_json::json!({"text": "two"}))
            })
        };
        let alpha = alpha
            .join()
            .expect("alpha call thread")
            .expect("alpha call");
        let beta = beta.join().expect("beta call thread").expect("beta call");
        let elapsed = started.elapsed();

        assert_eq!(alpha["structuredContent"]["server"], "alpha");
        assert_eq!(beta["structuredContent"]["server"], "beta");
        assert!(
            elapsed < Duration::from_millis(450),
            "independent MCP servers must overlap, elapsed={elapsed:?}"
        );

        drop(registry);
        cleanup_script(&script_path);
    }

    #[test]
    fn same_mcp_server_serializes_calls_without_restarting() {
        let script_path = write_bridge_mcp_server_script();
        let root = script_path.parent().expect("script parent");
        let log_path = root.join("serial.log");
        let servers = BTreeMap::from([(
            "serial".to_string(),
            manager_server_config_with_delay(&script_path, "serial", &log_path, Some(180)),
        )]);
        let registry = McpToolRegistry::new();
        registry.register_server(
            "serial",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "echo".into(),
                description: None,
                input_schema: None,
            }],
            vec![],
            None,
        );
        registry
            .install_server_manager("serial", manager_for_worker(&servers))
            .expect("install MCP worker");

        let started = Instant::now();
        let first = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                registry.call_tool("serial", "echo", &serde_json::json!({"text": "first"}))
            })
        };
        let second = {
            let registry = registry.clone();
            std::thread::spawn(move || {
                registry.call_tool("serial", "echo", &serde_json::json!({"text": "second"}))
            })
        };
        first
            .join()
            .expect("first call thread")
            .expect("first call");
        second
            .join()
            .expect("second call thread")
            .expect("second call");
        let elapsed = started.elapsed();

        assert!(
            elapsed >= Duration::from_millis(320),
            "one MCP server must not interleave stdio calls, elapsed={elapsed:?}"
        );
        let log = fs::read_to_string(&log_path).expect("read serial log");
        assert_eq!(
            log.lines().collect::<Vec<_>>(),
            vec!["initialize", "tools/list", "tools/call", "tools/call"]
        );

        drop(registry);
        cleanup_script(&script_path);
    }

    #[test]
    fn dropping_registry_stops_and_joins_the_mcp_worker_process() {
        let script_path = write_bridge_mcp_server_script();
        let root = script_path.parent().expect("script parent");
        let log_path = root.join("shutdown.log");
        let pid_path = root.join("server.pid");
        let servers = BTreeMap::from([(
            "shutdown".to_string(),
            manager_server_config_with_pid_file(&script_path, "shutdown", &log_path, &pid_path),
        )]);
        let registry = McpToolRegistry::new();
        registry
            .install_server_manager("shutdown", manager_for_worker(&servers))
            .expect("install MCP worker");

        let pid = fs::read_to_string(&pid_path)
            .expect("server writes its pid during discovery")
            .trim()
            .parse::<u32>()
            .expect("valid server pid");

        drop(registry);

        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            let running = std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|status| status.success());
            if !running {
                cleanup_script(&script_path);
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        cleanup_script(&script_path);
        panic!("MCP worker child process {pid} remained alive after registry drop");
    }

    #[test]
    fn upsert_overwrites_existing_server() {
        // given
        let registry = McpToolRegistry::new();
        registry.register_server("srv", McpConnectionStatus::Connecting, vec![], vec![], None);

        // when
        registry.register_server(
            "srv",
            McpConnectionStatus::Connected,
            vec![McpToolInfo {
                name: "inspect".into(),
                description: None,
                input_schema: None,
            }],
            vec![],
            Some("Inspector".into()),
        );
        let state = registry.get_server("srv").expect("server should exist");

        // then
        assert_eq!(state.status, McpConnectionStatus::Connected);
        assert_eq!(state.tools.len(), 1);
        assert_eq!(state.server_info.as_deref(), Some("Inspector"));
    }

    #[test]
    fn disconnect_missing_returns_none() {
        // given
        let registry = McpToolRegistry::new();

        // when
        let removed = registry.disconnect("missing");

        // then
        assert!(removed.is_none());
    }

    #[test]
    fn len_and_is_empty_transitions() {
        // given
        let registry = McpToolRegistry::new();

        // when
        registry.register_server(
            "alpha",
            McpConnectionStatus::Connected,
            vec![],
            vec![],
            None,
        );
        registry.register_server("beta", McpConnectionStatus::Connected, vec![], vec![], None);
        let after_create = registry.len();
        registry.disconnect("alpha");
        let after_first_remove = registry.len();
        registry.disconnect("beta");

        // then
        assert_eq!(after_create, 2);
        assert_eq!(after_first_remove, 1);
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
    }
}
