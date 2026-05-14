#![allow(deprecated)] // memory-light is intentionally used during migration to memory crate
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use memory::MemoryConfig as LegacyMemConfig;

pub struct UnifiedMemoryConfig {
    pub home_dir: PathBuf,
    pub sessions_dir: PathBuf,
    pub enable_legacy: bool,
    pub identity_path: Option<PathBuf>,
}

pub struct UnifiedMemoryManager {
    identity_path: PathBuf,
    memory_light: Option<Arc<memory_light::MemoryManager>>,
    memory_legacy: RwLock<Option<Arc<memory::cognitive::CognitiveContextManager>>>,
    event_logger: Option<Arc<crate::event_log::SessionEventLogger>>,
}

impl UnifiedMemoryManager {
    pub fn new(config: UnifiedMemoryConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let identity_path = config.identity_path.unwrap_or_else(|| config.home_dir.join("identity.md"));
        let sessions_dir = config.sessions_dir;

        let memory_light = memory_light::MemoryManager::new(&config.home_dir).ok().map(Arc::new);
        let memory_legacy = RwLock::new(None);
        if config.enable_legacy {
            let handle = tokio::runtime::Handle::try_current();
            if let Ok(h) = handle {
                let mem_cfg = LegacyMemConfig::default();
                if let Ok(mgr) = h.block_on(memory::cognitive::CognitiveContextManager::new(mem_cfg)) {
                    *memory_legacy.write().unwrap_or_else(|poisoned| {
                        tracing::warn!("unified memory RwLock poisoned; recovering");
                        poisoned.into_inner()
                    }) = Some(Arc::new(mgr));
                }
            }
        }

        let event_logger = crate::event_log::SessionEventLogger::new(&sessions_dir).ok().map(Arc::new);

        Ok(Self { identity_path, memory_light, memory_legacy, event_logger })
    }

    pub fn prepare_context(&self, session_id: &str, has_compaction: bool) -> String {
        let mut ctx = String::new();

        let identity = if self.identity_path.exists() {
            std::fs::read_to_string(&self.identity_path).unwrap_or_default()
        } else {
            String::from("## Identity\n- Agent: Cowd AI coding assistant\n- Role: Software engineering\n- Style: Concise, direct\n")
        };
        ctx.push_str(&identity);
        ctx.push_str("\n\n");

        if let Some(ref light) = self.memory_light {
            let essentials = light.prepare_context();
            if essentials.len() > 30 { ctx.push_str(&essentials); ctx.push_str("\n\n"); }
        }

        if has_compaction {
            if let Some(ref logger) = self.event_logger {
                let event_ctx = logger.rebuild_context(session_id, 20);
                if !event_ctx.is_empty() { ctx.push_str(&event_ctx); ctx.push_str("\n\n"); }
            }
        }

        // L3: legacy deep semantic recall (with lazy init)
        {
            let legacy = self.memory_legacy.read().unwrap_or_else(|poisoned| {
                tracing::warn!("unified memory RwLock poisoned; recovering");
                poisoned.into_inner()
            });
            if let Some(ref mgr) = *legacy {
                if let Ok(handle) = tokio::runtime::Handle::try_current() {
                    let entries = handle.block_on(mgr.recall(session_id, 5)).unwrap_or_default();
                    if !entries.is_empty() {
                        ctx.push_str("## Deep Memory\n");
                        for e in entries.iter().take(5) {
                            ctx.push_str(&format!("- [{}] {}\n", e.title, e.content.chars().take(200).collect::<String>()));
                        }
                        ctx.push_str("\n");
                    }
                }
            }
        }

        ctx
    }

    pub fn post_turn(&self, user_msg: &str, assistant_msg: &str, session_id: &str, session_messages: &[crate::session::ConversationMessage]) {
        if let Some(ref light) = self.memory_light {
            light.after_turn(user_msg, assistant_msg);
        }

        if let Some(ref event_logger) = self.event_logger {
            let turn = session_messages.len() as u32;
            for msg in session_messages.iter().rev().take(4) {
                for block in &msg.blocks {
                    match block {
                        crate::session::ContentBlock::ToolUse { name, .. } => {
                            event_logger.record(session_id, turn, "tool_call", &format!("tool used: {name}"));
                        }
                        crate::session::ContentBlock::ToolResult { tool_name, output, is_error, .. } => {
                            let status = if *is_error { "failed" } else { "completed" };
                            let preview: String = output.chars().take(80).collect();
                            event_logger.record(session_id, turn, "tool_result", &format!("{tool_name} {status}: {preview}"));
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    pub fn search_memories(&self, query: &str, limit: usize) -> Vec<memory_light::MemoryEntry> {
        match &self.memory_light {
            Some(light) => light.search(query, limit),
            None => Vec::new(),
        }
    }

    pub fn memory_light(&self) -> Option<&Arc<memory_light::MemoryManager>> { self.memory_light.as_ref() }
    pub fn event_logger(&self) -> Option<&Arc<crate::event_log::SessionEventLogger>> { self.event_logger.as_ref() }
}
