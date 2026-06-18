use std::sync::mpsc;
use std::sync::Arc;

use memory::MemoryOrchestrator;
use runtime::{MemoryCallback, ToolCallback};

pub struct TuiToolCallback {
    tx: mpsc::SyncSender<tui::CowdEvent>,
    pub orchestrator: Option<Arc<MemoryOrchestrator>>,
}

impl TuiToolCallback {
    pub fn new(
        tx: mpsc::SyncSender<tui::CowdEvent>,
        orchestrator: Option<Arc<MemoryOrchestrator>>,
    ) -> Self {
        Self { tx, orchestrator }
    }
}

impl ToolCallback for TuiToolCallback {
    fn on_tool_start(&self, id: &str, name: &str, preview: &str) {
        let _ = self.tx.send(tui::CowdEvent::ToolStart {
            id: id.to_string(),
            name: name.to_string(),
            preview: preview.to_string(),
        });
    }

    fn on_tool_progress(&self, id: &str, name: &str, progress: &str) {
        let _ = self.tx.send(tui::CowdEvent::ToolProgress {
            id: id.to_string(),
            name: name.to_string(),
            progress: progress.to_string(),
        });
    }

    fn on_tool_complete(&self, id: &str, name: &str, result_summary: &str, exit_code: Option<i32>) {
        let _ = self.tx.send(tui::CowdEvent::ToolComplete {
            id: id.to_string(),
            name: name.to_string(),
            summary: result_summary.to_string(),
            exit_code,
        });
    }

    fn on_usage(&self, usage: &runtime::TokenUsage) {
        let _ = self.tx.send(tui::CowdEvent::TokenUsage {
            input: usage.input_tokens as u64,
            output: usage.output_tokens as u64,
            cache_create: usage.cache_creation_input_tokens as u64,
            cache_read: usage.cache_read_input_tokens as u64,
        });
    }
}

pub struct TuiMemoryCallback {
    tx: mpsc::SyncSender<tui::CowdEvent>,
}

impl TuiMemoryCallback {
    pub fn new(tx: mpsc::SyncSender<tui::CowdEvent>) -> Self {
        Self { tx }
    }
}

impl MemoryCallback for TuiMemoryCallback {
    fn on_memory_update(&self, entries: Vec<(String, String, f64)>, status: &str) {
        let _ = self.tx.send(tui::CowdEvent::MemoryUpdate {
            entries,
            status: status.to_string(),
        });
    }

    fn on_memory_stats(&self, total_entries: usize, vector_count: usize, layers: Vec<String>) {
        let _ = self.tx.send(tui::CowdEvent::MemoryStats {
            total_entries,
            vector_count,
            layers,
        });
    }
}
