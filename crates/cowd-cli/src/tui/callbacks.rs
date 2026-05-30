use std::sync::mpsc;
use std::sync::Arc;

use crate::tui::TuiEvent;
use memory::MemoryOrchestrator;
use runtime::{MemoryCallback, ToolCallback};

pub struct TuiToolCallback {
    tx: mpsc::SyncSender<TuiEvent>,
    pub orchestrator: Option<Arc<MemoryOrchestrator>>,
}

impl TuiToolCallback {
    pub fn new(tx: mpsc::SyncSender<TuiEvent>, orchestrator: Option<Arc<MemoryOrchestrator>>) -> Self {
        Self { tx, orchestrator }
    }
}

impl ToolCallback for TuiToolCallback {
    fn on_tool_start(&self, id: &str, name: &str, preview: &str) {
        let _ = self.tx.send(TuiEvent::ToolStart {
            id: id.to_string(),
            name: name.to_string(),
            preview: preview.to_string(),
        });
    }

    fn on_tool_progress(&self, id: &str, name: &str, progress: &str) {
        let _ = self.tx.send(TuiEvent::ToolProgress {
            id: id.to_string(),
            name: name.to_string(),
            progress: progress.to_string(),
        });
    }

    fn on_tool_complete(&self, id: &str, name: &str, result_summary: &str, exit_code: Option<i32>) {
        let _ = self.tx.send(TuiEvent::ToolComplete {
            id: id.to_string(),
            name: name.to_string(),
            summary: result_summary.to_string(),
            exit_code,
        });
    }

    fn on_usage(&self, usage: &runtime::TokenUsage) {
        let _ = self.tx.send(TuiEvent::TokenUsage {
            input: usage.input_tokens as u64,
            output: usage.output_tokens as u64,
            cache_create: usage.cache_creation_input_tokens as u64,
            cache_read: usage.cache_read_input_tokens as u64,
        });
    }
}

pub struct TuiMemoryCallback {
    tx: mpsc::SyncSender<TuiEvent>,
}

impl TuiMemoryCallback {
    pub fn new(tx: mpsc::SyncSender<TuiEvent>) -> Self {
        Self { tx }
    }
}

impl MemoryCallback for TuiMemoryCallback {
    fn on_memory_update(&self, entries: Vec<(String, String, f64)>, status: &str) {
        let _ = self.tx.send(TuiEvent::MemoryUpdate {
            entries,
            status: status.to_string(),
        });
    }

    fn on_memory_stats(&self, total_entries: usize, vector_count: usize, layers: Vec<String>) {
        let _ = self.tx.send(TuiEvent::MemoryStats {
            total_entries,
            vector_count,
            layers,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_tool_start(event: &TuiEvent, exp_id: &str, exp_name: &str) {
        if let TuiEvent::ToolStart { id, name, .. } = event {
            assert_eq!(id, exp_id);
            assert_eq!(name, exp_name);
        } else {
            panic!("expected ToolStart, got {event:?}");
        }
    }

    #[test]
    fn tool_start_sends_event() {
        let (tx, rx) = crate::tui::tui_event_channel();
        let cb = TuiToolCallback::new(tx, None);
        cb.on_tool_start("t1", "bash", "ls -la");
        let event = rx.recv().unwrap();
        assert_tool_start(&event, "t1", "bash");
    }

    #[test]
    fn tool_progress_sends_event() {
        let (tx, rx) = crate::tui::tui_event_channel();
        let cb = TuiToolCallback::new(tx, None);
        cb.on_tool_progress("t1", "bash", "running...");
        let event = rx.recv().unwrap();
        if let TuiEvent::ToolProgress { id, name, progress } = event {
            assert_eq!(id, "t1");
            assert_eq!(name, "bash");
            assert_eq!(progress, "running...");
        } else {
            panic!("expected ToolProgress");
        }
    }

    #[test]
    fn tool_complete_sends_event() {
        let (tx, rx) = crate::tui::tui_event_channel();
        let cb = TuiToolCallback::new(tx, None);
        cb.on_tool_complete("t1", "bash", "files listed", Some(0));
        let event = rx.recv().unwrap();
        if let TuiEvent::ToolComplete { id, name, summary, exit_code } = event {
            assert_eq!(id, "t1");
            assert_eq!(name, "bash");
            assert_eq!(summary, "files listed");
            assert_eq!(exit_code, Some(0));
        } else {
            panic!("expected ToolComplete");
        }
    }

    #[test]
    fn tool_complete_error_exit_code() {
        let (tx, rx) = crate::tui::tui_event_channel();
        let cb = TuiToolCallback::new(tx, None);
        cb.on_tool_complete("t2", "grep", "not found", Some(1));
        let event = rx.recv().unwrap();
        if let TuiEvent::ToolComplete { exit_code, .. } = event {
            assert_eq!(exit_code, Some(1));
        } else {
            panic!("expected ToolComplete");
        }
    }

    #[test]
    fn channel_full_applies_backpressure() {
        let (tx, rx) = crate::tui::tui_event_channel();
        let cb = TuiToolCallback::new(tx, None);
        // Spawn a drainer that matches the producer rate
        let _drainer = std::thread::spawn(move || {
            while rx.recv().is_ok() {}
        });
        // With backpressure, send blocks — drainer keeps channel flowing
        for _ in 0..300 {
            cb.on_tool_start("x", "y", "z");
        }
    }
}
