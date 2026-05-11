use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressTracker {
    pub completed: usize,
    pub total: usize,
    pub current_phase: String,
    pub phases: Vec<PhaseRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseRecord {
    pub name: String,
    pub completed_at: i64,
    pub tasks_done: usize,
    pub tasks_total: usize,
}

impl ProgressTracker {
    pub fn new(phases: Vec<&str>) -> Self {
        let total = phases.len();
        let records: Vec<PhaseRecord> = phases.iter().map(|name| PhaseRecord {
            name: name.to_string(), completed_at: 0, tasks_done: 0, tasks_total: 1,
        }).collect();
        Self { completed: 0, total, current_phase: phases.first().map(|s| s.to_string()).unwrap_or_default(), phases: records }
    }

    pub fn pct(&self) -> f64 {
        if self.total == 0 { 0.0 } else { self.completed as f64 / self.total as f64 }
    }

    pub fn advance(&mut self) {
        if let Some(phase) = self.phases.iter_mut().find(|p| p.name == self.current_phase) {
            phase.completed_at = now_ms();
            phase.tasks_done = phase.tasks_total;
        }
        self.completed += 1;
        if let Some(idx) = self.phases.iter().position(|p| p.name == self.current_phase) {
            if idx + 1 < self.phases.len() {
                self.current_phase = self.phases[idx + 1].name.clone();
            }
        }
    }

    pub fn progress_bar(&self) -> String {
        let pct = self.pct();
        let filled = (pct * 20.0) as usize;
        format!("[{}{}] {:.0}%", "█".repeat(filled), "░".repeat(20 - filled), pct * 100.0)
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t06_pct_half_done() {
        let mut p = ProgressTracker::new(vec!["plan", "build", "test", "ship"]);
        assert!((p.pct() - 0.0).abs() < 0.01);
        p.advance();
        p.advance();
        assert!((p.pct() - 0.5).abs() < 0.01);
    }

    #[test]
    fn t06_advance_moves_phase() {
        let mut p = ProgressTracker::new(vec!["discovery", "planning", "building"]);
        assert_eq!(p.current_phase, "discovery");
        p.advance();
        assert_eq!(p.current_phase, "planning");
    }

    #[test]
    fn t06_progress_bar_format() {
        let mut p = ProgressTracker::new(vec!["a", "b"]);
        let bar = p.progress_bar();
        assert!(bar.contains('%'));
        p.advance();
        let bar2 = p.progress_bar();
        assert!(bar2.contains("█"));
    }
}