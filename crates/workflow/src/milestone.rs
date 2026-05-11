// M7.3 + 07: Milestone + ProjectLifecycle with validation gates.
// Derived from get-shit-done project lifecycle (new→plan→verify→ship→graduate).

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MilestoneStatus { Draft, InProgress, Reviewing, Completed, Graduated }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Milestone { pub name: String, pub status: MilestoneStatus }

impl Milestone {
    pub fn new(name: &str) -> Self { Self { name: name.to_string(), status: MilestoneStatus::Draft } }
    pub fn advance(&mut self) {
        self.status = match self.status {
            MilestoneStatus::Draft => MilestoneStatus::InProgress,
            MilestoneStatus::InProgress => MilestoneStatus::Reviewing,
            MilestoneStatus::Reviewing => MilestoneStatus::Completed,
            _ => self.status.clone(),
        };
    }
}

// 07: Project lifecycle with validation gates

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProjectPhase {
    Discovery,
    Planning,
    Building,
    Reviewing,
    Shipping,
    Graduated,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PhaseRecord {
    pub phase: ProjectPhase,
    pub started_at: i64,
    pub completed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectLifecycle {
    pub current: ProjectPhase,
    pub history: Vec<PhaseRecord>,
    pub has_milestone: bool,
    pub has_tests: bool,
    pub has_review: bool,
}

impl ProjectLifecycle {
    pub fn new() -> Self {
        Self {
            current: ProjectPhase::Discovery,
            history: vec![PhaseRecord { phase: ProjectPhase::Discovery, started_at: now_ms(), completed_at: None }],
            has_milestone: false, has_tests: false, has_review: false,
        }
    }

    pub fn set_milestone(&mut self, v: bool) { self.has_milestone = v; }
    pub fn set_tests(&mut self, v: bool) { self.has_tests = v; }
    pub fn set_review(&mut self, v: bool) { self.has_review = v; }

    pub fn advance(&mut self) -> Result<ProjectPhase, String> {
        let next = match self.current {
            ProjectPhase::Discovery => {
                if !self.has_milestone { return Err("need milestone to advance from Discovery".into()); }
                ProjectPhase::Planning
            }
            ProjectPhase::Planning => ProjectPhase::Building,
            ProjectPhase::Building => {
                if !self.has_tests { return Err("need tests to advance from Building".into()); }
                ProjectPhase::Reviewing
            }
            ProjectPhase::Reviewing => {
                if !self.has_review { return Err("need review to advance from Reviewing".into()); }
                ProjectPhase::Shipping
            }
            ProjectPhase::Shipping => ProjectPhase::Graduated,
            ProjectPhase::Graduated => return Err("already graduated".into()),
        };
        self.complete_current();
        self.current = next.clone();
        self.history.push(PhaseRecord { phase: next, started_at: now_ms(), completed_at: None });
        Ok(self.current.clone())
    }

    fn complete_current(&mut self) {
        if let Some(rec) = self.history.iter_mut().find(|r| r.phase == self.current) {
            rec.completed_at = Some(now_ms());
        }
    }

    pub fn phase_count(&self) -> usize { self.history.len() }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn t07_six_phases_present() {
        let phases = [ProjectPhase::Discovery, ProjectPhase::Planning, ProjectPhase::Building,
                       ProjectPhase::Reviewing, ProjectPhase::Shipping, ProjectPhase::Graduated];
        assert_eq!(phases.len(), 6);
    }

    #[test]
    fn t07_blocked_without_milestone() {
        let mut lc = ProjectLifecycle::new();
        let r = lc.advance();
        assert!(r.is_err());
        assert!(r.unwrap_err().contains("milestone"));
    }

    #[test]
    fn t07_full_lifecycle() {
        let mut lc = ProjectLifecycle::new();
        assert_eq!(lc.current, ProjectPhase::Discovery);
        lc.set_milestone(true);
        assert!(lc.advance().is_ok());
        assert_eq!(lc.current, ProjectPhase::Planning);
        assert!(lc.advance().is_ok());
        assert_eq!(lc.current, ProjectPhase::Building);
        lc.set_tests(true);
        assert!(lc.advance().is_ok());
        assert_eq!(lc.current, ProjectPhase::Reviewing);
        lc.set_review(true);
        assert!(lc.advance().is_ok());
        assert_eq!(lc.current, ProjectPhase::Shipping);
        assert!(lc.advance().is_ok());
        assert_eq!(lc.current, ProjectPhase::Graduated);
        assert_eq!(lc.phase_count(), 6);
    }
}