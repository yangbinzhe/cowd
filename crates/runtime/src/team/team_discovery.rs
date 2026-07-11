//! P9.2: TeamDiscovery — agent team auto-assembly from skill matching
//! and reputation scoring, with optional SQLite persistence.

use std::path::Path;
use std::sync::{Arc, Mutex};

use memory::agent_directory::{AgentDirectory, AgentInfo};
use memory::agent_reputation::ReputationManager;
use serde::{Deserialize, Serialize};

use crate::skill::SkillActivationRecord;

// ── TeamDiscoveryProtocol ──────────────────────────────────────────────────────

/// Discovers and ranks agents for one runtime scope using an injected
/// `AgentDirectory` plus an optional reputation database.
pub struct TeamDiscoveryProtocol {
    db: Option<Arc<Mutex<rusqlite::Connection>>>,
    directory: Arc<AgentDirectory>,
}

/// A simple team result: a leader and a list of workers.
#[derive(Debug, Clone)]
pub struct DiscoveredTeam {
    pub leader: AgentInfo,
    pub workers: Vec<AgentInfo>,
}

/// A persisted team record in the `teams` SQLite table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedTeam {
    pub team_id: String,
    pub task_description: String,
    pub leader_id: String,
    pub worker_ids: Vec<String>,
    pub created_at_ms: u64,
    pub status: String,
}

impl TeamDiscoveryProtocol {
    /// Create an in-memory-only discovery protocol (no persistence).
    pub fn new() -> Self {
        Self {
            db: None,
            directory: Arc::new(AgentDirectory::new()),
        }
    }

    /// Create a discovery protocol backed by a SQLite database at `db_path`.
    ///
    /// The `teams` table is created on first open if it does not exist.
    pub fn with_db(db_path: &Path) -> Result<Self, String> {
        let handle = storage::StorageHandle::sqlite(
            "team_discovery",
            db_path,
            "runtime.team_discovery",
            "storage_handle_since_0618_hardening",
        );
        Self::with_storage_handle(&handle)
    }

    pub fn with_storage_handle(handle: &storage::StorageHandle) -> Result<Self, String> {
        let conn = storage::SqliteConnectionFactory::default()
            .open_handle(handle)
            .map_err(|e| format!("open db: {e}"))?;
        conn.query_row("PRAGMA journal_mode=WAL", [], |_| Ok(()))
            .map_err(|e| format!("WAL pragma: {e}"))?;
        let _ = conn.execute("PRAGMA foreign_keys=ON", []);
        let _ = conn.execute("PRAGMA busy_timeout=5000", []);
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS teams (
                team_id    TEXT PRIMARY KEY,
                task_desc  TEXT NOT NULL,
                leader_id  TEXT NOT NULL,
                worker_ids TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                status     TEXT NOT NULL DEFAULT 'active'
            );",
        )
        .map_err(|e| format!("create table: {e}"))?;
        Ok(Self {
            db: Some(Arc::new(Mutex::new(conn))),
            directory: Arc::new(AgentDirectory::new()),
        })
    }

    /// Bind discovery to the owner-provided runtime directory. This avoids a
    /// process-global registry leaking agents across sessions or workspaces.
    #[must_use]
    pub fn with_directory(mut self, directory: Arc<AgentDirectory>) -> Self {
        self.directory = directory;
        self
    }

    #[must_use]
    pub fn directory(&self) -> &Arc<AgentDirectory> {
        &self.directory
    }

    // ── discover_team ──────────────────────────────────────────────────────

    /// Discover agents whose capabilities match `required_capabilities`, ranked by a
    /// composite score: capability-overlap count * (1 + reputation composite / 10).
    ///
    /// Returns agents sorted highest-score first.
    pub fn discover_team(
        &self,
        _task_description: &str,
        required_capabilities: &[String],
    ) -> Vec<AgentInfo> {
        let candidates = self.directory.discover(required_capabilities);
        if candidates.is_empty() {
            return Vec::new();
        }

        let mut scored: Vec<_> = candidates
            .into_iter()
            .map(|a| {
                let capability_count = a
                    .capabilities
                    .iter()
                    .filter(|c| required_capabilities.contains(c))
                    .count() as f64;
                let rep_bonus = a
                    .reputation
                    .as_ref()
                    .map(|r| r.composite() / 10.0)
                    .unwrap_or(0.0);
                let score = capability_count * (1.0 + rep_bonus);
                (score, a)
            })
            .collect();

        scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        scored.into_iter().map(|(_, a)| a).collect()
    }

    // ── auto_assemble ──────────────────────────────────────────────────────

    /// Auto-assemble a team for the given task: the highest-ranked agent
    /// becomes the leader; remaining agents become workers.
    pub fn auto_assemble(
        &self,
        task_description: &str,
        required_capabilities: &[String],
    ) -> Option<DiscoveredTeam> {
        let ranked = self.discover_team(task_description, required_capabilities);
        if ranked.is_empty() {
            return None;
        }

        let mut iter = ranked.into_iter();
        let leader = iter.next().expect("non-empty");

        Some(DiscoveredTeam {
            leader,
            workers: iter.collect(),
        })
    }

    /// Auto-assemble agents from skill activation candidates.
    ///
    /// The selected skill name becomes the strongest capability signal, while
    /// high-scoring candidate names are retained as secondary signals.
    pub fn auto_assemble_for_activation(
        &self,
        activation: &SkillActivationRecord,
    ) -> Option<DiscoveredTeam> {
        let required = capability_refs_from_activation(activation);
        if required.is_empty() {
            return None;
        }
        self.auto_assemble(&activation.query, &required)
    }

    // ── persistence helpers ────────────────────────────────────────────────

    /// Persist a discovered team to the `teams` table.
    /// Returns the generated `team_id`.
    pub fn persist_team(
        &self,
        leader_id: &str,
        worker_ids: &[AgentInfo],
        task_description: &str,
    ) -> Result<String, String> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| "no database configured".to_string())?;
        let conn = db.lock().map_err(|e| format!("lock error: {e}"))?;

        let team_id = format!(
            "team_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );
        let wids: Vec<String> = worker_ids.iter().map(|w| w.agent_id.clone()).collect();
        let wids_json = serde_json::to_string(&wids).map_err(|e| format!("json error: {e}"))?;
        let now_ms = now_millis();

        conn.execute(
            "INSERT OR REPLACE INTO teams (team_id, task_desc, leader_id, worker_ids, created_at, status) VALUES (?1, ?2, ?3, ?4, ?5, 'active')",
            rusqlite::params![team_id, task_description, leader_id, wids_json, now_ms as i64],
        )
        .map_err(|e| format!("db error: {e}"))?;

        tracing::info!(%team_id, "persisted discovered team");
        Ok(team_id)
    }

    /// Load all persisted teams from the `teams` table.
    pub fn load_teams(&self) -> Result<Vec<PersistedTeam>, String> {
        let db = self
            .db
            .as_ref()
            .ok_or_else(|| "no database configured".to_string())?;
        let conn = db.lock().map_err(|e| format!("lock error: {e}"))?;

        let mut stmt = conn
            .prepare("SELECT team_id, task_desc, leader_id, worker_ids, created_at, status FROM teams ORDER BY created_at DESC")
            .map_err(|e| format!("prepare error: {e}"))?;

        let rows = stmt
            .query_map([], |row| {
                let worker_ids_json: String = row.get(3)?;
                let worker_ids: Vec<String> =
                    serde_json::from_str(&worker_ids_json).unwrap_or_default();
                Ok(PersistedTeam {
                    team_id: row.get(0)?,
                    task_description: row.get(1)?,
                    leader_id: row.get(2)?,
                    worker_ids,
                    created_at_ms: row.get::<_, i64>(4)? as u64,
                    status: row.get(5)?,
                })
            })
            .map_err(|e| format!("query error: {e}"))?;

        let mut teams = Vec::new();
        for row in rows {
            teams.push(row.map_err(|e| format!("row error: {e}"))?);
        }
        Ok(teams)
    }

    /// Update reputation for an agent in the in-memory directory.
    pub fn record_task_outcome(&self, agent_id: &str, success: bool, peer_rating: Option<f64>) {
        let agents = self.directory.list_active();
        for mut agent in agents {
            if agent.agent_id != agent_id {
                continue;
            }
            let mut rep = agent.reputation.unwrap_or_default();
            rep.task_count += 1;
            if success {
                rep.success_rate = ((rep.success_rate * (rep.task_count - 1) as f64) + 1.0)
                    / rep.task_count as f64;
                rep.recent_failures = 0;
                rep.last_success_at_ms = now_millis();
            } else {
                rep.success_rate =
                    (rep.success_rate * (rep.task_count - 1) as f64) / rep.task_count as f64;
                rep.recent_failures += 1;
            }
            if let Some(rating) = peer_rating {
                let n = rep.task_count as f64;
                rep.peer_rating = (rep.peer_rating * (n - 1.0) + rating) / n;
            }
            agent.reputation = Some(rep);
            self.directory.register(agent);

            // P9: Bidirectional sync — also update ReputationManager
            if let Some(mgr) = ReputationManager::global_opt() {
                let _ =
                    mgr.record_completion(agent_id, if success { 0.85 } else { 0.4 }, success, &[]);
            }

            return;
        }
    }
}

impl Default for TeamDiscoveryProtocol {
    fn default() -> Self {
        Self::new()
    }
}

// ── helpers ────────────────────────────────────────────────────────────────────

fn capability_refs_from_activation(activation: &SkillActivationRecord) -> Vec<String> {
    let mut capabilities = Vec::new();
    if let Some(selected) = &activation.selected {
        capabilities.push(selected.clone());
    }
    for candidate in &activation.candidates {
        if candidate.score < 2 || capabilities.contains(&candidate.name) {
            continue;
        }
        capabilities.push(candidate.name.clone());
    }
    capabilities
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use memory::agent_directory::{AgentInfo, AgentStatus, ReputationScore};

    fn register_test_agent(
        proto: &TeamDiscoveryProtocol,
        id: &str,
        capabilities: Vec<String>,
        rep: Option<ReputationScore>,
    ) {
        let now = now_millis();
        proto.directory().register(AgentInfo {
            agent_id: id.to_string(),
            role: "Executor".to_string(),
            capabilities,
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: rep,
        });
    }

    fn cleanup(proto: &TeamDiscoveryProtocol, ids: &[&str]) {
        for id in ids {
            proto.directory().unregister(id);
        }
    }

    #[test]
    fn discover_team_ranks_by_skill_overlap() {
        let proto = TeamDiscoveryProtocol::new();
        register_test_agent(
            &proto,
            "td1_a1",
            vec!["rust".into(), "testing".into()],
            None,
        );
        register_test_agent(&proto, "td1_a2", vec!["rust".into()], None);
        let ranked =
            proto.discover_team("Build a Rust service", &["rust".into(), "testing".into()]);

        // Verify ordering: agent with 2 matches ranks above agent with 1 match
        let pos_a1 = ranked.iter().position(|a| a.agent_id == "td1_a1");
        let pos_a2 = ranked.iter().position(|a| a.agent_id == "td1_a2");
        assert!(pos_a1.is_some(), "td1_a1 should be present");
        assert!(pos_a2.is_some(), "td1_a2 should be present");
        assert!(
            pos_a1.unwrap() < pos_a2.unwrap(),
            "a1 (2 matches) should rank before a2 (1 match)"
        );

        cleanup(&proto, &["td1_a1", "td1_a2"]);
    }

    #[test]
    fn reputation_boosts_ranking() {
        let proto = TeamDiscoveryProtocol::new();
        register_test_agent(
            &proto,
            "td2_rep_a",
            vec!["rust".into()],
            Some(ReputationScore {
                success_rate: 0.95,
                task_count: 20,
                peer_rating: 4.8,
                last_success_at_ms: now_millis(),
                recent_failures: 0,
            }),
        );
        register_test_agent(
            &proto,
            "td2_rep_b",
            vec!["rust".into(), "testing".into()],
            Some(ReputationScore {
                success_rate: 0.1,
                task_count: 1,
                peer_rating: 0.5,
                last_success_at_ms: 0,
                recent_failures: 5,
            }),
        );

        let ranked = proto.discover_team("Rust refactoring", &["rust".into()]);

        let pos_a = ranked.iter().position(|a| a.agent_id == "td2_rep_a");
        let pos_b = ranked.iter().position(|a| a.agent_id == "td2_rep_b");
        assert!(pos_a.is_some(), "td2_rep_a should be present");
        assert!(pos_b.is_some(), "td2_rep_b should be present");
        assert!(
            pos_a.unwrap() < pos_b.unwrap(),
            "high-rep agent should rank before low-rep agent"
        );

        cleanup(&proto, &["td2_rep_a", "td2_rep_b"]);
    }

    #[test]
    fn auto_assemble_produces_leader_and_workers() {
        let proto = TeamDiscoveryProtocol::new();
        register_test_agent(
            &proto,
            "td3_lead",
            vec!["rust".into(), "planning".into(), "review".into()],
            None,
        );
        register_test_agent(&proto, "td3_wr", vec!["rust".into()], None);
        register_test_agent(&proto, "td3_wt", vec!["testing".into()], None);

        let team = proto
            .auto_assemble(
                "Build a Rust microservice",
                &["rust".into(), "planning".into()],
            )
            .expect("should assemble");

        assert_eq!(
            team.leader.agent_id, "td3_lead",
            "lead-agent should be leader"
        );

        let worker_ids: Vec<_> = team.workers.iter().map(|w| &w.agent_id).collect();
        assert!(
            worker_ids.contains(&&"td3_wr".to_string()),
            "w-rust should be a worker"
        );
        assert!(
            !worker_ids.contains(&&"td3_wt".to_string()),
            "w-test should NOT be a worker (no matching skills)"
        );

        cleanup(&proto, &["td3_lead", "td3_wr", "td3_wt"]);
    }

    #[test]
    fn auto_assemble_for_activation_uses_skill_candidate_names() {
        let proto = TeamDiscoveryProtocol::new();
        register_test_agent(
            &proto,
            "td_skill_release",
            vec!["release".into(), "git-release".into()],
            None,
        );
        register_test_agent(&proto, "td_skill_debug", vec!["debug".into()], None);

        let activation = crate::skill::SkillActivationRecord::new(
            "session-skill",
            1,
            "prepare a release",
            vec![crate::skill::RuntimeSkillCandidate {
                name: "release".to_string(),
                score: 10,
                reasons: vec!["tags:1".to_string()],
                path: None,
                source: crate::skill::activation::RuntimeSkillCandidateSource::Profile,
            }],
        );
        let team = proto
            .auto_assemble_for_activation(&activation)
            .expect("skill activation should assemble a team");

        assert_eq!(team.leader.agent_id, "td_skill_release");

        cleanup(&proto, &["td_skill_release", "td_skill_debug"]);
    }

    #[test]
    fn sqlite_persists_and_loads_teams() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let proto = TeamDiscoveryProtocol::with_db(&db_path).expect("create db");

        register_test_agent(&proto, "td4_lead", vec!["rust".into()], None);
        register_test_agent(&proto, "td4_w1", vec!["testing".into()], None);

        let now = now_millis();
        let w1 = AgentInfo {
            agent_id: "td4_w1".into(),
            role: "Executor".into(),
            capabilities: vec!["testing".into()],
            status: AgentStatus::Active,
            registered_at_ms: now,
            last_heartbeat_ms: now,
            reputation: None,
        };

        proto
            .persist_team("td4_lead", &[w1], "build tests")
            .expect("persist");

        let loaded = proto.load_teams().expect("load");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].task_description, "build tests");
        assert_eq!(loaded[0].leader_id, "td4_lead");
        assert_eq!(loaded[0].worker_ids, vec!["td4_w1"]);
        assert_eq!(loaded[0].status, "active");

        cleanup(&proto, &["td4_lead", "td4_w1"]);
    }

    #[test]
    fn record_task_outcome_updates_reputation_correctly() {
        let proto = TeamDiscoveryProtocol::new();
        register_test_agent(
            &proto,
            "td5_ag",
            vec!["rust".into()],
            Some(ReputationScore {
                success_rate: 0.5,
                task_count: 2,
                peer_rating: 3.0,
                last_success_at_ms: 0,
                recent_failures: 1,
            }),
        );

        proto.record_task_outcome("td5_ag", true, Some(4.0));

        let active = proto.directory().list_active();
        let agent = active.iter().find(|a| a.agent_id == "td5_ag").unwrap();
        let rep = agent.reputation.as_ref().unwrap();
        assert_eq!(rep.task_count, 3);
        assert!(rep.success_rate > 0.5);
        assert_eq!(rep.recent_failures, 0);
        assert!(rep.last_success_at_ms > 0);
        assert!(rep.peer_rating > 3.0);

        cleanup(&proto, &["td5_ag"]);
    }

    #[test]
    fn empty_capabilities_returns_no_agents() {
        let proto = TeamDiscoveryProtocol::new();
        let ranked = proto.discover_team("anything", &[]);
        assert!(ranked.is_empty());
    }

    #[test]
    fn no_matching_agents_returns_none_for_assembly() {
        let proto = TeamDiscoveryProtocol::new();
        let result = proto.auto_assemble("task", &["nobody-has-this".into()]);
        assert!(result.is_none());
    }
}
