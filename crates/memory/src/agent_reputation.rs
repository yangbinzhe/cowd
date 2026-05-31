//! AgentReputation — sub-agent performance tracking and role evolution.
//!
//! Tracks per-agent metrics (tasks completed, quality, punctuality, domain
//! expertise) in a SQLite table and computes a decaying reputation score
//! that influences future agent selection and role specialisation.

use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::params;
use rusqlite::OptionalExtension;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use crate::error::MemoryError;

// ---------------------------------------------------------------------------
// AgentMetrics
// ---------------------------------------------------------------------------

/// Persistent per-agent performance record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentMetrics {
    /// Unique agent identifier (matches `AgentDirectory::AgentInfo::agent_id`).
    pub agent_id: String,
    /// Total tasks completed by this agent.
    pub tasks_completed: u64,
    /// Rolling average quality score (0.0–1.0).
    pub avg_quality_score: f64,
    /// Fraction of tasks completed within their timeout / budget (0.0–1.0).
    pub on_time_rate: f64,
    /// Domain expertise map — e.g. `{"rust": 0.85, "python": 0.60}`.
    pub domain_expertise: HashMap<String, f64>,
    /// Composite reputation score computed from the fields above (0.0–1.0).
    pub reputation_score: f64,
    /// Timestamp of the last metric update.
    pub updated_at: DateTime<Utc>,
}

impl AgentMetrics {
    /// Create a fresh metric record for a new agent.
    pub fn new(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            tasks_completed: 0,
            avg_quality_score: 0.0,
            on_time_rate: 0.0,
            domain_expertise: HashMap::new(),
            reputation_score: 0.0,
            updated_at: Utc::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Decay configuration
// ---------------------------------------------------------------------------

/// Controls how quickly reputation decays when an agent is idle.
#[derive(Debug, Clone)]
pub struct DecayConfig {
    /// Half-life in days — after this many days of inactivity, the reputation
    /// score is halved (applied multiplicatively before the new task contributes).
    pub half_life_days: f64,
    /// Minimum reputation score floor (never decays below this).
    pub floor: f64,
    /// Weight of quality vs. reliability in the composite score.
    pub quality_weight: f64,
    pub reliability_weight: f64,
    pub volume_weight: f64,
    /// Task count at which the volume weight saturates.
    pub volume_saturation: u64,
}

impl Default for DecayConfig {
    fn default() -> Self {
        Self {
            half_life_days: 30.0,
            floor: 0.05,
            quality_weight: 0.45,
            reliability_weight: 0.30,
            volume_weight: 0.25,
            volume_saturation: 100,
        }
    }
}

// ---------------------------------------------------------------------------
// Reputation calculation (pure functions)
// ---------------------------------------------------------------------------

/// Apply exponential decay to the old composite score based on idle time.
///
/// `idle_days` is the number of days since the last metric update.
/// Returns the decayed score, clamped to `[config.floor, 1.0]`.
pub fn apply_decay(old_score: f64, idle_days: f64, config: &DecayConfig) -> f64 {
    if idle_days <= 0.0 {
        return old_score;
    }
    let lambda = std::f64::consts::LN_2 / config.half_life_days;
    let decayed = old_score * (-lambda * idle_days).exp();
    decayed.clamp(config.floor, 1.0)
}

/// Compute the composite reputation score from raw metrics.
///
/// Formula:
///   quality * Qw + reliability * Rw + min(tasks / saturation, 1) * Vw
pub fn compute_reputation(
    tasks_completed: u64,
    avg_quality: f64,
    on_time_rate: f64,
    config: &DecayConfig,
) -> f64 {
    let volume = (tasks_completed as f64 / config.volume_saturation as f64).min(1.0);
    let raw = avg_quality * config.quality_weight
        + on_time_rate * config.reliability_weight
        + volume * config.volume_weight;
    raw.clamp(0.0, 1.0)
}

/// Update a rolling average: `new_avg = old_avg + (new_val - old_avg) / (n + 1)`.
fn update_rolling_avg(old_avg: f64, new_val: f64, n: u64) -> f64 {
    old_avg + (new_val - old_avg) / (n as f64 + 1.0)
}

/// Update a rolling percentage: `new_rate = (old_rate * n + new_val) / (n + 1)`.
fn update_rolling_rate(old_rate: f64, new_val: f64, n: u64) -> f64 {
    (old_rate * n as f64 + new_val) / (n as f64 + 1.0)
}

// ---------------------------------------------------------------------------
// ReputationManager
// ---------------------------------------------------------------------------

/// Thread-safe manager for agent reputation metrics backed by a SQLite pool.
#[derive(Debug, Clone)]
pub struct ReputationManager {
    pool: Pool<SqliteConnectionManager>,
    decay_config: DecayConfig,
}

/// Global singleton for bidirectional reputation sync between modules.
static GLOBAL_REP_MGR: OnceLock<Arc<ReputationManager>> = OnceLock::new();

impl ReputationManager {
    /// Register a global [`ReputationManager`] instance for cross-module access.
    pub fn set_global(mgr: Arc<ReputationManager>) {
        let _ = GLOBAL_REP_MGR.set(mgr);
    }

    /// Retrieve the global [`ReputationManager`] if it has been set.
    pub fn global_opt() -> Option<Arc<ReputationManager>> {
        GLOBAL_REP_MGR.get().cloned()
    }

    /// Create a new manager using the provided r2d2 pool.
    pub fn new(pool: Pool<SqliteConnectionManager>, decay_config: DecayConfig) -> Self {
        Self { pool, decay_config }
    }

    /// Create with default decay config.
    pub fn with_default_config(pool: Pool<SqliteConnectionManager>) -> Self {
        Self::new(pool, DecayConfig::default())
    }

    /// Get a connection from the pool.
    fn conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>, MemoryError> {
        let conn = self.pool.get().map_err(|e| MemoryError::Store(e.to_string()))?;
        conn.execute_batch("PRAGMA foreign_keys=ON; PRAGMA busy_timeout=5000;")
            .map_err(|e| MemoryError::Store(e.to_string()))?;
        Ok(conn)
    }

    // -----------------------------------------------------------------------
    // CRUD
    // -----------------------------------------------------------------------

    /// Upsert agent metrics: creates a new record if the agent doesn't exist,
    /// otherwise updates the existing one.
    pub fn upsert(&self, metrics: &AgentMetrics) -> Result<(), MemoryError> {
        let conn = self.conn()?;
        let expertise_json = serde_json::to_string(&metrics.domain_expertise)
            .map_err(|e| MemoryError::Store(e.to_string()))?;
        conn.execute(
            r"INSERT INTO agent_metrics
               (agent_id, tasks_completed, avg_quality_score, on_time_rate,
                domain_expertise, reputation_score, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
               ON CONFLICT(agent_id) DO UPDATE SET
                 tasks_completed   = excluded.tasks_completed,
                 avg_quality_score = excluded.avg_quality_score,
                 on_time_rate      = excluded.on_time_rate,
                 domain_expertise  = excluded.domain_expertise,
                 reputation_score  = excluded.reputation_score,
                 updated_at        = excluded.updated_at",
            params![
                metrics.agent_id,
                metrics.tasks_completed as i64,
                metrics.avg_quality_score,
                metrics.on_time_rate,
                expertise_json,
                metrics.reputation_score,
                metrics.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|e| MemoryError::Store(e.to_string()))?;
        Ok(())
    }

    /// Record the completion of a single task by an agent.
    ///
    /// Updates rolling averages, applies decay to the old reputation score,
    /// and bumps domain expertise for the given set of domains.
    pub fn record_completion(
        &self,
        agent_id: &str,
        quality_score: f64,
        completed_on_time: bool,
        domains: &[String],
    ) -> Result<AgentMetrics, MemoryError> {
        let conn = self.conn()?;
        let now = Utc::now();

        // Fetch existing record directly using this connection (avoid nested pool get).
        let mut current = {
            let mut stmt = conn
                .prepare(
                    r"SELECT agent_id, tasks_completed, avg_quality_score, on_time_rate,
                              domain_expertise, reputation_score, updated_at
                       FROM agent_metrics WHERE agent_id = ?1",
                )
                .map_err(|e| MemoryError::Store(e.to_string()))?;
            stmt.query_row(params![agent_id], |row| {
                let expertise_str: String = row.get(4)?;
                let expertise: HashMap<String, f64> =
                    serde_json::from_str(&expertise_str).unwrap_or_default();
                let updated_str: String = row.get(6)?;
                let updated = DateTime::parse_from_rfc3339(&updated_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                Ok(AgentMetrics {
                    agent_id: row.get(0)?,
                    tasks_completed: row.get::<_, i64>(1)? as u64,
                    avg_quality_score: row.get(2)?,
                    on_time_rate: row.get(3)?,
                    domain_expertise: expertise,
                    reputation_score: row.get(5)?,
                    updated_at: updated,
                })
            })
            .optional()
            .map_err(|e| MemoryError::Store(e.to_string()))?
            .unwrap_or_else(|| AgentMetrics::new(agent_id))
        };

        // Compute idle time and apply decay to the old composite score.
        let idle_days = (now - current.updated_at).num_milliseconds() as f64
            / (1000.0 * 60.0 * 60.0 * 24.0);
        let decayed_old = apply_decay(current.reputation_score, idle_days, &self.decay_config);

        // Update rolling metrics.
        let n = current.tasks_completed;
        current.avg_quality_score =
            update_rolling_avg(current.avg_quality_score, quality_score, n);
        current.on_time_rate =
            update_rolling_rate(current.on_time_rate, if completed_on_time { 1.0 } else { 0.0 }, n);
        current.tasks_completed = n + 1;

        // Domain expertise: bump each domain by a small increment, bounded.
        for domain in domains {
            let entry = current
                .domain_expertise
                .entry(domain.clone())
                .or_insert(0.0);
            *entry = (*entry + 0.05).min(1.0);
        }

        // Recompute composite score, weighted blend of decayed old and fresh.
        current.reputation_score = compute_reputation(
            current.tasks_completed,
            current.avg_quality_score,
            current.on_time_rate,
            &self.decay_config,
        );
        // Blend with decayed historical score for stability.
        current.reputation_score =
            decayed_old * 0.3 + current.reputation_score * 0.7;

        current.updated_at = now;

        // Persist.
        let expertise_json = serde_json::to_string(&current.domain_expertise)
            .map_err(|e| MemoryError::Store(e.to_string()))?;
        conn.execute(
            r"INSERT INTO agent_metrics
               (agent_id, tasks_completed, avg_quality_score, on_time_rate,
                domain_expertise, reputation_score, updated_at)
               VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
               ON CONFLICT(agent_id) DO UPDATE SET
                 tasks_completed   = excluded.tasks_completed,
                 avg_quality_score = excluded.avg_quality_score,
                 on_time_rate      = excluded.on_time_rate,
                 domain_expertise  = excluded.domain_expertise,
                 reputation_score  = excluded.reputation_score,
                 updated_at        = excluded.updated_at",
            params![
                current.agent_id,
                current.tasks_completed as i64,
                current.avg_quality_score,
                current.on_time_rate,
                expertise_json,
                current.reputation_score,
                current.updated_at.to_rfc3339(),
            ],
        )
        .map_err(|e| MemoryError::Store(e.to_string()))?;

        Ok(current)
    }

    /// Fetch the metrics for a single agent by id.
    pub fn get(&self, agent_id: &str) -> Result<Option<AgentMetrics>, MemoryError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT agent_id, tasks_completed, avg_quality_score, on_time_rate,
                          domain_expertise, reputation_score, updated_at
                   FROM agent_metrics WHERE agent_id = ?1",
            )
            .map_err(|e| MemoryError::Store(e.to_string()))?;

        let result = stmt
            .query_row(params![agent_id], |row| {
                let expertise_str: String = row.get(4)?;
                let expertise: HashMap<String, f64> =
                    serde_json::from_str(&expertise_str).unwrap_or_default();
                let updated_str: String = row.get(6)?;
                let updated = DateTime::parse_from_rfc3339(&updated_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                Ok(AgentMetrics {
                    agent_id: row.get(0)?,
                    tasks_completed: row.get::<_, i64>(1)? as u64,
                    avg_quality_score: row.get(2)?,
                    on_time_rate: row.get(3)?,
                    domain_expertise: expertise,
                    reputation_score: row.get(5)?,
                    updated_at: updated,
                })
            })
            .optional()
            .map_err(|e| MemoryError::Store(e.to_string()))?;

        Ok(result)
    }

    /// List the top-N agents by reputation score, optionally filtered by domain.
    ///
    /// When `domain` is provided, agents are ranked by their expertise in that
    /// domain multiplied by their composite reputation score.
    pub fn list_top_agents(
        &self,
        domain: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AgentMetrics>, MemoryError> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare(
                r"SELECT agent_id, tasks_completed, avg_quality_score, on_time_rate,
                          domain_expertise, reputation_score, updated_at
                   FROM agent_metrics
                   ORDER BY reputation_score DESC
                   LIMIT ?1",
            )
            .map_err(|e| MemoryError::Store(e.to_string()))?;

        let rows = stmt
            .query_map(params![limit as i64], |row| {
                let expertise_str: String = row.get(4)?;
                let expertise: HashMap<String, f64> =
                    serde_json::from_str(&expertise_str).unwrap_or_default();
                let updated_str: String = row.get(6)?;
                let updated = DateTime::parse_from_rfc3339(&updated_str)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now());
                Ok(AgentMetrics {
                    agent_id: row.get(0)?,
                    tasks_completed: row.get::<_, i64>(1)? as u64,
                    avg_quality_score: row.get(2)?,
                    on_time_rate: row.get(3)?,
                    domain_expertise: expertise,
                    reputation_score: row.get(5)?,
                    updated_at: updated,
                })
            })
            .map_err(|e| MemoryError::Store(e.to_string()))?;

        let mut agents: Vec<AgentMetrics> = rows
            .filter_map(|r| r.ok())
            .collect();

        // If a domain filter is requested, re-rank by domain_expertise * reputation.
        if let Some(d) = domain {
            agents.sort_by(|a, b| {
                let score_a = a
                    .domain_expertise
                    .get(d)
                    .copied()
                    .unwrap_or(0.0)
                    * a.reputation_score;
                let score_b = b
                    .domain_expertise
                    .get(d)
                    .copied()
                    .unwrap_or(0.0)
                    * b.reputation_score;
                score_b
                    .partial_cmp(&score_a)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            agents.truncate(limit);
        }

        Ok(agents)
    }

    /// Delete the metrics record for an agent (e.g. on agent decommission).
    pub fn delete(&self, agent_id: &str) -> Result<(), MemoryError> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM agent_metrics WHERE agent_id = ?1",
            params![agent_id],
        )
        .map_err(|e| MemoryError::Store(e.to_string()))?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// DDL — call this from `init_schema()` in sqlite.rs
// ---------------------------------------------------------------------------

/// Returns the CREATE TABLE statement for `agent_metrics`.
/// Callers should execute this during schema initialization.
pub fn schema_ddl() -> &'static str {
    r"CREATE TABLE IF NOT EXISTS agent_metrics (
    agent_id          TEXT    PRIMARY KEY,
    tasks_completed   INTEGER NOT NULL DEFAULT 0,
    avg_quality_score REAL    NOT NULL DEFAULT 0.0,
    on_time_rate      REAL    NOT NULL DEFAULT 0.0,
    domain_expertise  TEXT    NOT NULL DEFAULT '{}',
    reputation_score  REAL    NOT NULL DEFAULT 0.0,
    updated_at        TEXT    NOT NULL
)"
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_pool() -> Pool<SqliteConnectionManager> {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::builder()
            .max_size(1)
            .build(manager)
            .expect("in-memory pool");
        // Create schema
        let conn = pool.get().expect("conn");
        conn.execute_batch(schema_ddl()).expect("ddl");
        pool
    }

    #[test]
    fn record_completion_updates_metrics() {
        let pool = test_pool();
        let mgr = ReputationManager::with_default_config(pool);
        let agent = "test-agent-1";

        let metrics = mgr
            .record_completion(agent, 0.9, true, &["rust".into()])
            .expect("record");
        assert_eq!(metrics.tasks_completed, 1);
        assert!((metrics.avg_quality_score - 0.9).abs() < 0.001);
        assert!((metrics.on_time_rate - 1.0).abs() < 0.001);
        assert!(metrics.reputation_score > 0.4); // volume weight causes low initial score
        assert!((metrics.domain_expertise.get("rust").copied().unwrap_or(0.0) - 0.05).abs() < 0.001);

        // Second task — scores should be rolling averages.
        let m2 = mgr
            .record_completion(agent, 0.5, false, &["python".into()])
            .expect("record2");
        assert_eq!(m2.tasks_completed, 2);
        let expected_quality = 0.9 + (0.5 - 0.9) / 2.0; // rolling avg
        assert!((m2.avg_quality_score - expected_quality).abs() < 0.001);
        assert!((m2.on_time_rate - 0.5).abs() < 0.001);
    }

    #[test]
    fn list_top_agents_respects_limit() {
        let pool = test_pool();
        let mgr = ReputationManager::with_default_config(pool);

        mgr.record_completion("a1", 0.95, true, &["rust".into()])
            .expect("a1");
        mgr.record_completion("a2", 0.7, true, &["rust".into()])
            .expect("a2");
        mgr.record_completion("a3", 0.85, true, &["python".into()])
            .expect("a3");

        let top = mgr.list_top_agents(None, 2).expect("top");
        assert_eq!(top.len(), 2);
        // a1 should rank higher than a2 (higher quality).
        assert_eq!(top[0].agent_id, "a1");
    }

    #[test]
    fn domain_filter_reranks() {
        let pool = test_pool();
        let mgr = ReputationManager::with_default_config(pool);

        mgr.record_completion("a1", 0.9, true, &["rust".into()])
            .expect("a1");
        mgr.record_completion("a2", 0.7, true, &["python".into()])
            .expect("a2");

        // Without domain filter: a1 > a2.
        let top = mgr.list_top_agents(None, 5).expect("top");
        assert_eq!(top[0].agent_id, "a1");

        // With "python" domain: a2 should outrank a1.
        let top_py = mgr.list_top_agents(Some("python"), 5).expect("python");
        assert_eq!(top_py[0].agent_id, "a2");
    }

    #[test]
    fn decay_reduces_score_over_time() {
        let config = DecayConfig {
            half_life_days: 30.0,
            floor: 0.05,
            quality_weight: 0.45,
            reliability_weight: 0.30,
            volume_weight: 0.25,
            volume_saturation: 100,
        };
        let original = 0.9;
        // After one half-life, score should be ~0.45.
        let decayed = apply_decay(original, 30.0, &config);
        assert!((decayed - 0.45).abs() < 0.01);
        // Floor should prevent decay below 0.05.
        let deep_decay = apply_decay(original, 365.0, &config);
        assert!((deep_decay - 0.05).abs() < 0.001);
    }

    #[test]
    fn compute_reputation_formula() {
        let config = DecayConfig::default();
        let score = compute_reputation(50, 0.9, 0.8, &config);
        // volume = 50/100 = 0.5
        // raw = 0.9*0.45 + 0.8*0.30 + 0.5*0.25 = 0.405 + 0.24 + 0.125 = 0.77
        assert!((score - 0.77).abs() < 0.01);
    }

    #[test]
    fn upsert_and_get_roundtrip() {
        let pool = test_pool();
        let mgr = ReputationManager::with_default_config(pool);
        let metrics = AgentMetrics {
            agent_id: "rt-agent".into(),
            tasks_completed: 42,
            avg_quality_score: 0.88,
            on_time_rate: 0.95,
            domain_expertise: {
                let mut m = HashMap::new();
                m.insert("rust".into(), 0.7);
                m
            },
            reputation_score: 0.82,
            updated_at: Utc::now(),
        };
        mgr.upsert(&metrics).expect("upsert");
        let got = mgr.get("rt-agent").expect("get").expect("some");
        assert_eq!(got.agent_id, "rt-agent");
        assert_eq!(got.tasks_completed, 42);
        assert!((got.avg_quality_score - 0.88).abs() < 0.001);
        assert!((got.on_time_rate - 0.95).abs() < 0.001);
        assert!((got.reputation_score - 0.82).abs() < 0.001);
        assert!((got.domain_expertise.get("rust").copied().unwrap_or(0.0) - 0.7).abs() < 0.001);
    }

    #[test]
    fn delete_removes_record() {
        let pool = test_pool();
        let mgr = ReputationManager::with_default_config(pool);
        mgr.record_completion("del-me", 0.5, true, &[]).expect("record");
        assert!(mgr.get("del-me").expect("get").is_some());
        mgr.delete("del-me").expect("delete");
        assert!(mgr.get("del-me").expect("get").is_none());
    }
}
