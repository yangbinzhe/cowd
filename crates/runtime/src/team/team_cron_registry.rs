#![allow(clippy::must_use_candidate)]
//! In-memory registries for Team and Cron lifecycle management.
//!
//! Provides TeamCreate/Delete and CronCreate/Delete/List runtime backing
//! to replace the stub implementations in the tools crate.
//!
//! P1-5 enhancement: 4 schedule formats (Relative/Interval/Cron/Timestamp),
//! grace window, inactivity timeout, next_run_at computation, async scheduler loop.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Timelike, Utc};
use serde::{Deserialize, Serialize};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Team {
    pub team_id: String,
    pub name: String,
    pub task_ids: Vec<String>,
    pub status: TeamStatus,
    pub created_at: u64,
    pub updated_at: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TeamStatus {
    Created,
    Running,
    Completed,
    Deleted,
}

impl std::fmt::Display for TeamStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "created"),
            Self::Running => write!(f, "running"),
            Self::Completed => write!(f, "completed"),
            Self::Deleted => write!(f, "deleted"),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TeamRegistry {
    inner: Arc<Mutex<TeamInner>>,
}

#[derive(Debug, Default)]
struct TeamInner {
    teams: HashMap<String, Team>,
    counter: u64,
}

impl TeamRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, name: &str, task_ids: Vec<String>) -> Team {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("team registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.counter += 1;
        let ts = now_secs();
        let team_id = format!("team_{:08x}_{}", ts, inner.counter);
        let team = Team {
            team_id: team_id.clone(),
            name: name.to_owned(),
            task_ids,
            status: TeamStatus::Created,
            created_at: ts,
            updated_at: ts,
        };
        inner.teams.insert(team_id, team.clone());
        team
    }

    pub fn get(&self, team_id: &str) -> Option<Team> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("team registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.teams.get(team_id).cloned()
    }

    pub fn list(&self) -> Vec<Team> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("team registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.teams.values().cloned().collect()
    }

    pub fn delete(&self, team_id: &str) -> Result<Team, String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("team registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let team = inner
            .teams
            .get_mut(team_id)
            .ok_or_else(|| format!("team not found: {team_id}"))?;
        team.status = TeamStatus::Deleted;
        team.updated_at = now_secs();
        Ok(team.clone())
    }

    pub fn remove(&self, team_id: &str) -> Option<Team> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("team registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.teams.remove(team_id)
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("team registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.teams.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronEntry {
    pub cron_id: String,
    pub schedule: String,
    pub prompt: String,
    pub description: Option<String>,
    pub enabled: bool,
    pub created_at: u64,
    pub updated_at: u64,
    pub last_run_at: Option<u64>,
    pub run_count: u64,
}

#[derive(Debug, Clone, Default)]
pub struct CronRegistry {
    inner: Arc<Mutex<CronInner>>,
}

#[derive(Debug, Default)]
struct CronInner {
    entries: HashMap<String, CronEntry>,
    counter: u64,
}

impl CronRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn create(&self, schedule: &str, prompt: &str, description: Option<&str>) -> CronEntry {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.counter += 1;
        let ts = now_secs();
        let cron_id = format!("cron_{:08x}_{}", ts, inner.counter);
        let entry = CronEntry {
            cron_id: cron_id.clone(),
            schedule: schedule.to_owned(),
            prompt: prompt.to_owned(),
            description: description.map(str::to_owned),
            enabled: true,
            created_at: ts,
            updated_at: ts,
            last_run_at: None,
            run_count: 0,
        };
        inner.entries.insert(cron_id, entry.clone());
        entry
    }

    pub fn get(&self, cron_id: &str) -> Option<CronEntry> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.entries.get(cron_id).cloned()
    }

    pub fn list(&self, enabled_only: bool) -> Vec<CronEntry> {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner
            .entries
            .values()
            .filter(|e| !enabled_only || e.enabled)
            .cloned()
            .collect()
    }

    pub fn delete(&self, cron_id: &str) -> Result<CronEntry, String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner
            .entries
            .remove(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))
    }

    /// Disable a cron entry without removing it.
    pub fn disable(&self, cron_id: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let entry = inner
            .entries
            .get_mut(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))?;
        entry.enabled = false;
        entry.updated_at = now_secs();
        Ok(())
    }

    /// Record a cron run.
    pub fn record_run(&self, cron_id: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let entry = inner
            .entries
            .get_mut(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))?;
        entry.last_run_at = Some(now_secs());
        entry.run_count += 1;
        entry.updated_at = now_secs();
        Ok(())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        inner.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Enable a cron entry (resume).
    pub fn enable(&self, cron_id: &str) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let entry = inner
            .entries
            .get_mut(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))?;
        entry.enabled = true;
        entry.updated_at = now_secs();
        Ok(())
    }

    /// Update an existing cron entry's schedule or prompt.
    pub fn update(
        &self,
        cron_id: &str,
        schedule: Option<&str>,
        prompt: Option<&str>,
    ) -> Result<CronEntry, String> {
        let mut inner = self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("cron registry lock poisoned; recovering");
            poisoned.into_inner()
        });
        let entry = inner
            .entries
            .get_mut(cron_id)
            .ok_or_else(|| format!("cron not found: {cron_id}"))?;
        if let Some(s) = schedule {
            entry.schedule = s.to_owned();
        }
        if let Some(p) = prompt {
            entry.prompt = p.to_owned();
        }
        entry.updated_at = now_secs();
        Ok(entry.clone())
    }
}

// ── P1-5: Enhanced Cron Scheduler ────────────────────────────────────────────

/// Flexible schedule format (borrowed from hermes scheduler).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", content = "value")]
pub enum ScheduleFormat {
    /// "30m" - one-shot, 30 minutes from now
    Relative(String),
    /// "every 2h" - recurring interval
    Interval(String),
    /// "0 9 * * *" - standard 5-field cron expression
    Cron(String),
    /// ISO8601 timestamp for exact execution time
    Timestamp(String),
}

impl std::fmt::Display for ScheduleFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Relative(s) => write!(f, "{}", s),
            Self::Interval(s) => write!(f, "every {}", s),
            Self::Cron(s) => write!(f, "{}", s),
            Self::Timestamp(s) => write!(f, "{}", s),
        }
    }
}

/// Enhanced cron job with grace window and next_run_at (P1-5).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    pub id: String,
    pub name: String,
    pub schedule: ScheduleFormat,
    pub prompt: String,
    pub enabled: bool,
    pub last_run_at: Option<String>, // ISO8601
    pub next_run_at: Option<String>, // ISO8601
    pub grace_window_secs: u64,      // Grace window (borrowed from hermes)
    pub run_count: u64,
    pub created_at: String, // ISO8601
    pub updated_at: String, // ISO8601
}

/// Execution status of a cron job run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CronExecutionStatus {
    Success,
    Failed,
    Timeout,
}

/// A single cron job execution log entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronExecutionLog {
    pub id: String,
    pub cron_job_id: String,
    pub cron_job_name: String,
    pub status: CronExecutionStatus,
    pub output: Option<String>,
    pub error: Option<String>,
    pub duration_ms: u64,
    pub triggered_by: String,
    pub started_at: String,
    pub finished_at: String,
}

/// Parse a schedule string into ScheduleFormat.
pub fn parse_schedule(input: &str) -> Result<ScheduleFormat, String> {
    let trimmed = input.trim();

    // Try ISO8601 timestamp first: "2026-04-20T09:00:00Z" or "2026-04-20 09:00:00"
    if trimmed.contains('T')
        || (trimmed.len() >= 16 && trimmed.chars().filter(|c| *c == '-' || *c == ':').count() >= 3)
    {
        if DateTime::parse_from_rfc3339(trimmed).is_ok()
            || chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S").is_ok()
            || chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S").is_ok()
        {
            return Ok(ScheduleFormat::Timestamp(trimmed.to_string()));
        }
    }

    // "every Xh/Xm/Xs" → Interval
    let lower = trimmed.to_lowercase();
    if lower.starts_with("every ") {
        let dur_str = trimmed.strip_prefix("every ").unwrap_or("").trim();
        if parse_duration_secs(dur_str).is_some() {
            return Ok(ScheduleFormat::Interval(dur_str.to_string()));
        }
        return Err(format!(
            "Invalid interval format: '{}'. Use e.g. 'every 2h', 'every 30m'",
            dur_str
        ));
    }

    // Standard cron: 5 fields separated by spaces
    let fields: Vec<&str> = trimmed.split_whitespace().collect();
    if fields.len() == 5 {
        // Basic validation: each field should be a valid cron token
        let valid_chars = fields.iter().all(|f| {
            f.chars().all(|c| {
                c.is_ascii_digit() || c == '*' || c == ',' || c == '-' || c == '/' || c == '?'
            })
        });
        if valid_chars {
            return Ok(ScheduleFormat::Cron(trimmed.to_string()));
        }
    }

    // Relative: "30m", "2h", "1d" → one-shot
    if parse_duration_secs(trimmed).is_some() {
        return Ok(ScheduleFormat::Relative(trimmed.to_string()));
    }

    Err(format!(
        "Cannot parse schedule: '{}'. Use: '30m' (relative), 'every 2h' (interval), '0 9 * * *' (cron), or ISO timestamp",
        trimmed
    ))
}

/// Parse a human duration string ("30m", "2h", "1d", "90s") into seconds.
pub fn parse_duration_secs(input: &str) -> Option<u64> {
    let input = input.trim();
    if input.is_empty() {
        return None;
    }

    // Try number+unit pairs: "2h30m", "1d12h"
    let mut total: u64 = 0;
    let mut num_buf = String::new();
    let mut parsed_something = false;

    for ch in input.chars() {
        if ch.is_ascii_digit() {
            num_buf.push(ch);
        } else {
            let num: u64 = num_buf.parse().ok()?;
            num_buf.clear();
            match ch {
                's' => total += num,
                'm' => total += num * 60,
                'h' => total += num * 3600,
                'd' => total += num * 86400,
                'w' => total += num * 86400 * 7,
                _ => return None,
            }
            parsed_something = true;
        }
    }

    if parsed_something && num_buf.is_empty() {
        Some(total)
    } else {
        None
    }
}

/// Compute the next run time for a schedule, starting from `from`.
pub fn compute_next_run(schedule: &ScheduleFormat, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
    match schedule {
        ScheduleFormat::Relative(dur_str) => {
            // One-shot: from + duration
            let secs = parse_duration_secs(dur_str)?;
            Some(from + ChronoDuration::seconds(secs as i64))
        }
        ScheduleFormat::Interval(dur_str) => {
            // Recurring: from + interval
            let secs = parse_duration_secs(dur_str)?;
            Some(from + ChronoDuration::seconds(secs as i64))
        }
        ScheduleFormat::Cron(expr) => {
            // Simplified cron: parse the 5-field expression
            compute_next_cron(expr, from)
        }
        ScheduleFormat::Timestamp(ts_str) => {
            // Exact timestamp
            if let Ok(dt) = ts_str.parse::<DateTime<Utc>>() {
                if dt > from {
                    Some(dt)
                } else {
                    None
                }
            } else if let Ok(ndt) =
                chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%dT%H:%M:%S")
            {
                let dt = DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc);
                if dt > from {
                    Some(dt)
                } else {
                    None
                }
            } else {
                None
            }
        }
    }
}

/// Simplified cron next-run computation.
/// Supports: minute hour day-of-month month day-of-week
/// Each field: *, specific values, ranges (1-5), steps (*/5)
fn compute_next_cron(expr: &str, from: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let fields: Vec<&str> = expr.split_whitespace().collect();
    if fields.len() != 5 {
        return None;
    }

    let minute = cron_field_values(fields[0], 0, 59)?;
    let hour = cron_field_values(fields[1], 0, 23)?;
    let day_of_month = cron_field_values(fields[2], 1, 31)?;
    let month = cron_field_values(fields[3], 1, 12)?;
    let day_of_week = cron_field_values(fields[4], 0, 6)?;

    // Search for next matching time, up to 366 days ahead
    let mut candidate = from + ChronoDuration::minutes(1);
    candidate = candidate.with_second(0)?.with_nanosecond(0)?;

    let limit = from + ChronoDuration::days(366);
    while candidate < limit {
        if !month.contains(&(candidate.month() as u8)) {
            candidate = candidate.with_day(1)? + ChronoDuration::days(31); // Skip to next month
            continue;
        }
        if !day_of_month.contains(&(candidate.day() as u8)) {
            candidate = candidate + ChronoDuration::days(1);
            candidate = candidate.with_hour(0)?.with_minute(0)?;
            continue;
        }
        if !day_of_week.contains(&(candidate.weekday().num_days_from_sunday() as u8)) {
            candidate = candidate + ChronoDuration::days(1);
            candidate = candidate.with_hour(0)?.with_minute(0)?;
            continue;
        }
        if !hour.contains(&(candidate.hour() as u8)) {
            candidate = candidate + ChronoDuration::hours(1);
            candidate = candidate.with_minute(0)?;
            continue;
        }
        if !minute.contains(&(candidate.minute() as u8)) {
            candidate = candidate + ChronoDuration::minutes(1);
            continue;
        }

        // All fields match
        return Some(candidate);
    }

    None
}

/// Expand a cron field (e.g. "*/5", "1-5", "0,30") into a set of matching values.
fn cron_field_values(field: &str, min: u8, max: u8) -> Option<Vec<u8>> {
    let mut values = Vec::new();

    for part in field.split(',') {
        if part == "*" || part == "?" {
            values.extend(min..=max);
        } else if part.contains('/') {
            let parts: Vec<&str> = part.split('/').collect();
            if parts.len() != 2 {
                return None;
            }
            let step: u8 = parts[1].parse().ok()?;
            let base: Vec<u8> = if parts[0] == "*" {
                (min..=max).collect()
            } else {
                cron_field_values(parts[0], min, max)?
            };
            let start = *base.first()?;
            values.extend((start..=max).step_by(step as usize));
        } else if part.contains('-') {
            let parts: Vec<&str> = part.split('-').collect();
            if parts.len() != 2 {
                return None;
            }
            let start: u8 = parts[0].parse().ok()?;
            let end: u8 = parts[1].parse().ok()?;
            values.extend(start..=end);
        } else {
            let val: u8 = part.parse().ok()?;
            values.push(val);
        }
    }

    values.sort();
    values.dedup();
    Some(values)
}

/// Async cron scheduler with persistence.
pub struct CronScheduler {
    jobs: Arc<tokio::sync::RwLock<Vec<CronJob>>>,
    storage_path: PathBuf,
    log_store: Arc<CronLogStore>,
}

impl CronScheduler {
    pub fn new(storage_path: PathBuf) -> Self {
        let jobs = Self::load_from_disk(&storage_path).unwrap_or_default();
        let logs_path = storage_path
            .parent()
            .map(|p| p.join("logs.json"))
            .unwrap_or_else(|| storage_path.with_file_name("logs.json"));
        let log_store = Arc::new(CronLogStore::new(logs_path));
        Self {
            jobs: Arc::new(tokio::sync::RwLock::new(jobs)),
            storage_path,
            log_store,
        }
    }

    /// Get a reference to the cron log store.
    pub fn log_store(&self) -> &Arc<CronLogStore> {
        &self.log_store
    }

    /// Load cron jobs from JSON file.
    fn load_from_disk(path: &PathBuf) -> Result<Vec<CronJob>, String> {
        let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let jobs: Vec<CronJob> = serde_json::from_str(&content).map_err(|e| e.to_string())?;
        Ok(jobs)
    }

    /// Save cron jobs to JSON file.
    async fn save_to_disk(&self) -> Result<(), String> {
        let jobs = self.jobs.read().await;
        let content = serde_json::to_string_pretty(&*jobs).map_err(|e| e.to_string())?;
        // Ensure parent directory exists
        if let Some(parent) = self.storage_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&self.storage_path, content).map_err(|e| e.to_string())
    }

    /// Create a new cron job.
    pub async fn create_job(
        &self,
        name: &str,
        schedule: &str,
        prompt: &str,
        grace_window_secs: u64,
    ) -> Result<CronJob, String> {
        let sched = parse_schedule(schedule)?;
        let now = Utc::now();
        let next_run = compute_next_run(&sched, now);

        let job = CronJob {
            id: format!(
                "cron_{}",
                &uuid::Uuid::new_v4().to_string().replace('-', "")[..12]
            ),
            name: name.to_string(),
            schedule: sched,
            prompt: prompt.to_string(),
            enabled: true,
            last_run_at: None,
            next_run_at: next_run.map(|dt| dt.to_rfc3339()),
            grace_window_secs,
            run_count: 0,
            created_at: now.to_rfc3339(),
            updated_at: now.to_rfc3339(),
        };

        self.jobs.write().await.push(job.clone());
        self.save_to_disk().await?;
        Ok(job)
    }

    /// List all cron jobs.
    pub async fn list_jobs(&self) -> Vec<CronJob> {
        self.jobs.read().await.clone()
    }

    /// Get a single cron job by ID.
    pub async fn get_job(&self, id: &str) -> Option<CronJob> {
        let jobs = self.jobs.read().await;
        jobs.iter().find(|j| j.id == id).cloned()
    }

    /// Delete a cron job.
    pub async fn delete_job(&self, id: &str) -> Result<CronJob, String> {
        let mut jobs = self.jobs.write().await;
        let idx = jobs
            .iter()
            .position(|j| j.id == id)
            .ok_or_else(|| format!("cron not found: {id}"))?;
        let removed = jobs.remove(idx);
        drop(jobs);
        self.save_to_disk().await?;
        Ok(removed)
    }

    /// Pause (disable) a cron job.
    pub async fn pause_job(&self, id: &str) -> Result<CronJob, String> {
        let mut jobs = self.jobs.write().await;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| format!("cron not found: {id}"))?;
        job.enabled = false;
        job.updated_at = Utc::now().to_rfc3339();
        let result = job.clone();
        drop(jobs);
        self.save_to_disk().await?;
        Ok(result)
    }

    /// Resume (enable) a cron job.
    pub async fn resume_job(&self, id: &str) -> Result<CronJob, String> {
        let mut jobs = self.jobs.write().await;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| format!("cron not found: {id}"))?;
        job.enabled = true;
        job.updated_at = Utc::now().to_rfc3339();
        // Recompute next_run_at
        job.next_run_at = compute_next_run(&job.schedule, Utc::now()).map(|dt| dt.to_rfc3339());
        let result = job.clone();
        drop(jobs);
        self.save_to_disk().await?;
        Ok(result)
    }

    /// Record a run (manual or scheduled).
    pub async fn record_run(&self, id: &str) -> Result<CronJob, String> {
        let mut jobs = self.jobs.write().await;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| format!("cron not found: {id}"))?;
        let now = Utc::now();
        job.last_run_at = Some(now.to_rfc3339());
        job.run_count += 1;
        job.updated_at = now.to_rfc3339();
        // Compute next run
        job.next_run_at = compute_next_run(&job.schedule, now).map(|dt| dt.to_rfc3339());
        let result = job.clone();
        drop(jobs);
        self.save_to_disk().await?;
        Ok(result)
    }

    /// Record a run with an execution log entry.
    pub async fn record_run_with_log(
        &self,
        id: &str,
        status: CronExecutionStatus,
        output: Option<String>,
        error: Option<String>,
        duration_ms: u64,
        triggered_by: &str,
    ) -> Result<CronJob, String> {
        // Update job metadata (same as record_run)
        let mut jobs = self.jobs.write().await;
        let job = jobs
            .iter_mut()
            .find(|j| j.id == id)
            .ok_or_else(|| format!("cron not found: {id}"))?;
        let now = Utc::now();
        let started_at = now.to_rfc3339();
        job.last_run_at = Some(started_at.clone());
        job.run_count += 1;
        job.updated_at = started_at.clone();
        job.next_run_at = compute_next_run(&job.schedule, now).map(|dt| dt.to_rfc3339());
        let result = job.clone();
        let job_name = job.name.clone();
        drop(jobs);
        self.save_to_disk().await?;

        // Append execution log
        let log_id = format!("cronlog_{:08x}", rand::random::<u32>());
        // Truncate output/error to 10KB
        let truncated_output = output.map(|o| {
            if o.len() > 10240 {
                o[..10240].to_string()
            } else {
                o
            }
        });
        let truncated_error = error.map(|e| {
            if e.len() > 10240 {
                e[..10240].to_string()
            } else {
                e
            }
        });
        let log = CronExecutionLog {
            id: log_id,
            cron_job_id: id.to_string(),
            cron_job_name: job_name,
            status,
            output: truncated_output,
            error: truncated_error,
            duration_ms,
            triggered_by: triggered_by.to_string(),
            started_at: started_at.clone(),
            finished_at: now.to_rfc3339(),
        };
        self.log_store.append_log(log).await?;

        Ok(result)
    }

    /// Check which jobs are due for execution.
    pub async fn get_due_jobs(&self) -> Vec<CronJob> {
        let now = Utc::now();
        let jobs = self.jobs.read().await;
        jobs.iter()
            .filter(|j| {
                if !j.enabled {
                    return false;
                }
                if let Some(next_str) = &j.next_run_at {
                    if let Ok(next) = next_str.parse::<DateTime<Utc>>() {
                        if now >= next {
                            // Grace window check
                            if let Some(last_str) = &j.last_run_at {
                                if let Ok(last) = last_str.parse::<DateTime<Utc>>() {
                                    if last + ChronoDuration::seconds(j.grace_window_secs as i64)
                                        > now
                                    {
                                        return false;
                                    }
                                }
                            }
                            return true;
                        }
                    }
                }
                false
            })
            .cloned()
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Cron Execution Log Store
// ═══════════════════════════════════════════════════════════════════════

const CRON_LOG_MAX_PER_JOB: usize = 100;

/// Persistent store for cron execution logs.
pub struct CronLogStore {
    logs: Arc<tokio::sync::RwLock<HashMap<String, Vec<CronExecutionLog>>>>,
    storage_path: PathBuf,
}

impl CronLogStore {
    pub fn new(storage_path: PathBuf) -> Self {
        let logs = Self::load_from_disk(&storage_path).unwrap_or_default();
        Self {
            logs: Arc::new(tokio::sync::RwLock::new(logs)),
            storage_path,
        }
    }

    fn load_from_disk(path: &PathBuf) -> Result<HashMap<String, Vec<CronExecutionLog>>, String> {
        let content = std::fs::read_to_string(path).map_err(|e| e.to_string())?;
        let logs: HashMap<String, Vec<CronExecutionLog>> =
            serde_json::from_str(&content).map_err(|e| e.to_string())?;
        Ok(logs)
    }

    async fn save_to_disk(&self) -> Result<(), String> {
        let logs = self.logs.read().await;
        let content = serde_json::to_string_pretty(&*logs).map_err(|e| e.to_string())?;
        if let Some(parent) = self.storage_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&self.storage_path, content).map_err(|e| e.to_string())
    }

    /// Append an execution log entry and persist.
    pub async fn append_log(&self, log: CronExecutionLog) -> Result<(), String> {
        let mut logs = self.logs.write().await;
        let entries = logs.entry(log.cron_job_id.clone()).or_default();
        entries.insert(0, log);
        // Enforce per-job cap
        if entries.len() > CRON_LOG_MAX_PER_JOB {
            entries.truncate(CRON_LOG_MAX_PER_JOB);
        }
        drop(logs);
        self.save_to_disk().await
    }

    /// List logs for a specific cron job with pagination.
    pub async fn list_logs(
        &self,
        cron_job_id: &str,
        limit: usize,
        offset: usize,
    ) -> (Vec<CronExecutionLog>, usize) {
        let logs = self.logs.read().await;
        if let Some(entries) = logs.get(cron_job_id) {
            let total = entries.len();
            let page: Vec<CronExecutionLog> =
                entries.iter().skip(offset).take(limit).cloned().collect();
            (page, total)
        } else {
            (vec![], 0)
        }
    }

    /// List all logs across all jobs with pagination (sorted by started_at desc).
    pub async fn list_all_logs(
        &self,
        limit: usize,
        offset: usize,
    ) -> (Vec<CronExecutionLog>, usize) {
        let logs = self.logs.read().await;
        let mut all: Vec<&CronExecutionLog> = logs.values().flatten().collect();
        all.sort_by(|a, b| b.started_at.cmp(&a.started_at));
        let total = all.len();
        let page: Vec<CronExecutionLog> =
            all.into_iter().skip(offset).take(limit).cloned().collect();
        (page, total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Team tests ──────────────────────────────────────

    #[test]
    fn creates_and_retrieves_team() {
        let registry = TeamRegistry::new();
        let team = registry.create("Alpha Squad", vec!["task_001".into(), "task_002".into()]);
        assert_eq!(team.name, "Alpha Squad");
        assert_eq!(team.task_ids.len(), 2);
        assert_eq!(team.status, TeamStatus::Created);

        let fetched = registry.get(&team.team_id).expect("team should exist");
        assert_eq!(fetched.team_id, team.team_id);
    }

    #[test]
    fn lists_and_deletes_teams() {
        let registry = TeamRegistry::new();
        let t1 = registry.create("Team A", vec![]);
        let t2 = registry.create("Team B", vec![]);

        let all = registry.list();
        assert_eq!(all.len(), 2);

        let deleted = registry.delete(&t1.team_id).expect("delete should succeed");
        assert_eq!(deleted.status, TeamStatus::Deleted);

        // Team is still listable (soft delete)
        let still_there = registry.get(&t1.team_id).unwrap();
        assert_eq!(still_there.status, TeamStatus::Deleted);

        // Hard remove
        registry.remove(&t2.team_id);
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn rejects_missing_team_operations() {
        let registry = TeamRegistry::new();
        assert!(registry.delete("nonexistent").is_err());
        assert!(registry.get("nonexistent").is_none());
    }

    // ── Cron tests ──────────────────────────────────────

    #[test]
    fn creates_and_retrieves_cron() {
        let registry = CronRegistry::new();
        let entry = registry.create("0 * * * *", "Check status", Some("hourly check"));
        assert_eq!(entry.schedule, "0 * * * *");
        assert_eq!(entry.prompt, "Check status");
        assert!(entry.enabled);
        assert_eq!(entry.run_count, 0);
        assert!(entry.last_run_at.is_none());

        let fetched = registry.get(&entry.cron_id).expect("cron should exist");
        assert_eq!(fetched.cron_id, entry.cron_id);
    }

    #[test]
    fn lists_with_enabled_filter() {
        let registry = CronRegistry::new();
        let c1 = registry.create("* * * * *", "Task 1", None);
        let c2 = registry.create("0 * * * *", "Task 2", None);
        registry
            .disable(&c1.cron_id)
            .expect("disable should succeed");

        let all = registry.list(false);
        assert_eq!(all.len(), 2);

        let enabled_only = registry.list(true);
        assert_eq!(enabled_only.len(), 1);
        assert_eq!(enabled_only[0].cron_id, c2.cron_id);
    }

    #[test]
    fn deletes_cron_entry() {
        let registry = CronRegistry::new();
        let entry = registry.create("* * * * *", "To delete", None);
        let deleted = registry
            .delete(&entry.cron_id)
            .expect("delete should succeed");
        assert_eq!(deleted.cron_id, entry.cron_id);
        assert!(registry.get(&entry.cron_id).is_none());
        assert!(registry.is_empty());
    }

    #[test]
    fn records_cron_runs() {
        let registry = CronRegistry::new();
        let entry = registry.create("*/5 * * * *", "Recurring", None);
        registry.record_run(&entry.cron_id).unwrap();
        registry.record_run(&entry.cron_id).unwrap();

        let fetched = registry.get(&entry.cron_id).unwrap();
        assert_eq!(fetched.run_count, 2);
        assert!(fetched.last_run_at.is_some());
    }

    #[test]
    fn rejects_missing_cron_operations() {
        let registry = CronRegistry::new();
        assert!(registry.delete("nonexistent").is_err());
        assert!(registry.disable("nonexistent").is_err());
        assert!(registry.record_run("nonexistent").is_err());
        assert!(registry.get("nonexistent").is_none());
    }

    #[test]
    fn team_status_display_all_variants() {
        // given
        let cases = [
            (TeamStatus::Created, "created"),
            (TeamStatus::Running, "running"),
            (TeamStatus::Completed, "completed"),
            (TeamStatus::Deleted, "deleted"),
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
                ("created".to_string(), "created"),
                ("running".to_string(), "running"),
                ("completed".to_string(), "completed"),
                ("deleted".to_string(), "deleted"),
            ]
        );
    }

    #[test]
    fn new_team_registry_is_empty() {
        // given
        let registry = TeamRegistry::new();

        // when
        let teams = registry.list();

        // then
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(teams.is_empty());
    }

    #[test]
    fn team_remove_nonexistent_returns_none() {
        // given
        let registry = TeamRegistry::new();

        // when
        let removed = registry.remove("missing");

        // then
        assert!(removed.is_none());
    }

    #[test]
    fn team_len_transitions() {
        // given
        let registry = TeamRegistry::new();

        // when
        let alpha = registry.create("Alpha", vec![]);
        let beta = registry.create("Beta", vec![]);
        let after_create = registry.len();
        registry.remove(&alpha.team_id);
        let after_first_remove = registry.len();
        registry.remove(&beta.team_id);

        // then
        assert_eq!(after_create, 2);
        assert_eq!(after_first_remove, 1);
        assert_eq!(registry.len(), 0);
        assert!(registry.is_empty());
    }

    #[test]
    fn cron_list_all_disabled_returns_empty_for_enabled_only() {
        // given
        let registry = CronRegistry::new();
        let first = registry.create("* * * * *", "Task 1", None);
        let second = registry.create("0 * * * *", "Task 2", None);
        registry
            .disable(&first.cron_id)
            .expect("disable should succeed");
        registry
            .disable(&second.cron_id)
            .expect("disable should succeed");

        // when
        let enabled_only = registry.list(true);
        let all_entries = registry.list(false);

        // then
        assert!(enabled_only.is_empty());
        assert_eq!(all_entries.len(), 2);
    }

    #[test]
    fn cron_create_without_description() {
        // given
        let registry = CronRegistry::new();

        // when
        let entry = registry.create("*/15 * * * *", "Check health", None);

        // then
        assert!(entry.cron_id.starts_with("cron_"));
        assert_eq!(entry.description, None);
        assert!(entry.enabled);
        assert_eq!(entry.run_count, 0);
        assert_eq!(entry.last_run_at, None);
    }

    #[test]
    fn new_cron_registry_is_empty() {
        // given
        let registry = CronRegistry::new();

        // when
        let enabled_only = registry.list(true);
        let all_entries = registry.list(false);

        // then
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(enabled_only.is_empty());
        assert!(all_entries.is_empty());
    }

    #[test]
    fn cron_record_run_updates_timestamp_and_counter() {
        // given
        let registry = CronRegistry::new();
        let entry = registry.create("*/5 * * * *", "Recurring", None);

        // when
        registry
            .record_run(&entry.cron_id)
            .expect("first run should succeed");
        registry
            .record_run(&entry.cron_id)
            .expect("second run should succeed");
        let fetched = registry.get(&entry.cron_id).expect("entry should exist");

        // then
        assert_eq!(fetched.run_count, 2);
        assert!(fetched.last_run_at.is_some());
        assert!(fetched.updated_at >= entry.updated_at);
    }

    #[test]
    fn cron_disable_updates_timestamp() {
        // given
        let registry = CronRegistry::new();
        let entry = registry.create("0 0 * * *", "Nightly", None);

        // when
        registry
            .disable(&entry.cron_id)
            .expect("disable should succeed");
        let fetched = registry.get(&entry.cron_id).expect("entry should exist");

        // then
        assert!(!fetched.enabled);
        assert!(fetched.updated_at >= entry.updated_at);
    }
}
