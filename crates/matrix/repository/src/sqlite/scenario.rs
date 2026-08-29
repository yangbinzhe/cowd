//! SQLite scenario specification, run, and result persistence.

use super::*;

pub(super) fn insert_scenario_spec(
    connection: &Connection,
    spec: &MatrixScenarioSpec,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_scenario_spec (
            scenario_id, source_snapshot_id, transform_ref, spec_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            spec.scenario_id,
            spec.base_snapshot.snapshot_id,
            spec.transform_ref,
            serde_json::to_string(spec)?,
            spec.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn find_scenario_spec(
    connection: &Connection,
    scenario_id: &str,
) -> Result<Option<MatrixScenarioSpec>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT spec_json FROM matrix_scenario_spec WHERE scenario_id = ?1",
            params![scenario_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_scenario_specs(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixScenarioSpec>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        "SELECT spec_json FROM matrix_scenario_spec ORDER BY created_at DESC, scenario_id ASC LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit.max(1) as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn insert_scenario_run(
    connection: &Connection,
    run: &MatrixScenarioRun,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_scenario_run (
            run_id, scenario_id, source_snapshot_id, status, run_json, started_at, completed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            run.run_id,
            run.scenario_id,
            run.base_snapshot.snapshot_id,
            scenario_run_status_name(run.status),
            serde_json::to_string(run)?,
            run.started_at.to_rfc3339(),
            run.completed_at.map(|value| value.to_rfc3339()),
        ],
    )?;
    Ok(())
}

pub(super) fn update_scenario_run(
    connection: &Connection,
    run: &MatrixScenarioRun,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"UPDATE matrix_scenario_run
          SET status = ?2, run_json = ?3, completed_at = ?4
          WHERE run_id = ?1",
        params![
            run.run_id,
            scenario_run_status_name(run.status),
            serde_json::to_string(run)?,
            run.completed_at.map(|value| value.to_rfc3339()),
        ],
    )?;
    Ok(())
}

pub(super) fn find_scenario_run(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<MatrixScenarioRun>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT run_json FROM matrix_scenario_run WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_scenario_runs(
    connection: &Connection,
    scenario_id: Option<&str>,
    limit: usize,
) -> Result<Vec<MatrixScenarioRun>, MatrixSqliteRepositoryError> {
    let (sql, parameter) = match scenario_id {
        Some(scenario_id) => (
            "SELECT run_json FROM matrix_scenario_run WHERE scenario_id = ?1 ORDER BY started_at DESC, run_id ASC LIMIT ?2",
            vec![
                rusqlite::types::Value::Text(scenario_id.to_string()),
                rusqlite::types::Value::Integer(limit.max(1) as i64),
            ],
        ),
        None => (
            "SELECT run_json FROM matrix_scenario_run ORDER BY started_at DESC, run_id ASC LIMIT ?1",
            vec![rusqlite::types::Value::Integer(limit.max(1) as i64)],
        ),
    };
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map(rusqlite::params_from_iter(parameter), |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn insert_scenario_result(
    connection: &Connection,
    result: &MatrixScenarioResult,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_scenario_result (
            result_id, run_id, scenario_id, boundary, result_json, completed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            result.result_id,
            result.run_id,
            result.scenario_id,
            result.boundary,
            serde_json::to_string(result)?,
            result.completed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn find_scenario_result(
    connection: &Connection,
    run_id: &str,
) -> Result<Option<MatrixScenarioResult>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT result_json FROM matrix_scenario_result WHERE run_id = ?1",
            params![run_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

const fn scenario_run_status_name(status: MatrixScenarioRunStatus) -> &'static str {
    match status {
        MatrixScenarioRunStatus::Running => "running",
        MatrixScenarioRunStatus::Succeeded => "succeeded",
        MatrixScenarioRunStatus::Failed => "failed",
        MatrixScenarioRunStatus::Cancelled => "cancelled",
    }
}

pub(super) fn priority_for_compute_job(job: &MatrixComputeJob) -> f32 {
    let metric_score = (job.metric_ids.len() as f32 / 8.0).min(1.0);
    let trigger_score = if job.trigger_fact_type.contains("shortage")
        || job.trigger_fact_type.contains("delivery")
        || job.trigger_fact_type.contains("quality")
    {
        0.9
    } else {
        0.55
    };
    (metric_score * 0.45 + trigger_score * 0.55).min(1.0)
}

pub(super) fn upsert_compute_job(
    connection: &Connection,
    job: &MatrixComputeJob,
) -> Result<MatrixComputeJob, MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_compute_job (
            job_id, trigger_fact_type, status, priority, job_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
        ON CONFLICT(job_id) DO UPDATE SET
            trigger_fact_type = excluded.trigger_fact_type,
            status = excluded.status,
            priority = excluded.priority,
            job_json = excluded.job_json,
            updated_at = excluded.updated_at",
        params![
            job.job_id,
            job.trigger_fact_type,
            job.status,
            job.priority,
            serde_json::to_string(job)?,
            job.created_at.to_rfc3339(),
            job.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(job.clone())
}

pub(super) fn find_compute_job(
    connection: &Connection,
    job_id: &str,
) -> Result<Option<MatrixComputeJob>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT job_json FROM matrix_compute_job WHERE job_id = ?1",
            params![job_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn latest_metric_state(
    connection: &Connection,
    metric_id: &str,
    entity_scope: &str,
    period: &str,
) -> Result<Option<MatrixMetricState>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1 AND entity_scope = ?2 AND period = ?3
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id, entity_scope, period],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn insert_metric_state(
    connection: &Connection,
    state: &MatrixMetricState,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_metric_state (
            state_id, metric_id, entity_scope, period, value, previous_value,
            delta, status, state_json, computed_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
        params![
            state.state_id,
            state.metric_id,
            state.entity_scope,
            state.period,
            state.value,
            state.previous_value,
            state.delta,
            format!("{:?}", state.status).to_ascii_lowercase(),
            serde_json::to_string(state)?,
            state.computed_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn insert_change_event(
    connection: &Connection,
    change: &MatrixChangeEvent,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_change_event (
            change_id, metric_id, entity_ref, period, delta, severity_hint,
            change_json, detected_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            change.change_id,
            change.metric_id,
            change.entity_ref,
            change.period,
            change.delta,
            change.severity_hint,
            serde_json::to_string(change)?,
            change.detected_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn find_change(
    connection: &Connection,
    change_id: &str,
) -> Result<Option<MatrixChangeEvent>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT change_json FROM matrix_change_event WHERE change_id = ?1",
            params![change_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn latest_metric_state_for_metric(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricState>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT state_json
              FROM matrix_metric_state
              WHERE metric_id = ?1
              ORDER BY computed_at DESC
              LIMIT 1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}
