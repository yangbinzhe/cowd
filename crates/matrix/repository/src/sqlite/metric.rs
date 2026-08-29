//! SQLite metric definition, lineage, and snapshot persistence.

use super::*;

#[derive(Debug, Clone)]
pub(super) struct MetricSourceRow {
    pub(super) fact_id: String,
    pub(super) fact_type: String,
    pub(super) metric_id: String,
    pub(super) entity_scope: String,
    pub(super) period: String,
    pub(super) measures: Value,
    pub(super) confidence: f32,
}

pub(super) fn metric_source_rows(
    connection: &Connection,
    metric_filter: Option<&BTreeSet<String>>,
    entity_scope: Option<&str>,
    period: Option<&str>,
) -> Result<Vec<MetricSourceRow>, MatrixSqliteRepositoryError> {
    if metric_filter.is_some_and(BTreeSet::is_empty) {
        return Ok(Vec::new());
    }
    let mut conditions = vec!["metric_key IS NOT NULL".to_string()];
    let mut parameters = Vec::new();
    if let Some(metric_filter) = metric_filter {
        conditions.push(format!(
            "metric_key IN ({})",
            std::iter::repeat_n("?", metric_filter.len())
                .collect::<Vec<_>>()
                .join(",")
        ));
        parameters.extend(metric_filter.iter().cloned());
    }
    if let Some(entity_scope) = entity_scope {
        conditions
            .push("COALESCE(json_extract(entity_refs_json, '$[0]'), 'enterprise') = ?".to_string());
        parameters.push(entity_scope.to_string());
    }
    if let Some(period) = period {
        conditions.push(
            "COALESCE(json_extract(dimensions_json, '$.period'), json_extract(dimensions_json, '$.week'), 'current') = ?"
                .to_string(),
        );
        parameters.push(period.to_string());
    }
    let sql = format!(
        r"SELECT fact_id, fact_type, entity_refs_json, metric_key, dimensions_json,
            measures_json, confidence
          FROM matrix_fact
          WHERE {}
          ORDER BY metric_key ASC, event_time ASC, fact_id ASC",
        conditions.join(" AND ")
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(rusqlite::params_from_iter(parameters.iter()), |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, String>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, f32>(6)?,
        ))
    })?;
    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            fact_type,
            entity_refs_json,
            metric_id,
            dimensions_json,
            measures_json,
            confidence,
        ) = row?;
        let entity_refs: Vec<String> = serde_json::from_str(&entity_refs_json)?;
        let dimensions: Value = serde_json::from_str(&dimensions_json)?;
        let measures: Value = serde_json::from_str(&measures_json)?;
        let entity_scope = entity_refs
            .first()
            .cloned()
            .unwrap_or_else(|| "enterprise".to_string());
        let period = dimensions
            .get("period")
            .or_else(|| dimensions.get("week"))
            .and_then(Value::as_str)
            .unwrap_or("current")
            .to_string();
        facts.push(MetricSourceRow {
            fact_id,
            fact_type,
            metric_id,
            entity_scope,
            period,
            measures,
            confidence,
        });
    }
    Ok(facts)
}

pub(super) fn metric_query_results(
    connection: &Connection,
    metric_filter: Option<&BTreeSet<String>>,
    entity_scope: Option<&str>,
    period: Option<&str>,
) -> Result<Vec<MatrixQueryResult>, MatrixSqliteRepositoryError> {
    let metric_ids = match metric_filter {
        Some(filter) => filter.iter().cloned().collect::<Vec<_>>(),
        None => {
            let mut statement = connection.prepare(
                "SELECT DISTINCT metric_key
                 FROM matrix_fact
                 WHERE metric_key IS NOT NULL
                 ORDER BY metric_key ASC",
            )?;
            let metric_ids = statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            metric_ids
        }
    };
    let mut results = Vec::new();
    for metric_id in metric_ids {
        let single_metric = BTreeSet::from([metric_id.clone()]);
        let rows = metric_source_rows(connection, Some(&single_metric), entity_scope, period)?;
        if rows.is_empty() {
            continue;
        }
        let fact_type = rows
            .first()
            .map(|row| row.fact_type.as_str())
            .unwrap_or("operations.metric");
        let mut definition = find_metric_definition(connection, &metric_id)?
            .unwrap_or_else(|| MatrixMetricDefinition::inferred(metric_id.clone(), fact_type));
        if definition.measure == "value"
            && rows
                .iter()
                .all(|row| row.measures.get("value").and_then(Value::as_f64).is_none())
        {
            definition.measure = infer_single_numeric_measure(&metric_id, &rows)?;
        }
        let plan = definition.query_plan();
        plan.validate()
            .map_err(|error| MatrixSqliteRepositoryError::InvalidMetricQuery(error.to_string()))?;
        upsert_metric_definition(connection, &definition)?;
        let inputs = rows
            .into_iter()
            .map(|row| {
                let numerator = explicit_measure(&row.measures, &plan.numerator_measure)?;
                let denominator = plan
                    .denominator_measure
                    .as_deref()
                    .map(|measure| explicit_measure(&row.measures, measure))
                    .transpose()?;
                Ok(MatrixQueryInput {
                    fact_ref: format!("matrix:fact:{}", row.fact_id),
                    fact_type: row.fact_type,
                    metric_id: row.metric_id,
                    entity_scope: row.entity_scope,
                    period: row.period,
                    numerator,
                    denominator,
                    confidence: row.confidence,
                })
            })
            .collect::<Result<Vec<_>, MatrixSqliteRepositoryError>>()?;
        results.extend(
            matrix_core::execute_matrix_query_plan(&plan, inputs).map_err(|error| {
                MatrixSqliteRepositoryError::InvalidMetricQuery(error.to_string())
            })?,
        );
    }
    Ok(results)
}

pub(super) fn explicit_measure(
    measures: &Value,
    measure: &str,
) -> Result<f64, MatrixSqliteRepositoryError> {
    measures
        .get(measure)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .ok_or_else(|| {
            MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
                "measure {measure} is missing or non-numeric"
            ))
        })
}

pub(super) fn infer_single_numeric_measure(
    metric_id: &str,
    rows: &[MetricSourceRow],
) -> Result<String, MatrixSqliteRepositoryError> {
    let mut candidates = BTreeSet::new();
    for row in rows {
        let object = row.measures.as_object().ok_or_else(|| {
            MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
                "metric {metric_id} measures must be an object"
            ))
        })?;
        candidates.extend(
            object
                .iter()
                .filter(|(_, value)| value.as_f64().is_some())
                .map(|(key, _)| key.clone()),
        );
    }
    if candidates.len() != 1 {
        return Err(MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
            "metric {metric_id} must register one explicit measure (found {})",
            candidates.len()
        )));
    }
    candidates.into_iter().next().ok_or_else(|| {
        MatrixSqliteRepositoryError::InvalidMetricQuery(format!(
            "metric {metric_id} has no explicit numeric measure"
        ))
    })
}

pub(super) fn upsert_metric_definition(
    connection: &Connection,
    definition: &MatrixMetricDefinition,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT INTO matrix_metric_definition (
            metric_id, definition_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4)
        ON CONFLICT(metric_id) DO UPDATE SET
            definition_json = excluded.definition_json,
            updated_at = excluded.updated_at",
        params![
            definition.metric_id,
            serde_json::to_string(definition)?,
            definition.created_at.to_rfc3339(),
            definition.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn find_metric_definition(
    connection: &Connection,
    metric_id: &str,
) -> Result<Option<MatrixMetricDefinition>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT definition_json FROM matrix_metric_definition WHERE metric_id = ?1",
            params![metric_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn upsert_metric_dependency(
    connection: &Connection,
    dependency: &MatrixMetricDependency,
) -> Result<MatrixMetricDependency, MatrixSqliteRepositoryError> {
    let mut dependency = dependency.clone();
    if let Some(existing) = find_metric_dependency_by_key(
        connection,
        &dependency.upstream_metric_id,
        &dependency.downstream_metric_id,
        &dependency.dependency_type,
    )? {
        dependency.dependency_id = existing.dependency_id;
        dependency.created_at = existing.created_at;
    }
    dependency.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO matrix_metric_dependency (
            dependency_id, upstream_metric_id, downstream_metric_id, dependency_type,
            confidence, dependency_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
        ON CONFLICT(dependency_id) DO UPDATE SET
            upstream_metric_id = excluded.upstream_metric_id,
            downstream_metric_id = excluded.downstream_metric_id,
            dependency_type = excluded.dependency_type,
            confidence = excluded.confidence,
            dependency_json = excluded.dependency_json,
            updated_at = excluded.updated_at",
        params![
            dependency.dependency_id,
            dependency.upstream_metric_id,
            dependency.downstream_metric_id,
            dependency.dependency_type,
            dependency.confidence,
            serde_json::to_string(&dependency)?,
            dependency.created_at.to_rfc3339(),
            dependency.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(dependency)
}

pub(super) fn find_metric_dependency_by_key(
    connection: &Connection,
    upstream_metric_id: &str,
    downstream_metric_id: &str,
    dependency_type: &str,
) -> Result<Option<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT dependency_json
              FROM matrix_metric_dependency
              WHERE upstream_metric_id = ?1
                AND downstream_metric_id = ?2
                AND dependency_type = ?3",
            params![upstream_metric_id, downstream_metric_id, dependency_type],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_upstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          WHERE downstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn list_downstream_metric_dependencies(
    connection: &Connection,
    metric_id: &str,
) -> Result<Vec<MatrixMetricDependency>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          WHERE upstream_metric_id = ?1
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map(params![metric_id], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn build_metric_lineage(
    connection: &Connection,
    metric_id: &str,
    max_depth: usize,
) -> Result<MatrixMetricLineage, MatrixSqliteRepositoryError> {
    let max_depth = max_depth.clamp(1, 6);
    let upstream_dependencies = list_upstream_metric_dependencies(connection, metric_id)?;
    let downstream_dependencies = list_downstream_metric_dependencies(connection, metric_id)?;
    let mut impacted = BTreeSet::new();
    let mut queue = VecDeque::from([(metric_id.to_string(), 0usize)]);
    while let Some((current_metric_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for dependency in list_downstream_metric_dependencies(connection, &current_metric_id)? {
            if impacted.insert(dependency.downstream_metric_id.clone()) {
                queue.push_back((dependency.downstream_metric_id, depth + 1));
            }
        }
    }
    Ok(MatrixMetricLineage {
        metric_id: metric_id.to_string(),
        upstream_dependencies,
        downstream_dependencies,
        impacted_metric_ids: impacted.into_iter().collect(),
        generated_at: Utc::now(),
    })
}

pub(super) fn metrics_affected_by_fact_type(
    connection: &Connection,
    fact_type: &str,
) -> Result<Vec<String>, MatrixSqliteRepositoryError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT dependency_json
          FROM matrix_metric_dependency
          ORDER BY updated_at DESC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let dependency: MatrixMetricDependency = serde_json::from_str(&row?)?;
        if dependency
            .required_fact_types
            .iter()
            .any(|candidate| candidate == fact_type)
        {
            impacted.insert(dependency.upstream_metric_id.clone());
            impacted.insert(dependency.downstream_metric_id.clone());
            for metric_id in build_metric_lineage(connection, &dependency.downstream_metric_id, 6)?
                .impacted_metric_ids
            {
                impacted.insert(metric_id);
            }
        }
    }
    Ok(impacted.into_iter().collect())
}

pub(super) fn metric_ids_for_fact_type(
    connection: &Connection,
    fact_type: &str,
) -> Result<Vec<String>, MatrixSqliteRepositoryError> {
    let mut impacted = BTreeSet::new();
    let mut statement = connection.prepare(
        r"SELECT definition_json
          FROM matrix_metric_definition
          ORDER BY metric_id ASC",
    )?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let definition: MatrixMetricDefinition = serde_json::from_str(&row?)?;
        if definition.inputs.iter().any(|input| input == fact_type) {
            impacted.insert(definition.metric_id);
        }
    }
    Ok(impacted.into_iter().collect())
}

pub(super) fn build_metric_attention_plan(
    connection: &Connection,
    trigger_fact_type: &str,
    entity_scope: Option<String>,
    period: Option<String>,
    metric_ids: Vec<String>,
    limit: usize,
) -> Result<MatrixMetricAttentionPlan, MatrixSqliteRepositoryError> {
    let limit = limit.clamp(1, 24);
    let mut scores = Vec::new();
    for metric_id in metric_ids {
        let definition = find_metric_definition(connection, &metric_id)?.unwrap_or_else(|| {
            MatrixMetricDefinition::inferred(metric_id.clone(), trigger_fact_type)
        });
        let lineage = build_metric_lineage(connection, &metric_id, 6)?;
        let latest = latest_metric_state_for_metric(connection, &metric_id)?;
        let latest_status = latest
            .as_ref()
            .map(|state| format!("{:?}", state.status).to_ascii_lowercase());
        let latest_delta = latest.as_ref().map(|state| state.delta);
        let score = MatrixMetricAttentionScore::new(
            metric_id.clone(),
            definition.business_priority,
            lineage.impacted_metric_ids.len() + lineage.upstream_dependencies.len(),
            latest_status,
            latest_delta,
        );
        scores.push(score);
    }
    scores.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .business_priority
                    .partial_cmp(&left.business_priority)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });
    scores.truncate(limit);
    let selected_metric_ids = scores
        .iter()
        .map(|score| score.metric_id.clone())
        .collect::<Vec<_>>();
    let compute_jobs = build_metric_compute_jobs(
        trigger_fact_type,
        &selected_metric_ids,
        entity_scope.clone(),
        period.clone(),
    );
    Ok(MatrixMetricAttentionPlan {
        plan_id: format!("metric-attention-plan-{}", uuid::Uuid::new_v4()),
        trigger_fact_type: trigger_fact_type.to_string(),
        entity_scope,
        period,
        limit,
        scored_metrics: scores,
        selected_metric_ids,
        compute_jobs,
        generated_at: Utc::now(),
    })
}

pub(super) fn build_metric_snapshot(
    connection: &Connection,
    metric_ids: Vec<String>,
    scope_ref: Option<String>,
) -> Result<MatrixMetricSnapshot, MatrixSqliteRepositoryError> {
    let mut unique_metric_ids = metric_ids;
    unique_metric_ids.sort();
    unique_metric_ids.dedup();
    let mut items = Vec::new();
    for metric_id in &unique_metric_ids {
        let state = latest_metric_state_for_metric(connection, metric_id)?;
        items.push(MatrixMetricSnapshotItem {
            metric_id: metric_id.clone(),
            state,
        });
    }
    let state_count = items.iter().filter(|item| item.state.is_some()).count();
    Ok(MatrixMetricSnapshot {
        snapshot_id: format!("metric-snapshot-{}", uuid::Uuid::new_v4()),
        scope_ref: scope_ref.unwrap_or_else(|| "global".to_string()),
        metric_ids: unique_metric_ids,
        items,
        created_at: Utc::now(),
        summary: format!("metric states materialized: {state_count}"),
    })
}

pub(super) fn insert_metric_snapshot(
    connection: &Connection,
    snapshot: &MatrixMetricSnapshot,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_metric_snapshot (
            snapshot_id, scope_ref, metric_ids_json, snapshot_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            snapshot.snapshot_id,
            snapshot.scope_ref,
            serde_json::to_string(&snapshot.metric_ids)?,
            serde_json::to_string(snapshot)?,
            snapshot.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}
