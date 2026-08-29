//! SQLite evidence, quality, impact, and fact query persistence.

use super::*;

pub(super) fn build_impact_trace(
    connection: &Connection,
    root_entity_id: &str,
    max_depth: usize,
) -> Result<MatrixImpactTrace, MatrixSqliteRepositoryError> {
    let max_depth = max_depth.clamp(1, 5);
    let mut queue = VecDeque::from([(root_entity_id.to_string(), 0usize)]);
    let mut seen_entities = BTreeSet::from([root_entity_id.to_string()]);
    let mut seen_relations = BTreeSet::new();
    let mut hops = Vec::new();

    while let Some((entity_id, depth)) = queue.pop_front() {
        if depth >= max_depth {
            continue;
        }
        for relation in list_entity_relations(connection, &entity_id, 500)? {
            if !seen_relations.insert(relation.relation_id.clone()) {
                continue;
            }
            let next_entity_id = if relation.from_entity_id == entity_id {
                relation.to_entity_id.clone()
            } else {
                relation.from_entity_id.clone()
            };
            let traversal_direction = if relation.from_entity_id == entity_id {
                "outbound"
            } else {
                "inbound"
            }
            .to_string();
            let from_entity = find_entity(connection, &relation.from_entity_id)?;
            let to_entity = find_entity(connection, &relation.to_entity_id)?;
            hops.push(MatrixImpactHop {
                depth: depth + 1,
                traversal_direction,
                relation,
                from_entity,
                to_entity,
            });
            if seen_entities.insert(next_entity_id.clone()) {
                queue.push_back((next_entity_id, depth + 1));
            }
        }
    }

    let mut entities = Vec::new();
    for entity_id in &seen_entities {
        if let Some(entity) = find_entity(connection, entity_id)? {
            entities.push(entity);
        }
    }
    Ok(MatrixImpactTrace {
        root_entity_id: root_entity_id.to_string(),
        max_depth,
        entities,
        hops,
        generated_at: Utc::now(),
    })
}

pub(super) fn upsert_attention(
    connection: &Connection,
    item: &MatrixAttentionItem,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_attention_item (
            attention_id, priority_score, status, attention_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            item.attention_id,
            item.priority_score,
            item.status,
            serde_json::to_string(item)?,
            item.created_at.to_rfc3339(),
            item.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn list_attention(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT attention_json
          FROM matrix_attention_item
          ORDER BY priority_score DESC, updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixAttentionItem>(&row?)?))
        .collect()
}

pub(super) fn find_attention(
    connection: &Connection,
    attention_id: &str,
) -> Result<Option<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT attention_json FROM matrix_attention_item WHERE attention_id = ?1",
            params![attention_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn latest_attention(
    connection: &Connection,
) -> Result<Option<MatrixAttentionItem>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT attention_json
              FROM matrix_attention_item
              ORDER BY priority_score DESC, updated_at DESC
              LIMIT 1",
            [],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn insert_evidence_packet(
    connection: &Connection,
    packet: &MatrixEvidencePacket,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_evidence_packet (
            packet_id, attention_id, packet_json, created_at
        ) VALUES (?1, ?2, ?3, ?4)",
        params![
            packet.packet_id,
            packet.attention_id,
            serde_json::to_string(packet)?,
            packet.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn insert_evidence_packet_once(
    connection: &Connection,
    packet: &MatrixEvidencePacket,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR IGNORE INTO matrix_evidence_packet (
            packet_id, attention_id, packet_json, created_at
        ) VALUES (?1, ?2, ?3, ?4)",
        params![
            packet.packet_id,
            packet.attention_id,
            serde_json::to_string(packet)?,
            packet.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn find_evidence_packet(
    connection: &Connection,
    packet_id: &str,
) -> Result<Option<MatrixEvidencePacket>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT packet_json FROM matrix_evidence_packet WHERE packet_id = ?1",
            params![packet_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_evidence_packets(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEvidencePacket>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT packet_json
          FROM matrix_evidence_packet
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixEvidencePacket>(&row?)?))
        .collect()
}

pub(super) fn insert_quality_gate(
    connection: &Connection,
    gate: &MatrixQualityGateDecision,
) -> Result<(), MatrixSqliteRepositoryError> {
    connection.execute(
        r"INSERT OR REPLACE INTO matrix_quality_gate (
            gate_id, target_ref, gate_type, decision, score, gate_json, created_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            gate.gate_id,
            gate.target_ref,
            gate.gate_type,
            gate.decision,
            gate.score,
            serde_json::to_string(gate)?,
            gate.created_at.to_rfc3339(),
        ],
    )?;
    Ok(())
}

pub(super) fn find_quality_gate(
    connection: &Connection,
    gate_id: &str,
) -> Result<Option<MatrixQualityGateDecision>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT gate_json FROM matrix_quality_gate WHERE gate_id = ?1",
            params![gate_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_recent_quality_gates(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixQualityGateDecision>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT gate_json
          FROM matrix_quality_gate
          ORDER BY created_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str(&row?)?)).collect()
}

pub(super) fn list_facts(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixFact>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
            dimensions_json, measures_json, event_time, valid_from, valid_to,
            source_ref, confidence, raw_hash
          FROM matrix_fact
          ORDER BY event_time DESC, fact_id ASC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, Option<String>>(4)?,
            row.get::<_, String>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, String>(7)?,
            row.get::<_, Option<String>>(8)?,
            row.get::<_, Option<String>>(9)?,
            row.get::<_, Option<String>>(10)?,
            row.get::<_, f32>(11)?,
            row.get::<_, String>(12)?,
        ))
    })?;

    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs_json,
            metric_key,
            dimensions_json,
            measures_json,
            event_time,
            valid_from,
            valid_to,
            source_ref,
            confidence,
            raw_hash,
        ) = row?;
        facts.push(MatrixFact {
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs: serde_json::from_str(&entity_refs_json)?,
            metric_key,
            dimensions: serde_json::from_str(&dimensions_json)?,
            measures: serde_json::from_str(&measures_json)?,
            event_time: parse_rfc3339_utc(&event_time)?,
            valid_from: parse_optional_rfc3339_utc(valid_from)?,
            valid_to: parse_optional_rfc3339_utc(valid_to)?,
            source_ref,
            confidence,
            raw_hash,
        });
    }
    Ok(facts)
}

pub(super) fn recall_facts(
    connection: &Connection,
    query: &MatrixRecallQuery,
) -> Result<Vec<MatrixFact>, MatrixSqliteRepositoryError> {
    let snapshot_ids = serde_json::to_string(&query.authorized_snapshot_ids)?;
    let terms = serde_json::to_string(&query.terms)?;
    let mut statement = connection.prepare(
        r"SELECT fact_id, snapshot_id, fact_type, entity_refs_json, metric_key,
            dimensions_json, measures_json, event_time, valid_from, valid_to,
            source_ref, confidence, raw_hash
          FROM matrix_fact
          WHERE snapshot_id IN (SELECT value FROM json_each(?1))
            AND (
              json_array_length(?2) = 0
              OR EXISTS (
                SELECT 1 FROM json_each(?2) AS term
                WHERE LOWER(
                  fact_type || ' ' || COALESCE(metric_key, '') || ' ' ||
                  COALESCE(source_ref, '') || ' ' || dimensions_json || ' ' || measures_json
                ) LIKE '%' || term.value || '%'
              )
            )
          ORDER BY confidence DESC, event_time DESC, fact_id ASC
          LIMIT ?3",
    )?;
    let rows = statement.query_map(
        params![snapshot_ids, terms, query.limit as i64],
        matrix_fact_sql_row,
    )?;
    matrix_facts_from_rows(rows)
}

type MatrixFactSqlRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    f32,
    String,
);

pub(super) fn matrix_fact_sql_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<MatrixFactSqlRow> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
        row.get(6)?,
        row.get(7)?,
        row.get(8)?,
        row.get(9)?,
        row.get(10)?,
        row.get(11)?,
        row.get(12)?,
    ))
}

pub(super) fn matrix_facts_from_rows<F>(
    rows: rusqlite::MappedRows<'_, F>,
) -> Result<Vec<MatrixFact>, MatrixSqliteRepositoryError>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<MatrixFactSqlRow>,
{
    let mut facts = Vec::new();
    for row in rows {
        let (
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs_json,
            metric_key,
            dimensions_json,
            measures_json,
            event_time,
            valid_from,
            valid_to,
            source_ref,
            confidence,
            raw_hash,
        ) = row?;
        facts.push(MatrixFact {
            fact_id,
            snapshot_id,
            fact_type,
            entity_refs: serde_json::from_str(&entity_refs_json)?,
            metric_key,
            dimensions: serde_json::from_str(&dimensions_json)?,
            measures: serde_json::from_str(&measures_json)?,
            event_time: parse_rfc3339_utc(&event_time)?,
            valid_from: parse_optional_rfc3339_utc(valid_from)?,
            valid_to: parse_optional_rfc3339_utc(valid_to)?,
            source_ref,
            confidence,
            raw_hash,
        });
    }
    Ok(facts)
}
