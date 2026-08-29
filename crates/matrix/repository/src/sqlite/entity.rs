//! SQLite entity and relationship persistence.

use super::*;

pub(super) fn upsert_entity(
    connection: &Connection,
    entity: &MatrixEntity,
) -> Result<MatrixEntity, MatrixSqliteRepositoryError> {
    let mut entity = entity.clone();
    if let Some(existing) =
        find_entity_by_canonical(connection, &entity.entity_type, &entity.canonical_key)?
    {
        entity.entity_id = existing.entity_id;
        entity.created_at = existing.created_at;
        entity.source_keys = merged_source_keys(&existing.source_keys, &entity.source_keys);
    }
    entity.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO matrix_entity (
            entity_id, entity_type, canonical_key, display_name, source_keys_json,
            attributes_json, confidence, entity_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
        ON CONFLICT(entity_id) DO UPDATE SET
            entity_type = excluded.entity_type,
            canonical_key = excluded.canonical_key,
            display_name = excluded.display_name,
            source_keys_json = excluded.source_keys_json,
            attributes_json = excluded.attributes_json,
            confidence = excluded.confidence,
            entity_json = excluded.entity_json,
            updated_at = excluded.updated_at",
        params![
            entity.entity_id,
            entity.entity_type,
            entity.canonical_key,
            entity.display_name,
            serde_json::to_string(&entity.source_keys)?,
            serde_json::to_string(&entity.attributes)?,
            entity.confidence,
            serde_json::to_string(&entity)?,
            entity.created_at.to_rfc3339(),
            entity.updated_at.to_rfc3339(),
        ],
    )?;
    connection.execute(
        "DELETE FROM matrix_entity_source_key WHERE entity_id = ?1",
        params![entity.entity_id],
    )?;
    for source_key in &entity.source_keys {
        connection.execute(
            r"INSERT INTO matrix_entity_source_key (
                source_system, source_key, entity_id, source_ref, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(source_system, source_key) DO UPDATE SET
                entity_id = excluded.entity_id,
                source_ref = excluded.source_ref",
            params![
                source_key.normalized_system(),
                source_key.normalized_key(),
                entity.entity_id,
                source_key.source_ref,
                Utc::now().to_rfc3339(),
            ],
        )?;
    }
    Ok(entity)
}

pub(super) fn merged_source_keys(
    existing: &[MatrixSourceKey],
    incoming: &[MatrixSourceKey],
) -> Vec<MatrixSourceKey> {
    let mut seen = BTreeSet::new();
    let mut keys = Vec::new();
    for source_key in existing.iter().chain(incoming.iter()) {
        let key = (source_key.normalized_system(), source_key.normalized_key());
        if seen.insert(key) {
            keys.push(source_key.clone());
        }
    }
    keys
}

pub(super) fn find_entity(
    connection: &Connection,
    entity_id: &str,
) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            "SELECT entity_json FROM matrix_entity WHERE entity_id = ?1",
            params![entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn find_entity_by_canonical(
    connection: &Connection,
    entity_type: &str,
    canonical_key: &str,
) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT entity_json
              FROM matrix_entity
              WHERE entity_type = ?1 AND canonical_key = ?2",
            params![entity_type, canonical_key],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn find_entity_by_source_key(
    connection: &Connection,
    source_system: &str,
    source_key: &str,
) -> Result<Option<MatrixEntity>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT e.entity_json
              FROM matrix_entity_source_key s
              JOIN matrix_entity e ON e.entity_id = s.entity_id
              WHERE s.source_system = ?1 AND s.source_key = ?2",
            params![
                matrix_core::normalize_key(source_system),
                matrix_core::normalize_key(source_key),
            ],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_entities(
    connection: &Connection,
    limit: usize,
) -> Result<Vec<MatrixEntity>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT entity_json
          FROM matrix_entity
          ORDER BY updated_at DESC
          LIMIT ?1",
    )?;
    let rows = statement.query_map(params![limit as i64], |row| row.get::<_, String>(0))?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixEntity>(&row?)?))
        .collect()
}

pub(super) fn upsert_relation(
    connection: &Connection,
    relation: &MatrixRelation,
) -> Result<MatrixRelation, MatrixSqliteRepositoryError> {
    if find_entity(connection, &relation.from_entity_id)?.is_none() {
        return Err(MatrixSqliteRepositoryError::NotFound(
            relation.from_entity_id.clone(),
        ));
    }
    if find_entity(connection, &relation.to_entity_id)?.is_none() {
        return Err(MatrixSqliteRepositoryError::NotFound(
            relation.to_entity_id.clone(),
        ));
    }

    let mut relation = relation.clone();
    if let Some(existing) = find_relation_by_key(
        connection,
        &relation.relation_type,
        &relation.from_entity_id,
        &relation.to_entity_id,
    )? {
        relation.relation_id = existing.relation_id;
        relation.created_at = existing.created_at;
    }
    relation.updated_at = Utc::now();
    connection.execute(
        r"INSERT INTO matrix_relation (
            relation_id, relation_type, from_entity_id, to_entity_id, attributes_json,
            confidence, relation_json, created_at, updated_at
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(relation_id) DO UPDATE SET
            relation_type = excluded.relation_type,
            from_entity_id = excluded.from_entity_id,
            to_entity_id = excluded.to_entity_id,
            attributes_json = excluded.attributes_json,
            confidence = excluded.confidence,
            relation_json = excluded.relation_json,
            updated_at = excluded.updated_at",
        params![
            relation.relation_id,
            relation.relation_type,
            relation.from_entity_id,
            relation.to_entity_id,
            serde_json::to_string(&relation.attributes)?,
            relation.confidence,
            serde_json::to_string(&relation)?,
            relation.created_at.to_rfc3339(),
            relation.updated_at.to_rfc3339(),
        ],
    )?;
    Ok(relation)
}

pub(super) fn find_relation_by_key(
    connection: &Connection,
    relation_type: &str,
    from_entity_id: &str,
    to_entity_id: &str,
) -> Result<Option<MatrixRelation>, MatrixSqliteRepositoryError> {
    connection
        .query_row(
            r"SELECT relation_json
              FROM matrix_relation
              WHERE relation_type = ?1 AND from_entity_id = ?2 AND to_entity_id = ?3",
            params![relation_type, from_entity_id, to_entity_id],
            |row| row.get::<_, String>(0),
        )
        .optional()?
        .map(|json| serde_json::from_str(&json).map_err(MatrixSqliteRepositoryError::from))
        .transpose()
}

pub(super) fn list_entity_relations(
    connection: &Connection,
    entity_id: &str,
    limit: usize,
) -> Result<Vec<MatrixRelation>, MatrixSqliteRepositoryError> {
    let mut statement = connection.prepare(
        r"SELECT relation_json
          FROM matrix_relation
          WHERE from_entity_id = ?1 OR to_entity_id = ?1
          ORDER BY updated_at DESC
          LIMIT ?2",
    )?;
    let rows = statement.query_map(params![entity_id, limit as i64], |row| {
        row.get::<_, String>(0)
    })?;
    rows.map(|row| Ok(serde_json::from_str::<MatrixRelation>(&row?)?))
        .collect()
}
