//! PostgreSQL artifact metadata repository adapter.

use super::*;

/// PostgreSQL compact-tier and metadata adapter for Runtime artifacts.
#[derive(Clone, Debug)]
pub struct PostgresArtifactRepository {
    executor: PostgresExecutor,
}

impl PostgresArtifactRepository {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        executor
            .apply_migrations(ARTIFACT_DOMAIN, ARTIFACT_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor })
    }
}

impl runtime::ArtifactMetadataRepository for PostgresArtifactRepository {
    fn put_object(&self, object: &runtime::ArtifactObjectRecord) -> Result<bool, String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO artifact_objects
                 (sha256, bytes, tier, compact_body, created_at_ms)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT(sha256) DO NOTHING",
                &[
                    &object.sha256,
                    &artifact_to_i64(object.bytes)?,
                    &artifact_tier_name(&object.tier),
                    &object.compact_body,
                    &artifact_to_i64(object.created_at_ms)?,
                ],
            )
            .map(|changed| changed == 1)
            .map_err(|error| error.to_string())
    }

    fn object(&self, sha256: &str) -> Result<Option<runtime::ArtifactObjectRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT sha256, bytes, tier, compact_body, created_at_ms
                 FROM artifact_objects WHERE sha256=$1",
                &[&sha256],
            )
            .map_err(|error| error.to_string())?
            .map(|row| artifact_object_from_row(&row))
            .transpose()
    }

    fn put_record(&self, record: &runtime::ArtifactRecord) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        connection
            .execute(
                "INSERT INTO artifact_records
                 (artifact_id, sha256, bytes, media_type, visibility_scope, tier,
                  created_at_ms, last_access_at_ms)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &record.artifact_id,
                    &record.sha256,
                    &artifact_to_i64(record.bytes)?,
                    &record.media_type,
                    &record.visibility_scope,
                    &artifact_tier_name(&record.tier),
                    &artifact_to_i64(record.created_at_ms)?,
                    &artifact_to_i64(record.last_access_at_ms)?,
                ],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn record(&self, artifact_id: &str) -> Result<Option<runtime::ArtifactRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT artifact_id, sha256, bytes, media_type, visibility_scope, tier,
                        created_at_ms, last_access_at_ms
                 FROM artifact_records WHERE artifact_id=$1",
                &[&artifact_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| artifact_record_from_row(&row))
            .transpose()
    }

    fn touch(&self, artifact_id: &str, at_ms: u64) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "UPDATE artifact_records SET last_access_at_ms=$2 WHERE artifact_id=$1",
                &[&artifact_id, &artifact_to_i64(at_ms)?],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn remove_record(&self, artifact_id: &str) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "DELETE FROM artifact_records WHERE artifact_id=$1",
                &[&artifact_id],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn unreferenced_objects_before(
        &self,
        before_ms: u64,
        limit: usize,
    ) -> Result<Vec<runtime::ArtifactObjectRecord>, String> {
        self.executor
            .checkout_background()
            .map_err(|error| error.to_string())?
            .query(
                "SELECT object.sha256, object.bytes, object.tier, object.compact_body,
                        object.created_at_ms
                 FROM artifact_objects object
                 LEFT JOIN artifact_records record ON record.sha256=object.sha256
                 WHERE record.artifact_id IS NULL AND object.created_at_ms <= $1
                 ORDER BY object.created_at_ms ASC LIMIT $2",
                &[
                    &artifact_to_i64(before_ms)?,
                    &artifact_to_i64(limit as u64)?,
                ],
            )
            .map_err(|error| error.to_string())?
            .iter()
            .map(artifact_object_from_row)
            .collect()
    }

    fn remove_object(&self, sha256: &str) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "DELETE FROM artifact_objects
                 WHERE sha256=$1
                 AND NOT EXISTS (
                    SELECT 1 FROM artifact_records WHERE artifact_records.sha256=$1
                 )",
                &[&sha256],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "INSERT INTO artifact_pins (artifact_id, owner, until_ms)
                 VALUES ($1, $2, $3)
                 ON CONFLICT(artifact_id, owner)
                 DO UPDATE SET until_ms=EXCLUDED.until_ms",
                &[&artifact_id, &owner, &artifact_to_i64(until_ms)?],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String> {
        self.executor
            .checkout_critical()
            .map_err(|error| error.to_string())?
            .execute(
                "DELETE FROM artifact_pins WHERE artifact_id=$1 AND owner=$2",
                &[&artifact_id, &owner],
            )
            .map(|_| ())
            .map_err(|error| error.to_string())
    }

    fn is_pinned(&self, artifact_id: &str, at_ms: u64) -> Result<bool, String> {
        self.executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?
            .query_one(
                "SELECT EXISTS(
                    SELECT 1 FROM artifact_pins
                    WHERE artifact_id=$1 AND until_ms>$2
                 )",
                &[&artifact_id, &artifact_to_i64(at_ms)?],
            )
            .map(|row| row.get(0))
            .map_err(|error| error.to_string())
    }

    fn stats(&self, at_ms: u64) -> Result<runtime::ArtifactStoreStats, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let object_row = connection
            .query_one(
                "SELECT COUNT(*), COALESCE(SUM(bytes), 0)::BIGINT,
                        COALESCE(SUM(CASE WHEN tier='compact' THEN bytes ELSE 0 END), 0)::BIGINT,
                        COALESCE(SUM(CASE WHEN tier='blob' THEN bytes ELSE 0 END), 0)::BIGINT
                 FROM artifact_objects",
                &[],
            )
            .map_err(|error| error.to_string())?;
        let artifacts = connection
            .query_one("SELECT COUNT(*) FROM artifact_records", &[])
            .map_err(|error| error.to_string())?
            .get::<_, i64>(0);
        let pins = connection
            .query_one(
                "SELECT COUNT(*) FROM artifact_pins WHERE until_ms>$1",
                &[&artifact_to_i64(at_ms)?],
            )
            .map_err(|error| error.to_string())?
            .get::<_, i64>(0);
        Ok(runtime::ArtifactStoreStats {
            objects: artifact_from_i64(object_row.get(0), "objects")?,
            artifacts: artifact_from_i64(artifacts, "artifacts")?,
            physical_bytes: artifact_from_i64(object_row.get(1), "physical_bytes")?,
            compact_bytes: artifact_from_i64(object_row.get(2), "compact_bytes")?,
            blob_bytes: artifact_from_i64(object_row.get(3), "blob_bytes")?,
            pins: artifact_from_i64(pins, "pins")?,
        })
    }
}

fn artifact_object_from_row(row: &Row) -> Result<runtime::ArtifactObjectRecord, String> {
    Ok(runtime::ArtifactObjectRecord {
        sha256: row.get(0),
        bytes: artifact_from_i64(row.get(1), "bytes")?,
        tier: artifact_tier(row.get::<_, String>(2).as_str())?,
        compact_body: row.get(3),
        created_at_ms: artifact_from_i64(row.get(4), "created_at_ms")?,
    })
}

fn artifact_record_from_row(row: &Row) -> Result<runtime::ArtifactRecord, String> {
    Ok(runtime::ArtifactRecord {
        artifact_id: row.get(0),
        sha256: row.get(1),
        bytes: artifact_from_i64(row.get(2), "bytes")?,
        media_type: row.get(3),
        visibility_scope: row.get(4),
        tier: artifact_tier(row.get::<_, String>(5).as_str())?,
        created_at_ms: artifact_from_i64(row.get(6), "created_at_ms")?,
        last_access_at_ms: artifact_from_i64(row.get(7), "last_access_at_ms")?,
    })
}

fn artifact_tier_name(tier: &runtime::ArtifactObjectTier) -> &'static str {
    match tier {
        runtime::ArtifactObjectTier::Compact => "compact",
        runtime::ArtifactObjectTier::Blob => "blob",
    }
}

fn artifact_tier(value: &str) -> Result<runtime::ArtifactObjectTier, String> {
    match value {
        "compact" => Ok(runtime::ArtifactObjectTier::Compact),
        "blob" => Ok(runtime::ArtifactObjectTier::Blob),
        value => Err(format!("unknown artifact tier `{value}`")),
    }
}

fn artifact_to_i64(value: u64) -> Result<i64, String> {
    i64::try_from(value).map_err(|_| format!("artifact integer {value} exceeds PostgreSQL BIGINT"))
}

fn artifact_from_i64(value: i64, field: &str) -> Result<u64, String> {
    u64::try_from(value).map_err(|_| format!("artifact field `{field}` is negative"))
}
