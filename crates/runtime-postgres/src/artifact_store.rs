//! PostgreSQL artifact metadata repository adapter.

use super::*;

/// PostgreSQL compact-tier and metadata adapter for Runtime artifacts.
#[derive(Clone, Debug)]
pub struct PostgresArtifactRepository {
    executor: PostgresExecutor,
    #[cfg(test)]
    pub(crate) body_query_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl PostgresArtifactRepository {
    pub fn new(executor: PostgresExecutor) -> Result<Self, String> {
        validate_clean_domain_catalog(&executor, ARTIFACT_DOMAIN, ARTIFACT_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        executor
            .apply_migrations(ARTIFACT_DOMAIN, ARTIFACT_MIGRATIONS)
            .map_err(|error| error.to_string())?;
        Ok(Self { executor, #[cfg(test)] body_query_count: Default::default() })
    }
}

impl runtime::ArtifactMetadataRepository for PostgresArtifactRepository {
    fn reserve_attempt(
        &self,
        object: &runtime::ArtifactObjectRecord,
        quota: u64,
        token: &str,
        publication_id: &str,
    ) -> Result<runtime::ArtifactObjectRecord, String> {
        if token.is_empty()
            || token.len() > 128
            || !token
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
            || publication_id.is_empty()
            || publication_id.len() > 128
        {
            return Err("invalid bounded artifact attempt".into());
        }
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        tx.query_one("SELECT pg_advisory_xact_lock(hashtextextended(current_schema() || ':artifact-quota',0))", &[]).map_err(|error| error.to_string())?;
        if let Some(row) = tx.query_opt("SELECT token,sha256,generation,publication_id,state FROM artifact_publication_attempts WHERE token=$1", &[&token]).map_err(|error| error.to_string())? {
            let attempt = artifact_attempt_from_row(&row)?;
            if attempt.sha256 != object.sha256 || attempt.publication_id != publication_id || attempt.state != runtime::ArtifactReservationState::Active { return Err("artifact attempt conflict or finalized".into()); }
            let row = tx.query_opt("SELECT sha256,bytes,tier,compact_body,created_at_ms,generation FROM artifact_objects WHERE sha256=$1 AND generation=$2 AND NOT deleting", &[&attempt.sha256,&attempt.generation]).map_err(|error| error.to_string())?.ok_or("artifact attempt generation missing")?;
            return artifact_object_from_row(&row);
        }
        let existing = tx.query_opt("SELECT sha256,bytes,tier,compact_body,created_at_ms,generation,deleting FROM artifact_objects WHERE sha256=$1 FOR UPDATE", &[&object.sha256]).map_err(|error| error.to_string())?;
        let selected = if let Some(row) = existing {
            if row.get::<_, bool>(6) {
                return Err("artifact generation is being collected".into());
            }
            let selected = artifact_object_from_row(&row)?;
            if selected.bytes != object.bytes {
                return Err("artifact object conflict".into());
            }
            selected
        } else {
            let used = artifact_from_i64(
                tx.query_one(
                    "SELECT COALESCE(SUM(bytes),0)::BIGINT FROM artifact_objects",
                    &[],
                )
                .map_err(|error| error.to_string())?
                .get(0),
                "quota",
            )?;
            if object.bytes > quota.saturating_sub(used) {
                return Err("artifact quota exhausted".into());
            }
            tx.execute("INSERT INTO artifact_objects(sha256,bytes,tier,compact_body,created_at_ms,generation) VALUES($1,$2,$3,$4,$5,$6)",
                &[&object.sha256,&artifact_to_i64(object.bytes)?,&artifact_tier_name(&object.tier),&object.compact_body,&artifact_to_i64(object.created_at_ms)?,&object.generation]).map_err(|error| error.to_string())?;
            object.clone()
        };
        tx.execute("INSERT INTO artifact_publication_attempts(token,sha256,generation,publication_id,state) VALUES($1,$2,$3,$4,'active')", &[&token,&object.sha256,&selected.generation,&publication_id]).map_err(|error| error.to_string())?;
        tx.execute(
            "UPDATE artifact_objects SET inflight=inflight+1 WHERE sha256=$1",
            &[&object.sha256],
        )
        .map_err(|error| error.to_string())?;
        tx.commit()
            .map_err(|_| "artifact reservation commit outcome unknown".to_string())?;
        Ok(selected)
    }

    fn reservation(&self, token: &str) -> Result<Option<runtime::ArtifactReservation>, String> {
        self.executor.checkout_online_read().map_err(|error| error.to_string())?.query_opt(
            "SELECT token,sha256,generation,publication_id,state FROM artifact_publication_attempts WHERE token=$1", &[&token]
        ).map_err(|error| error.to_string())?.as_ref().map(artifact_attempt_from_row).transpose()
    }

    fn publish_attempt(
        &self,
        record: &runtime::ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
    ) -> Result<(), String> {
        if matches!(
            record.prepared_origin,
            Some(runtime::PreparedArtifactOrigin::ToolEffectClosure { .. })
        ) {
            return Err("reserved process closure publication".into());
        }
        self.publish_attempt_inner(record, generation, token, pin)
    }

    fn process_closure_record(
        &self,
        artifact_id: &str,
    ) -> Result<Option<runtime::ArtifactRecord>, String> {
        self.executor.checkout_online_read().map_err(|error| error.to_string())?.query_opt(
            "SELECT artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms,record_class,prepared_origin
             FROM artifact_records WHERE artifact_id=$1 AND octet_length(artifact_id)<=1024
             AND octet_length(sha256)<=80 AND octet_length(media_type)<=256 AND octet_length(visibility_scope)<=1024
             AND octet_length(tier)<=32 AND octet_length(record_class)<=32
             AND octet_length(prepared_origin::TEXT)<=8192",
            &[&artifact_id],
        ).map_err(|error| error.to_string())?.map(|row| artifact_record_from_row(&row)).transpose()
    }

    fn process_closure_object(
        &self,
        sha256: &str,
    ) -> Result<Option<runtime::ArtifactObjectRecord>, String> {
        self.executor.checkout_online_read().map_err(|error| error.to_string())?.query_opt(
            "SELECT sha256,bytes,tier,compact_body,created_at_ms,generation FROM artifact_objects
             WHERE sha256=$1 AND bytes BETWEEN 0 AND 32768 AND octet_length(sha256)<=80
             AND octet_length(tier)<=32 AND octet_length(generation)<=128
             AND (compact_body IS NULL OR octet_length(compact_body)<=32768)",
            &[&sha256],
        ).map_err(|error| error.to_string())?.map(|row| artifact_object_from_row(&row)).transpose()
    }

    fn publish_process_closure_attempt(
        &self,
        record: &runtime::ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
        authority: &runtime::ProcessClosurePublicationAuthority,
    ) -> Result<(), String> {
        if !matches!(
            record.prepared_origin,
            Some(runtime::PreparedArtifactOrigin::ToolEffectClosure { .. })
        ) || !authority.authorizes(record)
        {
            return Err("process closure publication authority mismatch".into());
        }
        self.publish_attempt_inner(record, generation, token, pin)
    }

    fn release_attempt(&self, token: &str, sha256: &str, generation: &str) -> Result<bool, String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let object = tx.query_opt("SELECT inflight FROM artifact_objects WHERE sha256=$1 AND generation=$2 FOR UPDATE", &[&sha256,&generation]).map_err(|error| error.to_string())?;
        let Some(row) = tx.query_opt("SELECT token,sha256,generation,publication_id,state FROM artifact_publication_attempts WHERE token=$1 FOR UPDATE", &[&token]).map_err(|error| error.to_string())? else { return Ok(false); };
        let attempt = artifact_attempt_from_row(&row)?;
        if attempt.sha256 != sha256 || attempt.generation != generation {
            return Err("artifact release identity conflict".into());
        }
        if attempt.state != runtime::ArtifactReservationState::Active {
            return Ok(false);
        }
        if object.is_none_or(|row| row.get::<_, i64>(0) <= 0) {
            return Err("artifact release generation missing or underflow".into());
        }
        tx.execute("UPDATE artifact_objects SET inflight=inflight-1,ready=ready OR inflight=1 WHERE sha256=$1 AND generation=$2", &[&sha256,&generation]).map_err(|error| error.to_string())?;
        tx.execute(
            "UPDATE artifact_publication_attempts SET state='released' WHERE token=$1",
            &[&token],
        )
        .map_err(|error| error.to_string())?;
        tx.commit()
            .map_err(|_| "artifact release commit outcome unknown".to_string())?;
        Ok(true)
    }

    fn active_attempts_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<runtime::ArtifactReservation>, String> {
        self.executor.checkout_background().map_err(|error| error.to_string())?.query(
            "SELECT token,sha256,generation,publication_id,state FROM artifact_publication_attempts WHERE state='active' AND ($1::TEXT IS NULL OR token>$1) ORDER BY token LIMIT $2",
            &[&after,&(limit.clamp(1,100) as i64)]
        ).map_err(|error| error.to_string())?.iter().map(artifact_attempt_from_row).collect()
    }

    fn prepared_high_water(&self) -> Result<Option<String>, String> {
        Ok(self.executor.checkout_background().map_err(|error| error.to_string())?.query_opt("SELECT artifact_id FROM artifact_records WHERE record_class='private_prepared' ORDER BY artifact_id DESC LIMIT 1", &[]).map_err(|error| error.to_string())?.map(|row| row.get(0)))
    }

    fn prepared_page(
        &self,
        after: Option<&str>,
        through: &str,
        limit: usize,
    ) -> Result<runtime::PreparedArtifactPage, String> {
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(|error| error.to_string())?;
        let identities = connection.query("SELECT artifact_id,octet_length(artifact_id)+octet_length(sha256)+octet_length(media_type)+octet_length(visibility_scope)+COALESCE(octet_length(prepared_origin::TEXT),0)+512 FROM artifact_records WHERE record_class='private_prepared' AND ($1::TEXT IS NULL OR artifact_id>$1) AND artifact_id<=$2 ORDER BY artifact_id LIMIT $3", &[&after,&through,&(limit.clamp(1,100) as i64 + 1)]).map_err(|error| error.to_string())?;
        let mut page = runtime::PreparedArtifactPage::default();
        let mut bytes = 0usize;
        page.complete = true;
        for row in identities {
            let size: i32 = row.get(1);
            if page.candidates.len() >= limit.clamp(1, 100) || bytes + size as usize > 1024 * 1024 {
                if page.candidates.is_empty() {
                    return Err("prepared metadata exceeds page budget".into());
                }
                page.complete = false;
                break;
            }
            bytes += size as usize;
            let identity: String = row.get(0);
            let row = connection.query_one("SELECT r.artifact_id,r.sha256,r.bytes,r.media_type,r.visibility_scope,r.tier,r.created_at_ms,r.last_access_at_ms,r.record_class,r.prepared_origin,o.generation FROM artifact_records r JOIN artifact_objects o USING(sha256) WHERE r.artifact_id=$1", &[&identity]).map_err(|error| error.to_string())?;
            page.candidates.push(runtime::PreparedArtifactCandidate {
                record: artifact_record_from_row(&row)?,
                generation: row.get(10),
            });
            page.last_examined_id = Some(identity);
        }
        Ok(page)
    }

    fn retire_prepared(
        &self,
        candidate: &runtime::PreparedArtifactCandidate,
    ) -> Result<runtime::PreparedArtifactRetirement, String> {
        let expected = &candidate.record;
        if expected.record_class != runtime::ArtifactRecordClass::PrivatePrepared {
            return Ok(runtime::PreparedArtifactRetirement::Retained);
        }
        let Some(origin) = &expected.prepared_origin else {
            return Ok(runtime::PreparedArtifactRetirement::Retained);
        };
        origin.validate()?;
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        if tx
            .query_opt(
                "SELECT sha256 FROM artifact_objects WHERE sha256=$1 AND generation=$2 FOR UPDATE",
                &[&expected.sha256, &candidate.generation],
            )
            .map_err(|error| error.to_string())?
            .is_none()
        {
            return Ok(runtime::PreparedArtifactRetirement::Retained);
        }
        let Some(row) = tx.query_opt("SELECT artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms,record_class,prepared_origin FROM artifact_records WHERE artifact_id=$1 FOR UPDATE", &[&expected.artifact_id]).map_err(|error| error.to_string())? else { return Ok(runtime::PreparedArtifactRetirement::Missing); };
        let record = artifact_record_from_row(&row)?;
        if record.record_class != runtime::ArtifactRecordClass::PrivatePrepared
            || record.content_reference() != expected.content_reference()
            || record.prepared_origin != expected.prepared_origin
        {
            return Ok(runtime::PreparedArtifactRetirement::Retained);
        }
        let pins = tx
            .query(
                "SELECT owner,until_ms FROM artifact_pins WHERE artifact_id=$1 FOR UPDATE",
                &[&record.artifact_id],
            )
            .map_err(|error| error.to_string())?;
        if pins.len() != 1
            || pins[0].get::<_, String>(0) != runtime::ARTIFACT_PREPARED_PIN_OWNER
            || pins[0].get::<_, i64>(1) != i64::MAX
        {
            return Ok(runtime::PreparedArtifactRetirement::Retained);
        }
        tx.execute(
            "DELETE FROM artifact_records WHERE artifact_id=$1",
            &[&record.artifact_id],
        )
        .map_err(|error| error.to_string())?;
        tx.commit()
            .map_err(|_| "prepared retirement outcome unknown".to_string())?;
        Ok(runtime::PreparedArtifactRetirement::Removed)
    }

    fn adopt_live(&self, expected: &runtime::ArtifactRecord) -> Result<(), String> {
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let row = tx.query_opt("SELECT artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms,record_class,prepared_origin FROM artifact_records WHERE artifact_id=$1 FOR UPDATE", &[&expected.artifact_id]).map_err(|error| error.to_string())?.ok_or("artifact adoption missing")?;
        let record = artifact_record_from_row(&row)?;
        if record.content_reference() != expected.content_reference()
            || record.prepared_origin != expected.prepared_origin
        {
            return Err("artifact adoption identity changed".into());
        }
        if record.record_class == runtime::ArtifactRecordClass::Live {
            return Ok(());
        }
        tx.execute(
            "UPDATE artifact_records SET record_class='live' WHERE artifact_id=$1",
            &[&record.artifact_id],
        )
        .map_err(|error| error.to_string())?;
        tx.commit()
            .map_err(|_| "artifact adoption outcome unknown".to_string())
    }

    fn prepared_readable(
        &self,
        expected: &runtime::ArtifactRecord,
        origin: &runtime::PreparedArtifactOrigin,
    ) -> Result<bool, String> {
        origin.validate()?;
        let closure = matches!(
            origin,
            runtime::PreparedArtifactOrigin::ToolEffectClosure { .. }
        );
        if closure && expected.record_class != runtime::ArtifactRecordClass::PrivatePrepared {
            return Ok(false);
        }
        let origin = serde_json::to_value(origin).map_err(|error| error.to_string())?;
        Ok(self.executor.checkout_online_read().map_err(|error| error.to_string())?.query_one(
            "SELECT EXISTS(SELECT 1 FROM artifact_records r JOIN artifact_objects o USING(sha256) WHERE r.artifact_id=$1 AND r.sha256=$2 AND r.bytes=$3 AND r.media_type=$4 AND r.visibility_scope=$5 AND r.prepared_origin=$6 AND o.ready AND NOT o.deleting AND (NOT $9 OR r.record_class='private_prepared') AND (r.record_class='live' OR EXISTS(SELECT 1 FROM artifact_pins p WHERE p.artifact_id=r.artifact_id AND p.owner=$7 AND p.until_ms=$8)))",
            &[&expected.artifact_id,&expected.sha256,&artifact_to_i64(expected.bytes)?,&expected.media_type,&expected.visibility_scope,&origin,&runtime::ARTIFACT_PREPARED_PIN_OWNER,&i64::MAX,&closure]
        ).map_err(|error| error.to_string())?.get(0))
    }
    fn claim_object_deletion(&self, sha256: &str, generation: &str) -> Result<bool, String> {
        let mut connection = self
            .executor
            .checkout_background()
            .map_err(|_| "artifact checkout failed".to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|_| "artifact transaction failed".to_string())?;
        let Some(row) = tx.query_opt("SELECT ready,inflight FROM artifact_objects WHERE sha256=$1 AND generation=$2 FOR UPDATE", &[&sha256,&generation]).map_err(|_| "artifact deletion lock failed".to_string())? else { return Ok(false); };
        if !row.get::<_, bool>(0) || row.get::<_, i64>(1) != 0 {
            return Ok(false);
        }
        let references: i64 = tx
            .query_one(
                "SELECT COUNT(*) FROM artifact_records WHERE sha256=$1",
                &[&sha256],
            )
            .map_err(|_| "artifact reference check failed".to_string())?
            .get(0);
        if references != 0 {
            return Ok(false);
        }
        tx.execute(
            "UPDATE artifact_objects SET deleting=TRUE WHERE sha256=$1 AND generation=$2",
            &[&sha256, &generation],
        )
        .map_err(|_| "artifact deletion claim failed".to_string())?;
        tx.commit()
            .map_err(|_| "artifact deletion claim outcome unknown".to_string())?;
        Ok(true)
    }

    fn finish_object_deletion(&self, sha256: &str, generation: &str) -> Result<(), String> {
        self.executor.checkout_background().map_err(|_| "artifact checkout failed".to_string())?
            .execute("DELETE FROM artifact_objects WHERE sha256=$1 AND generation=$2 AND deleting AND inflight=0 AND NOT EXISTS(SELECT 1 FROM artifact_records WHERE sha256=$1)", &[&sha256,&generation])
            .map(|_| ()).map_err(|_| "artifact deletion finish failed".to_string())
    }

    fn object(&self, sha256: &str) -> Result<Option<runtime::ArtifactObjectRecord>, String> {
        #[cfg(test)]
        self.body_query_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT sha256, bytes, tier, compact_body, created_at_ms, generation
                 FROM artifact_objects WHERE sha256=$1",
                &[&sha256],
            )
            .map_err(|error| error.to_string())?
            .map(|row| artifact_object_from_row(&row))
            .transpose()
    }

    fn checkpoint_object_metadata(
        &self, sha256: &str,
    ) -> Result<Option<(runtime::ArtifactObjectRecord, String)>, String> {
        let mut connection = self.executor.checkout_online_read().map_err(|error| error.to_string())?;
        connection.query_opt(
            "SELECT sha256, bytes, tier, NULL::bytea, created_at_ms, generation, xmin::text
             FROM artifact_objects WHERE sha256=$1
             AND (tier <> 'compact' OR (compact_body IS NOT NULL AND octet_length(compact_body)=bytes))",
            &[&sha256],
        ).map_err(|error| error.to_string())?.map(|row| {
            Ok((artifact_object_from_row(&row)?, row.get(6)))
        }).transpose()
    }

    fn record(&self, artifact_id: &str) -> Result<Option<runtime::ArtifactRecord>, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        connection
            .query_opt(
                "SELECT artifact_id, sha256, bytes, media_type, visibility_scope, tier,
                        created_at_ms, last_access_at_ms, record_class, prepared_origin
                 FROM artifact_records WHERE artifact_id=$1",
                &[&artifact_id],
            )
            .map_err(|error| error.to_string())?
            .map(|row| artifact_record_from_row(&row))
            .transpose()
    }

    fn catalog_page(
        &self,
        query: runtime::ArtifactCatalogQuery,
    ) -> Result<runtime::ArtifactCatalogPage, String> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(|error| error.to_string())?;
        let mut transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .map_err(|error| error.to_string())?;
        let revisions = transaction.query("SELECT s.scope_key, coalesce(r.revision,0)::BIGINT FROM unnest($1::TEXT[]) AS s(scope_key) LEFT JOIN artifact_catalog_invalidations r USING(scope_key) ORDER BY s.scope_key", &[&query.scopes])
            .map_err(|error| error.to_string())?.into_iter().map(|row| (row.get::<_,String>(0),row.get::<_,i64>(1))).collect::<std::collections::BTreeMap<_,_>>();
        let snapshot = match query.snapshot {
            Some(snapshot) if snapshot.revisions == revisions => snapshot,
            Some(_) => {
                return Err("Artifact catalog source changed; start a fresh discovery".into())
            }
            None => runtime::ArtifactCatalogSnapshot {
                fence: transaction
                    .query_one("SELECT pg_current_snapshot()::TEXT", &[])
                    .map_err(|error| error.to_string())?
                    .get(0),
                revisions,
            },
        };
        let limit = query.limit.clamp(1, 128);
        let sql_limit = (limit + 1) as i64;
        let mut records = transaction.query(
            "SELECT artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms,record_class,prepared_origin
             FROM artifact_records WHERE record_class='live' AND visibility_scope=ANY($1)
               AND pg_visible_in_snapshot(catalog_creation_xid,$2::TEXT::pg_snapshot)
               AND ($3::TEXT IS NULL OR artifact_id > $3)
               AND ($4='' OR strpos(lower(artifact_id||' '||sha256||' '||media_type),lower($4))>0)
             ORDER BY artifact_id LIMIT $5", &[&query.scopes,&snapshot.fence,&query.after_id,&query.query,&sql_limit])
            .map_err(|error| error.to_string())?.iter().map(artifact_record_from_row).collect::<Result<Vec<_>,_>>()?;
        let has_more = records.len() > limit;
        records.truncate(limit);
        let next_id = has_more.then(|| records.last().expect("nonempty page").artifact_id.clone());
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(runtime::ArtifactCatalogPage {
            records,
            snapshot,
            next_id,
        })
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
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|_| "artifact checkout failed".to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|_| "artifact transaction failed".to_string())?;
        let row = tx
            .query_opt(
                "SELECT record_class FROM artifact_records WHERE artifact_id=$1 FOR UPDATE",
                &[&artifact_id],
            )
            .map_err(|_| "artifact record lock failed".to_string())?;
        if row.is_some_and(|row| row.get::<_, String>(0) == "private_prepared") {
            return Err("private prepared retirement requires exact roots".into());
        }
        let pinned: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM artifact_pins WHERE artifact_id=$1 AND until_ms>(extract(epoch FROM clock_timestamp())*1000)::BIGINT)", &[&artifact_id]).map_err(|_| "artifact pin read failed".to_string())?.get(0);
        if pinned {
            return Err("artifact is pinned".into());
        }
        tx.execute(
            "DELETE FROM artifact_records WHERE artifact_id=$1",
            &[&artifact_id],
        )
        .map_err(|_| "artifact record delete failed".to_string())?;
        tx.commit()
            .map_err(|_| "artifact delete outcome unknown".to_string())
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
                "SELECT object.sha256, object.bytes, object.tier, NULL::BYTEA,
                        object.created_at_ms, object.generation
                 FROM artifact_objects object
                 LEFT JOIN artifact_records record ON record.sha256=object.sha256
                 WHERE record.artifact_id IS NULL AND object.ready AND object.inflight=0 AND object.created_at_ms <= $1
                 ORDER BY object.created_at_ms ASC LIMIT $2",
                &[
                    &artifact_to_i64(before_ms)?,
                    &artifact_to_i64(limit.min(100) as u64)?,
                ],
            )
            .map_err(|error| error.to_string())?
            .iter()
            .map(artifact_object_from_row)
            .collect()
    }

    fn pin(&self, artifact_id: &str, owner: &str, until_ms: u64) -> Result<(), String> {
        if owner == runtime::ARTIFACT_PREPARED_PIN_OWNER {
            return Err("reserved prepared protection".into());
        }
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|_| "artifact checkout failed".to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|_| "artifact transaction failed".to_string())?;
        tx.query_opt(
            "SELECT artifact_id FROM artifact_records WHERE artifact_id=$1 FOR UPDATE",
            &[&artifact_id],
        )
        .map_err(|_| "artifact pin lock failed".to_string())?
        .ok_or("artifact record missing")?;
        tx.execute(
            "UPDATE artifact_records SET record_class='live' WHERE artifact_id=$1",
            &[&artifact_id],
        )
        .map_err(|error| error.to_string())?;
        tx.execute("INSERT INTO artifact_pins(artifact_id,owner,until_ms) VALUES($1,$2,$3) ON CONFLICT(artifact_id,owner) DO UPDATE SET until_ms=GREATEST(artifact_pins.until_ms,EXCLUDED.until_ms)", &[&artifact_id,&owner,&artifact_to_i64(until_ms)?]).map_err(|_| "artifact pin failed".to_string())?;
        tx.commit()
            .map_err(|_| "artifact pin commit outcome unknown".to_string())
    }

    fn unpin(&self, artifact_id: &str, owner: &str) -> Result<(), String> {
        if owner == runtime::ARTIFACT_PREPARED_PIN_OWNER {
            return Err("reserved prepared protection".into());
        }
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
        generation: row.get(5),
        sha256: row.get(0),
        bytes: artifact_from_i64(row.get(1), "bytes")?,
        tier: artifact_tier(row.get::<_, String>(2).as_str())?,
        compact_body: row.get(3),
        created_at_ms: artifact_from_i64(row.get(4), "created_at_ms")?,
    })
}

fn artifact_record_from_row(row: &Row) -> Result<runtime::ArtifactRecord, String> {
    Ok(runtime::ArtifactRecord {
        record_class: artifact_class(&row.get::<_, String>(8))?,
        prepared_origin: row
            .get::<_, Option<serde_json::Value>>(9)
            .and_then(|value| serde_json::from_value(value).ok()),
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

fn artifact_class_name(class: runtime::ArtifactRecordClass) -> &'static str {
    match class {
        runtime::ArtifactRecordClass::Live => "live",
        runtime::ArtifactRecordClass::PrivatePrepared => "private_prepared",
    }
}

fn artifact_class(class: &str) -> Result<runtime::ArtifactRecordClass, String> {
    match class {
        "live" => Ok(runtime::ArtifactRecordClass::Live),
        "private_prepared" => Ok(runtime::ArtifactRecordClass::PrivatePrepared),
        _ => Err("unsupported artifact class".into()),
    }
}

fn artifact_attempt_from_row(row: &Row) -> Result<runtime::ArtifactReservation, String> {
    Ok(runtime::ArtifactReservation {
        token: row.get(0),
        sha256: row.get(1),
        generation: row.get(2),
        publication_id: row.get(3),
        state: match row.get::<_, String>(4).as_str() {
            "active" => runtime::ArtifactReservationState::Active,
            "consumed" => runtime::ArtifactReservationState::Consumed,
            "released" => runtime::ArtifactReservationState::Released,
            _ => return Err("unsupported artifact reservation state".into()),
        },
    })
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

impl PostgresArtifactRepository {
    fn publish_attempt_inner(
        &self,
        record: &runtime::ArtifactRecord,
        generation: &str,
        token: &str,
        pin: Option<(&str, u64)>,
    ) -> Result<(), String> {
        if record.record_class == runtime::ArtifactRecordClass::PrivatePrepared {
            record
                .prepared_origin
                .as_ref()
                .ok_or("prepared origin missing")?
                .validate()?;
            if pin
                != Some((
                    runtime::ARTIFACT_PREPARED_PIN_OWNER,
                    runtime::ARTIFACT_PERMANENT_PIN_UNTIL_MS,
                ))
            {
                return Err("prepared protection missing".into());
            }
        }
        let mut connection = self
            .executor
            .checkout_critical()
            .map_err(|error| error.to_string())?;
        let mut tx = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let object = tx.query_opt("SELECT bytes,tier,inflight FROM artifact_objects WHERE sha256=$1 AND generation=$2 AND NOT deleting FOR UPDATE", &[&record.sha256,&generation]).map_err(|error| error.to_string())?.ok_or("artifact generation missing")?;
        if artifact_from_i64(object.get(0), "bytes")? != record.bytes
            || object.get::<_, String>(1) != artifact_tier_name(&record.tier)
        {
            return Err("artifact object identity conflict".into());
        }
        let attempt = tx.query_opt("SELECT token,sha256,generation,publication_id,state FROM artifact_publication_attempts WHERE token=$1 FOR UPDATE", &[&token]).map_err(|error| error.to_string())?.ok_or("artifact attempt missing")?;
        let attempt = artifact_attempt_from_row(&attempt)?;
        if attempt.sha256 != record.sha256
            || attempt.generation != generation
            || attempt.publication_id != record.artifact_id
            || attempt.state == runtime::ArtifactReservationState::Released
        {
            return Err("artifact attempt identity conflict".into());
        }
        let existing = tx.query_opt("SELECT artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms,record_class,prepared_origin FROM artifact_records WHERE artifact_id=$1 FOR UPDATE", &[&record.artifact_id]).map_err(|error| error.to_string())?.as_ref().map(artifact_record_from_row).transpose()?;
        if let Some(existing) = &existing {
            if existing.content_reference() != record.content_reference()
                || existing.prepared_origin != record.prepared_origin
            {
                return Err("artifact publication identity conflict".into());
            }
        } else if attempt.state == runtime::ArtifactReservationState::Consumed {
            return Err("consumed artifact publication missing".into());
        }
        if attempt.state == runtime::ArtifactReservationState::Consumed {
            return Ok(());
        }
        if object.get::<_, i64>(2) <= 0 {
            return Err("artifact reservation underflow".into());
        }
        let origin = record
            .prepared_origin
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| error.to_string())?;
        if existing.is_none() {
            tx.execute("INSERT INTO artifact_records(artifact_id,sha256,bytes,media_type,visibility_scope,tier,created_at_ms,last_access_at_ms,record_class,prepared_origin) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)",
                &[&record.artifact_id,&record.sha256,&artifact_to_i64(record.bytes)?,&record.media_type,&record.visibility_scope,&artifact_tier_name(&record.tier),&artifact_to_i64(record.created_at_ms)?,&artifact_to_i64(record.last_access_at_ms)?,&artifact_class_name(record.record_class),&origin]).map_err(|error| error.to_string())?;
        }
        if let Some((owner, until)) = pin {
            if owner != runtime::ARTIFACT_PREPARED_PIN_OWNER
                || existing.as_ref().is_none_or(|existing| {
                    existing.record_class == runtime::ArtifactRecordClass::PrivatePrepared
                })
            {
                tx.execute("INSERT INTO artifact_pins(artifact_id,owner,until_ms) VALUES($1,$2,$3) ON CONFLICT(artifact_id,owner) DO UPDATE SET until_ms=GREATEST(artifact_pins.until_ms,EXCLUDED.until_ms)", &[&record.artifact_id,&owner,&artifact_to_i64(until)?]).map_err(|error| error.to_string())?;
            }
        }
        tx.execute("UPDATE artifact_objects SET ready=TRUE,inflight=inflight-1 WHERE sha256=$1 AND generation=$2", &[&record.sha256,&generation]).map_err(|error| error.to_string())?;
        tx.execute(
            "UPDATE artifact_publication_attempts SET state='consumed' WHERE token=$1",
            &[&token],
        )
        .map_err(|error| error.to_string())?;
        tx.commit()
            .map_err(|_| "artifact publication commit outcome unknown".to_string())
    }
}
