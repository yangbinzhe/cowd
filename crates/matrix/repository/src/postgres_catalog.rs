//! Scoped directory queries over the canonical Matrix tables.
use super::*;
use crate::{MatrixCatalogPage, MatrixCatalogQuery, MatrixCatalogRecord, MatrixCatalogSnapshot};

impl PostgresMatrixRepository {
    pub(super) fn catalog_page(
        &self,
        query: &MatrixCatalogQuery,
    ) -> MatrixStoreResult<MatrixCatalogPage> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .map_err(postgres_error)?;
        let authorization = &query.authorization;
        let revisions=transaction.query("SELECT s.snapshot_id, coalesce(r.revision,0)::BIGINT FROM unnest($1::TEXT[]) AS s(snapshot_id) LEFT JOIN matrix_catalog_invalidations r USING(snapshot_id) ORDER BY s.snapshot_id", &[&authorization.authorized_snapshot_ids])
            .map_err(postgres_error)?.into_iter().map(|row| (row.get::<_,String>(0),row.get::<_,i64>(1))).collect();
        let snapshot = match &query.snapshot {
            Some(snapshot) if snapshot.revisions == revisions => snapshot.clone(),
            Some(_) => {
                return Err(MatrixStoreError::Backend(
                    "Matrix directory source changed; restart discovery".into(),
                ))
            }
            None => MatrixCatalogSnapshot {
                fence: transaction
                    .query_one("SELECT pg_current_snapshot()::TEXT", &[])
                    .map_err(postgres_error)?
                    .get(0),
                revisions,
            },
        };
        let limit = authorization.limit.clamp(1, 65);
        let sql_limit = (limit + 1) as i64;
        let mut records=transaction.query(
            "SELECT kind,payload FROM (
                SELECT 'fact' AS kind, 'matrix:fact:'||id AS entry_ref, payload
                FROM matrix_fact WHERE payload->>'snapshot_id'=ANY($1)
                  AND pg_visible_in_snapshot(catalog_creation_xid,$2::TEXT::pg_snapshot)
                UNION ALL
                SELECT 'source_snapshot' AS kind, 'matrix:source_snapshot:'||id AS entry_ref, payload
                FROM matrix_source_snapshot WHERE id=ANY($1)
                  AND pg_visible_in_snapshot(catalog_creation_xid,$2::TEXT::pg_snapshot)
             ) authorized
             WHERE ($3::TEXT IS NULL OR entry_ref>$3)
               AND ($4::TEXT IS NULL OR entry_ref=$4)
               AND (cardinality($5::TEXT[])=0 OR EXISTS (SELECT 1 FROM unnest($5::TEXT[]) term WHERE strpos(lower(payload::TEXT),term)>0))
             ORDER BY entry_ref LIMIT $6",
            &[&authorization.authorized_snapshot_ids,&snapshot.fence,&query.after_ref,&query.exact_ref,&authorization.terms,&sql_limit])
            .map_err(postgres_error)?.into_iter().map(|row| {
                let kind:String=row.get(0);let payload:Value=row.get(1);
                match kind.as_str() {
                    "fact" => serde_json::from_value(payload).map(MatrixCatalogRecord::Fact).map_err(json_error),
                    "source_snapshot" => serde_json::from_value(payload).map(MatrixCatalogRecord::SourceSnapshot).map_err(json_error),
                    _ => Err(MatrixStoreError::Backend("invalid Matrix catalog record kind".into())),
                }
            }).collect::<MatrixStoreResult<Vec<_>>>()?;
        let more = records.len() > limit;
        records.truncate(limit);
        let next_ref = more.then(|| records.last().expect("nonempty page").reference());
        transaction.commit().map_err(postgres_error)?;
        Ok(MatrixCatalogPage {
            records,
            snapshot,
            next_ref,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires isolated COWD_TEST_POSTGRES_URL"]
    fn matrix_catalog_snapshot_pages_all_granted_sources_and_fences_mutations() {
        let resolver = storage::StaticSecretRefResolver::new([(
            "matrix-catalog".into(),
            std::env::var("COWD_TEST_POSTGRES_URL").unwrap(),
        )]);
        let executor = PostgresExecutor::connect(
            PostgresConnectionConfig::new("test", "matrix-catalog", "matrix-catalog-test"),
            &resolver,
        )
        .unwrap();
        let schema = format!("matrix_catalog_{}", uuid::Uuid::new_v4().simple());
        executor
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .unwrap();
        let store =
            PostgresMatrixRepository::new(executor.scoped_namespace(&schema).unwrap()).unwrap();
        let make = |id: &str, snapshot_id: &str| MatrixFact {
            fact_id: id.into(),
            snapshot_id: snapshot_id.into(),
            fact_type: "needle".into(),
            entity_refs: vec![],
            metric_key: None,
            dimensions: serde_json::json!({"region":"east"}),
            measures: serde_json::json!({"value":1}),
            event_time: Utc::now(),
            valid_from: None,
            valid_to: None,
            source_ref: None,
            confidence: 0.9,
            raw_hash: id.into(),
        };
        let mut snapshot = MatrixSourceSnapshot::new("needle", MatrixSourceKind::Manual, "1");
        snapshot.snapshot_id = "allowed".into();
        store.upsert_source_snapshot(snapshot.clone()).unwrap();
        for i in 0..140 {
            store
                .ingest_fact(&make(&format!("fact-{i:03}"), "allowed"))
                .unwrap();
        }
        store.ingest_fact(&make("private", "private")).unwrap();
        let mut connection = store.executor.checkout_critical().unwrap();
        let mut pending = connection.transaction().unwrap();
        write_json(&mut pending, FACT, "inflight", &make("inflight", "allowed")).unwrap();
        let mut request = MatrixCatalogQuery {
            authorization: crate::MatrixRecallQuery::new(
                vec!["allowed".into(), "future".into()],
                "needle",
                7,
            ),
            exact_ref: None,
            after_ref: None,
            snapshot: None,
        };
        let mut page = store.catalog_page(&request).unwrap();
        let original = page.snapshot.clone();
        pending.commit().unwrap();
        let mut future = snapshot.clone();
        future.snapshot_id = "future".into();
        store.upsert_source_snapshot(future).unwrap();
        let mut seen = BTreeSet::new();
        loop {
            for record in page.records {
                assert!(seen.insert(record.reference()));
            }
            let Some(next) = page.next_ref else {
                break;
            };
            request.after_ref = Some(next);
            request.snapshot = Some(page.snapshot);
            store
                .ingest_fact(&make(&format!("new-{}", seen.len()), "allowed"))
                .unwrap();
            page = store.catalog_page(&request).unwrap();
        }
        assert_eq!(seen.len(), 141);
        assert!(seen.contains("matrix:source_snapshot:allowed"));
        assert!(!seen.contains("matrix:source_snapshot:future"));
        assert!(!seen.contains("matrix:fact:inflight"));
        request.after_ref = None;
        request.snapshot = None;
        request.exact_ref = Some("matrix:fact:private".into());
        assert!(store.catalog_page(&request).unwrap().records.is_empty());
        request.exact_ref = Some("matrix:fact:inflight".into());
        assert_eq!(store.catalog_page(&request).unwrap().records.len(), 1);
        request.exact_ref = None;
        request.snapshot = Some(original);
        store.ingest_fact(&make("private", "private")).unwrap();
        store.catalog_page(&request).unwrap();
        store.upsert_source_snapshot(snapshot.clone()).unwrap();
        store.catalog_page(&request).unwrap();
        snapshot.metadata = serde_json::json!({"changed":true});
        store.upsert_source_snapshot(snapshot).unwrap();
        assert!(store
            .catalog_page(&request)
            .unwrap_err()
            .to_string()
            .contains("source changed"));
        request.snapshot = None;
        request.snapshot = Some(store.catalog_page(&request).unwrap().snapshot);
        store.ingest_fact(&make("fact-000", "allowed")).unwrap();
        assert!(store.catalog_page(&request).is_err());
        request.snapshot = None;
        request.snapshot = Some(store.catalog_page(&request).unwrap().snapshot);
        store
            .executor
            .checkout_critical()
            .unwrap()
            .execute("DELETE FROM matrix_fact WHERE id='fact-001'", &[])
            .unwrap();
        assert!(store.catalog_page(&request).is_err());
        drop(connection);
        drop(store);
        executor
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
    }
}
