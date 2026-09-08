use super::*;
use fact_kernel::{FactCatalogPage, FactCatalogQuery, FactCatalogSnapshot};

impl PostgresFactLedger {
    pub(crate) fn read_catalog_page(
        &self,
        query: &FactCatalogQuery,
    ) -> FactLedgerResult<FactCatalogPage> {
        let mut connection = self
            .executor
            .checkout_online_read()
            .map_err(storage_error)?;
        let mut transaction = connection.transaction().map_err(postgres_error)?;
        transaction
            .batch_execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
            .map_err(postgres_error)?;
        let keys = query.revision_keys();
        let revisions = transaction.query("SELECT s.key, coalesce(r.revision,0)::BIGINT FROM unnest($1::TEXT[]) AS s(key) LEFT JOIN fact_catalog_invalidations r USING(key) ORDER BY s.key", &[&keys])
            .map_err(postgres_error)?.into_iter().map(|row| (row.get::<_,String>(0),row.get::<_,i64>(1))).collect();
        let snapshot = match &query.snapshot {
            Some(snapshot) if snapshot.revisions == revisions => snapshot.clone(),
            Some(_) => {
                return Err(FactLedgerError::backend(
                    "Fact directory source changed; restart discovery",
                ))
            }
            None => FactCatalogSnapshot {
                fence: transaction
                    .query_one("SELECT pg_current_snapshot()::TEXT", &[])
                    .map_err(postgres_error)?
                    .get(0),
                revisions,
            },
        };
        let auth = &query.authorization;
        let limit = auth.limit.clamp(1, 65);
        let sql_limit = (limit + 1) as i64;
        let mut records: Vec<FactRecord> = transaction.query(
            "SELECT payload FROM fact_records
             WHERE (fact_id=ANY($1) OR (scope_key=ANY($2) AND boundary=ANY($3)))
               AND (cardinality($4::TEXT[])=0 OR EXISTS (SELECT 1 FROM unnest($4::TEXT[]) AS term WHERE strpos(lower(payload->>'statement'),term)>0))
               AND ($5::TEXT IS NULL OR fact_id=$5)
               AND ($6::TEXT IS NULL OR fact_id>$6)
               AND pg_visible_in_snapshot(catalog_creation_xid,$7::TEXT::pg_snapshot)
             ORDER BY fact_id LIMIT $8",
            &[&auth.authorized_fact_ids,&auth.authorized_scope_keys,&auth.authorized_boundaries,&auth.terms,&query.exact_id,&query.after_id,&snapshot.fence,&sql_limit])
            .map_err(postgres_error)?.iter().map(row_json).collect::<FactLedgerResult<_>>()?;
        let more = records.len() > limit;
        records.truncate(limit);
        let next_id = more.then(|| {
            records
                .last()
                .expect("nonempty page")
                .id
                .as_str()
                .to_owned()
        });
        transaction.commit().map_err(postgres_error)?;
        Ok(FactCatalogPage {
            records,
            snapshot,
            next_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[ignore = "requires isolated COWD_TEST_POSTGRES_URL"]
    fn fact_catalog_snapshot_excludes_inflight_and_append_and_invalidates_updates_and_deletes() {
        let url = std::env::var("COWD_TEST_POSTGRES_URL").unwrap();
        let resolver = storage::StaticSecretRefResolver::new([("fact-catalog".into(), url)]);
        let executor = PostgresExecutor::connect(
            PostgresConnectionConfig::new("test", "fact-catalog", "fact-catalog-test"),
            &resolver,
        )
        .unwrap();
        let schema = format!(
            "fact_catalog_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap()
        );
        executor
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!("CREATE SCHEMA {schema}"))
            .unwrap();
        let ledger = PostgresFactLedger::new(executor.scoped_namespace(&schema).unwrap()).unwrap();
        let make = |id: &str, scope: &str| {
            let mut fact = FactRecord::new("test", format!("needle {id}"));
            fact.id = fact_kernel::FactId::from_string(id);
            fact.scope_key = Some(scope.into());
            fact.boundary = harness_contract::reality::RealityBoundary::Observed;
            fact
        };
        for index in 0..140 {
            ledger
                .upsert_fact(make(&format!("fact-{index:03}"), "session:allowed"))
                .unwrap();
        }
        ledger
            .upsert_fact(make("private", "session:private"))
            .unwrap();
        let mut pending_connection = ledger.executor.checkout_critical().unwrap();
        let mut pending = pending_connection.transaction().unwrap();
        upsert_fact_in(&mut pending, make("inflight", "session:allowed")).unwrap();
        let mut request = FactCatalogQuery {
            authorization: FactRecallQuery::new(
                vec![],
                vec!["session:allowed".into()],
                vec!["observed".into()],
                "needle",
                7,
            ),
            exact_id: None,
            after_id: None,
            snapshot: None,
        };
        let mut page = ledger.catalog_page(&request).unwrap();
        let original = page.snapshot.clone();
        pending.commit().unwrap();
        let mut seen = std::collections::BTreeSet::new();
        loop {
            for fact in page.records {
                assert!(seen.insert(fact.id.as_str().to_owned()));
            }
            let Some(after) = page.next_id else {
                break;
            };
            request.after_id = Some(after);
            request.snapshot = Some(page.snapshot);
            ledger
                .upsert_fact(make(&format!("new-{}", seen.len()), "session:allowed"))
                .unwrap();
            page = ledger.catalog_page(&request).unwrap();
        }
        assert_eq!(seen.len(), 140);
        assert!(!seen.contains("inflight"));
        assert!(!seen.contains("private"));
        request.after_id = None;
        request.snapshot = None;
        request.exact_id = Some("private".into());
        assert!(ledger.catalog_page(&request).unwrap().records.is_empty());
        request.exact_id = Some("inflight".into());
        assert_eq!(ledger.catalog_page(&request).unwrap().records.len(), 1);
        request.exact_id = None;
        request.snapshot = Some(original);
        let mut private = ledger.get_fact("private").unwrap().unwrap();
        private.statement = "changed outside lease".into();
        ledger.upsert_fact(private).unwrap();
        ledger.catalog_page(&request).unwrap();
        let mut changed = ledger.get_fact("fact-000").unwrap().unwrap();
        ledger.upsert_fact(changed.clone()).unwrap();
        ledger.catalog_page(&request).unwrap();
        changed.statement = "changed source".into();
        ledger.upsert_fact(changed).unwrap();
        assert!(ledger
            .catalog_page(&request)
            .unwrap_err()
            .to_string()
            .contains("source changed"));
        request.snapshot = None;
        request.snapshot = Some(ledger.catalog_page(&request).unwrap().snapshot);
        ledger
            .executor
            .checkout_critical()
            .unwrap()
            .execute("DELETE FROM fact_records WHERE fact_id='fact-001'", &[])
            .unwrap();
        assert!(ledger.catalog_page(&request).is_err());
        drop(pending_connection);
        drop(ledger);
        executor
            .checkout_critical()
            .unwrap()
            .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
            .unwrap();
    }
}
