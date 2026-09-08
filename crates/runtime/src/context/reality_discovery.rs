//! Active Reality reads reuse the passive recall owner's lease interpretation.
use super::*;
use fact_kernel::{FactCatalogQuery, FactCatalogSnapshot, FactRecord};
use sha2::{Digest, Sha256};

#[derive(Debug, Clone)]
pub struct RealityDirectoryPage {
    pub records: Vec<FactRecord>,
    pub next_cursor: Option<String>,
}
#[derive(Debug, Clone)]
pub struct RealityExactContent {
    pub source_ref: String,
    pub scope: Option<String>,
    pub source_time: String,
    pub sha256: String,
    pub content: String,
    pub related_read_refs: Vec<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FactCursor {
    version: u8,
    binding: String,
    after_id: String,
    snapshot: FactCatalogSnapshot,
}

impl RealityRecallPort {
    pub async fn discover_facts(
        &self,
        lease: &AgentDataLease,
        query: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<RealityDirectoryPage, String> {
        lease.validate().map_err(|e| e.to_string())?;
        let mut authorization = self.fact_query(lease, query, limit);
        if authorization.terms.is_empty() && !query.trim().is_empty() {
            authorization.terms.push(query.trim().to_lowercase());
        }
        let binding = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(
                    lease,
                    &authorization.authorized_scope_keys,
                    &authorization.terms
                ))
                .map_err(|e| e.to_string())?
            )
        );
        let cursor = cursor
            .map(|value| {
                serde_json::from_str::<FactCursor>(value)
                    .map_err(|_| "invalid Fact directory cursor".to_string())
            })
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|c| c.version != 1 || c.binding != binding || c.after_id.is_empty())
        {
            return Err("Fact directory cursor does not match the current lease or query".into());
        }
        let request = FactCatalogQuery {
            authorization,
            exact_id: None,
            after_id: cursor.as_ref().map(|c| c.after_id.clone()),
            snapshot: cursor.map(|c| c.snapshot),
        };
        let ledger = self.fact_ledger.clone();
        let page = tokio::task::spawn_blocking(move || ledger.catalog_page(&request))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let next_cursor = page
            .next_id
            .map(|after_id| {
                serde_json::to_string(&FactCursor {
                    version: 1,
                    binding,
                    after_id,
                    snapshot: page.snapshot,
                })
                .map_err(|e| e.to_string())
            })
            .transpose()?;
        Ok(RealityDirectoryPage {
            records: page.records,
            next_cursor,
        })
    }

    /// Evidence is reachable only through a currently authorized Fact that cites it.
    pub async fn read_fact(
        &self,
        lease: &AgentDataLease,
        source_ref: &str,
        parent_ref: Option<&str>,
    ) -> Result<RealityExactContent, String> {
        lease.validate().map_err(|e| e.to_string())?;
        let evidence_id = source_ref.strip_prefix("fact:evidence:").map(str::to_owned);
        let fact_ref = if evidence_id.is_some() {
            parent_ref.ok_or("Fact evidence requires its parent_ref")?
        } else {
            if parent_ref.is_some() {
                return Err("parent_ref applies only to Fact evidence".into());
            }
            source_ref
        };
        let id = fact_ref
            .strip_prefix("fact:")
            .filter(|id| !id.is_empty() && !id.starts_with("evidence:"))
            .ok_or("invalid Fact reference")?
            .to_owned();
        let request = FactCatalogQuery {
            authorization: self.fact_query(lease, "", 1),
            exact_id: Some(id),
            after_id: None,
            snapshot: None,
        };
        let ledger = self.fact_ledger.clone();
        let source_ref = source_ref.to_owned();
        tokio::task::spawn_blocking(move || {
            let fact = ledger
                .catalog_page(&request)
                .map_err(|e| e.to_string())?
                .records
                .into_iter()
                .next()
                .ok_or("Fact is unavailable in the current data lease")?;
            let (value, related_read_refs) = if let Some(id) = evidence_id {
                if !fact.evidence.iter().any(|e| e.as_str() == id) {
                    return Err("Fact does not cite this evidence".into());
                }
                let evidence = ledger
                    .get_evidence(&id)
                    .map_err(|e| e.to_string())?
                    .ok_or("Fact evidence is unavailable")?;
                (
                    serde_json::to_value(evidence).map_err(|e| e.to_string())?,
                    Vec::new(),
                )
            } else {
                let refs = fact
                    .evidence
                    .iter()
                    .map(|e| format!("fact:evidence:{}", e.as_str()))
                    .collect();
                (
                    serde_json::to_value(&fact).map_err(|e| e.to_string())?,
                    refs,
                )
            };
            let content = serde_json::to_string(&value).map_err(|e| e.to_string())?;
            let sha256 = format!("sha256:{:x}", Sha256::digest(content.as_bytes()));
            Ok(RealityExactContent {
                source_ref,
                scope: fact.scope_key,
                source_time: fact.updated_at.to_rfc3339(),
                sha256,
                content,
                related_read_refs,
            })
        })
        .await
        .map_err(|e| e.to_string())?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fact_kernel::{EphemeralFactLedger, FactId};
    #[tokio::test]
    async fn fact_discovery_pages_bind_lease_and_mutations_and_exact_reads_never_grant_ids() {
        let root = tempfile::tempdir().unwrap();
        let ledger = Arc::new(EphemeralFactLedger::new());
        let port = RealityRecallPort::with_fact_ledger(root.path(), ledger.clone())
            .with_workspace_scope(root.path());
        let lease = AgentDataLease {
            session_id: "session".into(),
            task_id: "task".into(),
            team_id: None,
            read_scopes: vec![CognitiveReadScope::Session],
            write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
            fact_boundaries: vec!["observed".into()],
            fact_refs: Vec::new(),
            matrix_snapshot_refs: Vec::new(),
        };
        for index in 0..140 {
            let mut fact = FactRecord::new("test", format!("directory needle {index}"));
            fact.id = FactId::from_string(format!("fact-{index:03}"));
            fact.scope_key = Some(FactScope::Session("session".into()).key());
            fact.boundary = RealityBoundary::Observed;
            ledger.upsert_fact(fact).unwrap();
        }
        let mut hidden = FactRecord::new("private", "directory needle hidden");
        hidden.id = FactId::from_string("hidden");
        hidden.scope_key = Some("session:private".into());
        hidden.boundary = RealityBoundary::Observed;
        ledger.upsert_fact(hidden).unwrap();
        let mut page = port
            .discover_facts(&lease, "needle", None, 7)
            .await
            .unwrap();
        let first = page.next_cursor.clone().unwrap();
        let mut seen = std::collections::BTreeSet::new();
        assert!(port
            .discover_facts(&lease, "different", Some(&first), 7)
            .await
            .is_err());
        let mut changed = lease.clone();
        changed.task_id = "other".into();
        assert!(port
            .discover_facts(&changed, "needle", Some(&first), 7)
            .await
            .is_err());
        assert!(port.read_fact(&lease, "fact:hidden", None).await.is_err());
        loop {
            for fact in page.records {
                assert!(seen.insert(fact.id.as_str().to_owned()));
                let exact = port
                    .read_fact(&lease, &format!("fact:{}", fact.id.as_str()), None)
                    .await
                    .unwrap();
                assert_eq!(
                    serde_json::from_str::<Value>(&exact.content).unwrap()["id"],
                    fact.id.as_str()
                );
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            let mut new = FactRecord::new("append", "needle appended");
            new.scope_key = Some("session:session".into());
            new.boundary = RealityBoundary::Observed;
            ledger.upsert_fact(new).unwrap();
            page = port
                .discover_facts(&lease, "needle", Some(&cursor), 7)
                .await
                .unwrap();
        }
        assert_eq!(seen.len(), 140);
        let mut exact_grant = lease.clone();
        exact_grant.fact_boundaries.clear();
        exact_grant.fact_refs = vec!["fact:hidden".into()];
        assert!(port
            .read_fact(&exact_grant, "fact:hidden", None)
            .await
            .is_ok());
        assert!(port
            .read_fact(&exact_grant, "fact:fact-000", None)
            .await
            .is_err());
        let mut fact = ledger.get_fact("fact-000").unwrap().unwrap();
        fact.statement = "changed source".into();
        ledger.upsert_fact(fact).unwrap();
        assert!(port
            .discover_facts(&lease, "needle", Some(&first), 7)
            .await
            .unwrap_err()
            .contains("source changed"));
        let mut empty = lease;
        empty.fact_boundaries.clear();
        assert!(port
            .discover_facts(&empty, "", None, 7)
            .await
            .unwrap()
            .records
            .is_empty());
    }
}

#[derive(Debug, Clone)]
pub struct MatrixDirectoryPage {
    pub records: Vec<matrix_repository::MatrixCatalogRecord>,
    pub next_cursor: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MatrixCursor {
    version: u8,
    binding: String,
    after_ref: String,
    snapshot: matrix_repository::MatrixCatalogSnapshot,
}
impl RealityRecallPort {
    pub async fn discover_matrix(
        &self,
        lease: &AgentDataLease,
        query: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<MatrixDirectoryPage, String> {
        lease.validate().map_err(|e| e.to_string())?;
        let mut authorization =
            MatrixRecallQuery::new(Self::matrix_snapshot_ids(lease), query, limit);
        if authorization.terms.is_empty() && !query.trim().is_empty() {
            authorization.terms.push(query.trim().to_lowercase());
        }
        let binding = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(lease, &authorization.terms)).map_err(|e| e.to_string())?
            )
        );
        let cursor = cursor
            .map(|s| {
                serde_json::from_str::<MatrixCursor>(s)
                    .map_err(|_| "invalid Matrix directory cursor".to_string())
            })
            .transpose()?;
        if cursor
            .as_ref()
            .is_some_and(|c| c.version != 1 || c.binding != binding || c.after_ref.is_empty())
        {
            return Err("Matrix cursor does not match the current lease or query".into());
        }
        let request = matrix_repository::MatrixCatalogQuery {
            authorization,
            exact_ref: None,
            after_ref: cursor.as_ref().map(|c| c.after_ref.clone()),
            snapshot: cursor.map(|c| c.snapshot),
        };
        let store = self.matrix_store()?.clone();
        let page = tokio::task::spawn_blocking(move || store.catalog_page(&request))
            .await
            .map_err(|e| e.to_string())?
            .map_err(|e| e.to_string())?;
        let next_cursor = page
            .next_ref
            .map(|after_ref| {
                serde_json::to_string(&MatrixCursor {
                    version: 1,
                    binding,
                    after_ref,
                    snapshot: page.snapshot,
                })
                .map_err(|e| e.to_string())
            })
            .transpose()?;
        Ok(MatrixDirectoryPage {
            records: page.records,
            next_cursor,
        })
    }
    pub async fn read_matrix(
        &self,
        lease: &AgentDataLease,
        reference: &str,
    ) -> Result<RealityExactContent, String> {
        lease.validate().map_err(|e| e.to_string())?;
        if !reference
            .strip_prefix("matrix:fact:")
            .or_else(|| reference.strip_prefix("matrix:source_snapshot:"))
            .is_some_and(|id| !id.is_empty())
        {
            return Err("Matrix exact reads require a Fact or SourceSnapshot reference".into());
        }
        let request = matrix_repository::MatrixCatalogQuery {
            authorization: MatrixRecallQuery::new(Self::matrix_snapshot_ids(lease), "", 1),
            exact_ref: Some(reference.to_owned()),
            after_ref: None,
            snapshot: None,
        };
        let store = self.matrix_store()?.clone();
        tokio::task::spawn_blocking(move || {
            let record = store
                .catalog_page(&request)
                .map_err(|e| e.to_string())?
                .records
                .into_iter()
                .next()
                .ok_or("Matrix source is unavailable in the current data lease")?;
            let value = record.value().map_err(|e| e.to_string())?;
            let content = serde_json::to_string(&value).map_err(|e| e.to_string())?;
            let related_read_refs = match &record {
                matrix_repository::MatrixCatalogRecord::Fact(fact) => {
                    vec![format!("matrix:source_snapshot:{}", fact.snapshot_id)]
                }
                _ => Vec::new(),
            };
            Ok(RealityExactContent {
                source_ref: record.reference(),
                scope: Some(format!("matrix:source_snapshot:{}", record.snapshot_id())),
                source_time: record.source_time(),
                sha256: format!("sha256:{:x}", Sha256::digest(content.as_bytes())),
                content,
                related_read_refs,
            })
        })
        .await
        .map_err(|e| e.to_string())?
    }
}
