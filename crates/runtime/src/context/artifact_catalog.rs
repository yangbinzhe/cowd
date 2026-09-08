//! Metadata discovery is scoped and snapshot-bound; raw bytes remain in the
//! existing ArtifactStore and are read using the canonical immutable selector.
use super::*;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactCatalogSnapshot {
    pub fence: String,
    pub revisions: BTreeMap<String, i64>,
}
#[derive(Debug, Clone)]
pub struct ArtifactCatalogQuery {
    pub scopes: Vec<String>,
    pub query: String,
    pub after_id: Option<String>,
    pub snapshot: Option<ArtifactCatalogSnapshot>,
    pub limit: usize,
}
#[derive(Debug, Clone)]
pub struct ArtifactCatalogPage {
    pub records: Vec<ArtifactRecord>,
    pub snapshot: ArtifactCatalogSnapshot,
    pub next_id: Option<String>,
}
#[derive(Debug, Clone)]
pub struct ArtifactDirectoryPage {
    pub records: Vec<ArtifactRecord>,
    pub next_cursor: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    binding: String,
    after_id: String,
    snapshot: ArtifactCatalogSnapshot,
}

impl ArtifactStore {
    pub async fn discover_page(
        &self,
        authorized_scopes: &[String],
        query: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<ArtifactDirectoryPage, ArtifactError> {
        let mut scopes = authorized_scopes
            .iter()
            .filter(|scope| !scope.is_empty())
            .cloned()
            .collect::<Vec<_>>();
        scopes.sort();
        scopes.dedup();
        if scopes.is_empty() {
            return Err(ArtifactError::Unauthorized);
        }
        let query = query.trim().to_lowercase();
        let binding = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(&scopes, &query))
                    .map_err(|error| ArtifactError::Metadata(error.to_string()))?
            )
        );
        let cursor = cursor
            .map(|value| {
                serde_json::from_str::<Cursor>(value).map_err(|_| {
                    ArtifactError::Metadata("invalid Artifact directory cursor".into())
                })
            })
            .transpose()?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.version != 1 || cursor.binding != binding || cursor.after_id.is_empty()
        }) {
            return Err(ArtifactError::Metadata(
                "Artifact directory cursor does not match scope or query".into(),
            ));
        }
        let request = ArtifactCatalogQuery {
            scopes,
            query,
            after_id: cursor.as_ref().map(|cursor| cursor.after_id.clone()),
            snapshot: cursor.map(|cursor| cursor.snapshot),
            limit: limit.clamp(1, 128),
        };
        let repository = self.inner.repository.clone();
        let page = tokio::task::spawn_blocking(move || repository.catalog_page(request))
            .await
            .map_err(|error| ArtifactError::Blocking(error.to_string()))?
            .map_err(ArtifactError::Metadata)?;
        let next_cursor = page
            .next_id
            .map(|after_id| {
                serde_json::to_string(&Cursor {
                    version: 1,
                    binding,
                    after_id,
                    snapshot: page.snapshot,
                })
                .map_err(|error| ArtifactError::Metadata(error.to_string()))
            })
            .transpose()?;
        Ok(ArtifactDirectoryPage {
            records: page.records,
            next_cursor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn artifact_catalog_pages_survive_own_receipts_and_touch_but_reject_deletion() {
        let root = tempfile::tempdir().unwrap();
        let store = ArtifactStore::for_test_default(root.path());
        let descriptor = ArtifactWriteDescriptor {
            media_type: "text/plain".into(),
            visibility_scope: "session:catalog".into(),
            expected_bytes: None,
            original_name: None,
        };
        let scopes = vec![descriptor.visibility_scope.clone()];
        let mut expected = std::collections::BTreeSet::new();
        for index in 0..140 {
            expected.insert(
                store
                    .write_bytes(descriptor.clone(), format!("source-{index}").as_bytes())
                    .await
                    .unwrap()
                    .selector,
            );
        }
        let hidden = store
            .write_bytes(
                ArtifactWriteDescriptor {
                    visibility_scope: "session:private".into(),
                    ..descriptor.clone()
                },
                b"private",
            )
            .await
            .unwrap();
        let mut page = store
            .discover_page(&scopes, "text/plain", None, 7)
            .await
            .unwrap();
        let first_cursor = page.next_cursor.clone().unwrap();
        assert!(store
            .discover_page(
                &["session:private".into()],
                "text/plain",
                Some(&first_cursor),
                7
            )
            .await
            .is_err());
        assert!(store
            .discover_page(&scopes, "different", Some(&first_cursor), 7)
            .await
            .is_err());
        let mut actual = std::collections::BTreeSet::new();
        let mut pages = 0;
        loop {
            pages += 1;
            assert!(pages < 30);
            for record in page.records {
                let reference = record.content_reference();
                assert!(actual.insert(reference.selector.clone()));
                let bytes = store.read(&reference, &scopes[0], None).await.unwrap();
                assert_eq!(
                    format!("sha256:{:x}", Sha256::digest(&bytes)),
                    reference.sha256
                );
            }
            let Some(cursor) = page.next_cursor else {
                break;
            };
            store
                .write_bytes(descriptor.clone(), b"new tool receipt")
                .await
                .unwrap();
            page = store
                .discover_page(&scopes, "text/plain", Some(&cursor), 7)
                .await
                .unwrap();
        }
        assert_eq!(actual, expected);
        assert!(!actual.contains(&hidden.selector));
        store.delete(&hidden, "session:private").unwrap();
        store
            .discover_page(&scopes, "text/plain", Some(&first_cursor), 7)
            .await
            .unwrap();
        let removed = store.resolve(expected.first().unwrap()).unwrap();
        store.delete(&removed, &scopes[0]).unwrap();
        assert!(store
            .discover_page(&scopes, "text/plain", Some(&first_cursor), 7)
            .await
            .unwrap_err()
            .to_string()
            .contains("source changed"));
    }
}
