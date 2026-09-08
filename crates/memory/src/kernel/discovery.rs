//! Explicit catalog discovery is complete over its lexical scope. Ranked hybrid
//! recall remains a separate, bounded recommendation owned by the same kernel.
use super::*;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize)]
pub struct MemoryDiscoveryItem {
    pub atom: MemoryAtomView,
    pub scope: MemoryScope,
    pub revision: String,
    pub updated_at: String,
    pub preview: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct MemoryDiscoveryResult {
    pub selected: Vec<MemoryDiscoveryItem>,
    pub next_cursor: Option<String>,
    pub scope_revisions: BTreeMap<String, i64>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    version: u8,
    binding: Uuid,
    after_id: Option<String>,
    skipped: usize,
    revisions: BTreeMap<String, i64>,
}
fn invalid(message: impl Into<String>) -> MemoryKernelError {
    MemoryError::Store(message.into()).into()
}

impl MemoryKernel {
    pub async fn discover_page(
        &self,
        ctx: &MemoryTurnContext,
        query: &str,
        cursor: Option<&str>,
        limit: usize,
    ) -> MemoryKernelResult<MemoryDiscoveryResult> {
        let query = query.trim();
        let binding = Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            &serde_json::to_vec(&(ctx, query)).map_err(|e| invalid(e.to_string()))?,
        );
        let cursor = cursor
            .map(|cursor| {
                serde_json::from_str::<Cursor>(cursor)
                    .map_err(|_| invalid("invalid memory discovery cursor"))
            })
            .transpose()?;
        if cursor.as_ref().is_some_and(|cursor| {
            cursor.version != 1
                || cursor.binding != binding
                || cursor
                    .after_id
                    .as_deref()
                    .is_some_and(|id| Uuid::try_parse(id).is_err())
        }) {
            return Err(invalid(
                "memory discovery cursor does not match query or Runtime Memory Binding",
            ));
        }
        let mut after_id = cursor.as_ref().and_then(|cursor| cursor.after_id.clone());
        let mut skipped = cursor.as_ref().map_or(0, |cursor| cursor.skipped);
        let page = self
            .manager
            .discover_memory_page(crate::store::MemoryDiscoveryQuery {
                scopes: memory_binding_search_scopes(ctx),
                query: query.into(),
                after_id: after_id.clone(),
                skip: skipped,
                expected_revisions: cursor.map(|cursor| cursor.revisions),
                limit: limit.clamp(1, 128),
            })
            .await?;
        let lifecycle = page
            .lifecycle
            .into_iter()
            .map(|value| (value.key, value.value))
            .collect::<HashMap<_, _>>();
        let mut selected = Vec::new();
        for entry in page.entries {
            if !memory_entry_visible_to_ctx(&entry, ctx) {
                skipped = skipped
                    .checked_add(1)
                    .ok_or_else(|| invalid("memory cursor overflow"))?;
                continue;
            }
            after_id = Some(entry.id.to_string());
            skipped = 0;
            let mut atom = MemoryAtomView::from_entry(&entry, MemoryInformationState::Trace);
            if let Some(value) = lifecycle.get(&lifecycle_key(entry.id)) {
                let events: Vec<MemoryLifecycleEvent> =
                    serde_json::from_str(value).map_err(|_| {
                        invalid("memory discovery lifecycle unavailable; retry after source repair")
                    })?;
                if let Some(event) = events.last() {
                    atom.state = event.to;
                }
            }
            if matches!(atom.state, MemoryState::Superseded | MemoryState::Archived) {
                continue;
            }
            let mut revision_value =
                serde_json::to_value(&entry).map_err(|e| invalid(e.to_string()))?;
            if let Some(value) = revision_value.as_object_mut() {
                value.remove("access_count");
                value.remove("last_accessed_at");
            }
            let revision = Uuid::new_v5(
                &Uuid::NAMESPACE_OID,
                &serde_json::to_vec(&(revision_value, atom.state))
                    .map_err(|e| invalid(e.to_string()))?,
            )
            .to_string();
            selected.push(MemoryDiscoveryItem {
                atom,
                scope: entry.scope,
                revision,
                updated_at: entry.updated_at.to_rfc3339(),
                preview: entry.content.chars().take(480).collect(),
            });
        }
        let next_cursor = page
            .next_id
            .map(|_| {
                serde_json::to_string(&Cursor {
                    version: 1,
                    binding,
                    after_id,
                    skipped,
                    revisions: page.revisions.clone(),
                })
                .map_err(|e| invalid(e.to_string()))
            })
            .transpose()?;
        Ok(MemoryDiscoveryResult {
            selected,
            next_cursor,
            scope_revisions: page.revisions,
        })
    }
}
