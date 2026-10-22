//! Immutable publication in the existing ArtifactStore; adapters supply authorized bytes.
use harness_contract::context::{ArtifactRef, ArtifactWriteDescriptor};
use sha2::{Digest, Sha256};

pub(crate) const FRESH_OBJECT_KIND: &str = "artifact.fresh_object_created";

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FreshObjectCreated {
    pub version: u32,
    pub session_id: String,
    pub artifact: ArtifactRef,
    pub precreation_head: u64,
}

fn fresh_record_identity(artifact: &ArtifactRef) -> bool {
    let digest = artifact
        .sha256
        .strip_prefix("sha256:")
        .unwrap_or(&artifact.sha256);
    artifact
        .selector
        .strip_prefix("artifact://art_")
        .is_some_and(|identity| {
            identity.len() == 32
                && identity
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
                && identity.as_bytes()[12] == b'4'
                && matches!(identity.as_bytes()[16], b'8' | b'9' | b'a' | b'b')
        })
        && artifact.is_durable()
        && !artifact.media_type.trim().is_empty()
        && digest.len() == 64
        && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(crate) fn fresh_object_precreation_head(
    events: &[crate::DurableRuntimeEvent],
    session_id: &str,
    canonical: &ArtifactRef,
) -> Result<Option<u64>, String> {
    let stream = format!("session:{session_id}");
    let mut head = None;
    for event in events
        .iter()
        .filter(|event| event.kind == FRESH_OBJECT_KIND)
    {
        let names_target = event.payload["artifact"]["selector"] == canonical.selector
            || event
                .refs
                .iter()
                .any(|reference| reference.id == canonical.selector);
        if !names_target {
            continue;
        }
        let fact: FreshObjectCreated = serde_json::from_value(event.payload.clone())
            .map_err(|error| format!("invalid fresh object fact:{error}"))?;
        let canonical_payload = serde_json::to_value(&fact).map_err(|error| error.to_string())?;
        let key = format!("fresh-object:{}", canonical.selector);
        if head.is_some()
            || canonical_payload != event.payload
            || event.schema_version != 1
            || event.scope != crate::RuntimeEventScope::Session
            || event.stream_id != stream
            || event.actor.as_deref() != Some("runtime_content_publication")
            || event.status.as_deref() != Some("created")
            || event.transaction_id != key
            || event.idempotency_key.as_deref() != Some(key.as_str())
            || event.transaction_index != 0
            || fact.version != 1
            || fact.session_id != session_id
            || fact.artifact != *canonical
            || !fresh_record_identity(canonical)
            || canonical.visibility_scope != stream
            || fact.precreation_head.checked_add(1) != Some(event.sequence)
            || event.refs != fresh_object_refs(session_id, &canonical.selector)
        {
            return Err("fresh object fact conflicts with canonical identity or causal cut".into());
        }
        head = Some(fact.precreation_head);
    }
    Ok(head)
}

fn fresh_object_refs(session_id: &str, selector: &str) -> Vec<crate::RuntimeEventRef> {
    vec![
        crate::RuntimeEventRef {
            kind: "session".into(),
            id: session_id.into(),
        },
        crate::RuntimeEventRef {
            kind: "artifact".into(),
            id: selector.into(),
        },
    ]
}

pub fn verify_content_revision(bytes: &[u8], expected: &str) -> Result<(), String> {
    let actual = format!("{:x}", Sha256::digest(bytes));
    if actual != expected.strip_prefix("sha256:").unwrap_or(expected) {
        return Err("source_version_conflict".into());
    }
    Ok(())
}

/// Immutable source identity plus the next unread byte. A continuation is never
/// authorization; every call resolves the current record and checks its scope.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicationContinuation {
    pub source: ArtifactRef,
    pub next_byte: u64,
}

/// Keeps original resident-byte admission alive while the returned body is in
/// use. The same guard is moved into blocking IO, so cancelling the async waiter
/// cannot return capacity before the physical read has exited.
#[derive(Debug, thiserror::Error)]
pub enum PublicationReadError {
    #[error("context_capacity:{0}; use bounded publication pages")]
    Capacity(String),
    #[error("publication source rejected:{0}")]
    Source(String),
    #[error("publication read failed:{0}")]
    Io(String),
}
impl From<String> for PublicationReadError {
    fn from(value: String) -> Self {
        Self::Source(value)
    }
}
impl From<&str> for PublicationReadError {
    fn from(value: &str) -> Self {
        Self::Source(value.into())
    }
}

pub struct AuthorizedPublicationBytes {
    bytes: Vec<u8>,
    _reservation: crate::execution_core::graph::ExecutionResourceLease,
}

impl std::fmt::Debug for AuthorizedPublicationBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthorizedPublicationBytes")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

impl AsRef<[u8]> for AuthorizedPublicationBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::ops::Deref for AuthorizedPublicationBytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
pub struct AuthorizedPublicationPage {
    pub source: ArtifactRef,
    pub start: u64,
    pub end: u64,
    pub total_bytes: u64,
    pub continuation: Option<PublicationContinuation>,
    pub body: AuthorizedPublicationBytes,
}

const PUBLICATION_PAGE_MAX_BYTES: u64 = 64 * 1024;
pub(crate) const PUBLICATION_RESIDENT_BYTES: usize = 64 * 1024 * 1024;

struct PublicationReadPin {
    store: std::sync::Arc<crate::ArtifactStore>,
    artifact: ArtifactRef,
    owner: String,
}
impl Drop for PublicationReadPin {
    fn drop(&mut self) {
        if let Err(error) = self.store.unpin(&self.artifact, &self.owner) {
            tracing::warn!(%error, "publication read pin release failed; pin retained");
        }
    }
}

impl crate::RuntimeServices {
    pub async fn publish_authorized_content(
        &self,
        bytes: &[u8],
        expected_sha256: &str,
        media_type: &str,
        session_id: &str,
    ) -> Result<ArtifactRef, String> {
        if session_id.trim().is_empty() || media_type.trim().is_empty() {
            return Err("publication requires Session scope and media type".into());
        }
        verify_content_revision(bytes, expected_sha256)?;
        let stream = format!("session:{session_id}");
        let precreation_head = self
            .event_store()
            .stream_revision(&stream)
            .map_err(|error| format!("publication precreation head failed:{error}"))?;
        let artifact = self
            .artifact_store()
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: media_type.into(),
                    visibility_scope: format!("session:{session_id}"),
                    expected_bytes: Some(bytes.len() as u64),
                    original_name: None,
                },
                bytes,
            )
            .await
            .map_err(|error| error.to_string())?;
        if !fresh_record_identity(&artifact)
            || artifact.visibility_scope != stream
            || artifact.media_type != media_type
            || artifact.bytes != bytes.len() as u64
            || artifact.sha256.trim_start_matches("sha256:")
                != format!("{:x}", Sha256::digest(bytes))
        {
            return Err("publication minted an invalid record; uncertified orphan retained".into());
        }
        let key = format!("fresh-object:{}", artifact.selector);
        let receipt = self.event_store().append_transaction(crate::AppendTransactionRequest {
            transaction_id: key.clone(),
            expected_streams: vec![crate::ExpectedStreamRevision {
                stream_id: stream.clone(), expected_revision: precreation_head,
            }],
            events: vec![crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream,
                    scope: crate::RuntimeEventScope::Session,
                    kind: FRESH_OBJECT_KIND.into(),
                    status: Some("created".into()),
                    actor: Some("runtime_content_publication".into()),
                    refs: fresh_object_refs(session_id, &artifact.selector),
                    payload: serde_json::to_value(FreshObjectCreated {
                        version: 1, session_id: session_id.into(),
                        artifact: artifact.clone(), precreation_head,
                    }).map_err(|error| error.to_string())?,
                },
                idempotency_key: Some(key), schema_version: 1,
            }],
        }).map_err(|error| match error {
            crate::RuntimeEventStoreError::StaleRevision { .. } =>
                format!("publication CAS failed; uncertified orphan retained:{error}"),
            _ => format!("publication journal failed or uncertain; orphan retained without successful publication:{error}"),
        })?;
        if receipt.duplicate
            || receipt.event_ids.len() != 1
            || receipt.transaction_id != format!("fresh-object:{}", artifact.selector)
            || receipt.stream_revisions.len() != 1
            || receipt.stream_revisions[0].stream_id != format!("session:{session_id}")
            || receipt.stream_revisions[0].expected_revision != precreation_head
            || precreation_head.checked_add(1)
                != Some(receipt.stream_revisions[0].committed_revision)
        {
            return Err(
                "publication journal receipt uncertain; orphan retained without retry".into(),
            );
        }
        Ok(artifact)
    }

    fn authorized_publication_source(
        &self,
        content_ref: &str,
        expected_sha256: &str,
        session_id: &str,
        authorized_scopes: &[String],
    ) -> Result<ArtifactRef, String> {
        let artifact = self
            .artifact_store()
            .resolve(content_ref)
            .map_err(|e| e.to_string())?;
        if artifact.visibility_scope != format!("session:{session_id}")
            && !authorized_scopes
                .iter()
                .any(|scope| scope == &artifact.visibility_scope)
        {
            return Err("content is outside authorized scope".into());
        }
        if artifact.sha256.trim_start_matches("sha256:")
            != expected_sha256.trim_start_matches("sha256:")
        {
            return Err("source_version_conflict".into());
        }
        Ok(artifact)
    }

    async fn read_publication_range(
        &self,
        artifact: &ArtifactRef,
        range: std::ops::Range<u64>,
        session_id: &str,
    ) -> Result<AuthorizedPublicationBytes, PublicationReadError> {
        let count = range
            .end
            .checked_sub(range.start)
            .ok_or("invalid publication range")?;
        // Covers returned bytes, UTF8/base64/JSON expansion and verification
        // scratch before repository.object may materialize a compact body.
        let resident = count
            .checked_mul(8)
            .and_then(|n| n.checked_add(self.artifact_store().public_read_scratch_bytes()))
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| PublicationReadError::Capacity("resident budget overflow".into()))?;
        let kind = crate::execution_core::graph::ExecutionResourceKind::ContextBytes;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| e.to_string())?
            .as_millis() as u64;
        let request = crate::execution_core::graph::ResourceAdmissionRequest::new(
            crate::execution_core::graph::ExecutionServiceClass::Foreground,
            [(kind.clone(), resident.max(1))],
        )
        .with_fairness_key(format!("context:{session_id}"))
        .with_deadline_at_ms(now.saturating_add(5000));
        let lease = match self
            .resource_manager()
            .admit(request)
            .await
            .map_err(|e| PublicationReadError::Capacity(e.to_string()))?
        {
            crate::execution_core::graph::ResourceAdmissionDecision::Granted { lease, .. } => lease,
            _ => {
                return Err(PublicationReadError::Capacity(
                    "publication reservation not admitted".into(),
                ))
            }
        };
        let store = self.artifact_store().clone();
        let artifact = artifact.clone();
        tokio::task::spawn_blocking(move || {
            let _lease = lease;
            // Resolve rejects PrivatePrepared before pin can adopt anything.
            if store
                .resolve(&artifact.selector)
                .map_err(|e| e.to_string())?
                != artifact
            {
                return Err("source_version_conflict".into());
            }
            let owner = format!("publication-read:{}", uuid::Uuid::new_v4());
            store
                .pin(&artifact, &owner, crate::ARTIFACT_PERMANENT_PIN_UNTIL_MS)
                .map_err(|e| e.to_string())?;
            let _pin = PublicationReadPin {
                store: store.clone(),
                artifact: artifact.clone(),
                owner,
            };
            let bytes = store
                .read_public_range_blocking(&artifact, &artifact.visibility_scope, range)
                .map_err(|e| e.to_string())?;
            Ok(AuthorizedPublicationBytes {
                bytes,
                _reservation: _lease,
            })
        })
        .await
        .map_err(|e| PublicationReadError::Io(e.to_string()))?
        .map_err(PublicationReadError::Io)
    }

    pub async fn read_authorized_publication_page(
        &self,
        content_ref: &str,
        expected_sha256: &str,
        session_id: &str,
        authorized_scopes: &[String],
        max_bytes: u64,
        continuation: Option<&PublicationContinuation>,
    ) -> Result<AuthorizedPublicationPage, PublicationReadError> {
        if max_bytes == 0 || max_bytes > PUBLICATION_PAGE_MAX_BYTES {
            return Err("publication page max_bytes must be 1..=65536".into());
        }
        let artifact = self.authorized_publication_source(
            content_ref,
            expected_sha256,
            session_id,
            authorized_scopes,
        )?;
        let start = match continuation {
            None => 0,
            Some(cursor) if cursor.source == artifact && cursor.next_byte <= artifact.bytes => {
                cursor.next_byte
            }
            Some(_) => return Err("publication continuation source revision/range changed".into()),
        };
        let end = start.saturating_add(max_bytes).min(artifact.bytes);
        let body = self
            .read_publication_range(&artifact, start..end, session_id)
            .await?;
        Ok(AuthorizedPublicationPage {
            total_bytes: artifact.bytes,
            source: artifact.clone(),
            start,
            end,
            continuation: (end < artifact.bytes).then_some(PublicationContinuation {
                source: artifact,
                next_byte: end,
            }),
            body,
        })
    }

    pub async fn read_authorized_publication(
        &self,
        content_ref: &str,
        expected_sha256: &str,
        session_id: &str,
        authorized_scopes: &[String],
    ) -> Result<(ArtifactRef, AuthorizedPublicationBytes), PublicationReadError> {
        let artifact = self.authorized_publication_source(
            content_ref,
            expected_sha256,
            session_id,
            authorized_scopes,
        )?;
        let bytes = self
            .read_publication_range(&artifact, 0..artifact.bytes, session_id)
            .await?;
        Ok((artifact, bytes))
    }

}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::runtime_event_store::*;
    use std::sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    };

    #[derive(Debug)]
    pub(crate) struct JournalFault {
        inner: EphemeralRuntimeEventStore,
        mode: AtomicU8,
        attempted: Mutex<Vec<AppendTransactionRequest>>,
        read_override: Mutex<Option<(String, Vec<DurableRuntimeEvent>)>>,
        source_view: Mutex<(String, u64)>,
        page_events: std::sync::atomic::AtomicUsize,
        full_reads: std::sync::atomic::AtomicUsize,
        projection_cursor_race: AtomicU8,
        source_reads_763: Mutex<Option<(std::thread::ThreadId, Vec<(String, String, usize, usize)>)>>,
    }

    impl JournalFault {
        pub(crate) fn new_journal() -> (Arc<RuntimeEventStore>, Arc<Self>) {
            let backend = Arc::new(Self {
                inner: EphemeralRuntimeEventStore::new(),
                mode: AtomicU8::new(0),
                attempted: Mutex::new(Vec::new()),
                read_override: Mutex::new(None),
                source_view: Mutex::new((uuid::Uuid::new_v4().to_string(), 0)),
                page_events: std::sync::atomic::AtomicUsize::new(0),
                full_reads: std::sync::atomic::AtomicUsize::new(0),
                projection_cursor_race: AtomicU8::new(0),
                source_reads_763: Mutex::new(None),
            });
            (
                Arc::new(RuntimeEventStore::from_backend(backend.clone())),
                backend,
            )
        }

        pub(crate) fn replace_read(&self, stream: &str, events: Option<Vec<DurableRuntimeEvent>>) {
            let mut view = self.source_view.lock().unwrap();
            view.1 = view.1.checked_add(1).expect("journal view epoch overflow");
            *self.read_override.lock().unwrap() = events.map(|events| (stream.into(), events));
        }

        fn set_mode(&self, mode: u8) {
            let mut view = self.source_view.lock().unwrap();
            view.1 = view.1.checked_add(1).expect("journal view epoch overflow");
            self.mode.store(mode, Ordering::SeqCst);
        }

        pub(crate) fn begin_source_reads_763(&self) {
            *self.source_reads_763.lock().unwrap() = Some((std::thread::current().id(), Vec::new()));
        }

        pub(crate) fn take_source_reads_763(&self) -> Vec<(String, String, usize, usize)> {
            self.source_reads_763.lock().unwrap().take().unwrap().1
        }

        fn record_source_read_763(&self, port: &str, stream: &str, events: &[DurableRuntimeEvent]) {
            let mut probe = self.source_reads_763.lock().unwrap();
            if let Some((thread, reads)) = probe.as_mut() {
                if *thread != std::thread::current().id() {
                    return;
                }
                let bytes = events.iter().map(|event| serde_json::to_vec(event).unwrap().len()).sum();
                reads.push((port.into(), stream.into(), events.len(), bytes));
            }
        }

        pub(crate) fn history_reads(&self) -> (usize, usize) {
            (
                self.page_events.load(Ordering::SeqCst),
                self.full_reads.load(Ordering::SeqCst),
            )
        }

        pub(crate) fn advance_cursor_after_projection_page(&self) {
            self.projection_cursor_race.store(1, Ordering::SeqCst);
        }

        pub(crate) fn reset_history_reads(&self) {
            self.page_events.store(0, Ordering::SeqCst);
            self.full_reads.store(0, Ordering::SeqCst);
        }
    }

    macro_rules! delegate_ports {
        ($(fn $name:ident(&self $(, $arg:ident: $kind:ty)*) -> $result:ty;)*) => {
            $(#[allow(unused_variables, clippy::too_many_arguments)]
            fn $name(&self $(, $arg: $kind)*) -> $result {
                self.inner.$name($($arg),*)
            })*
        };
    }


    fn resident_active(services: &crate::RuntimeServices) -> usize {
        services
            .resource_manager()
            .snapshots()
            .unwrap()
            .iter()
            .filter(|s| s.kind == crate::execution_core::graph::ExecutionResourceKind::ContextBytes)
            .map(|s| s.active_leases)
            .sum()
    }

    #[tokio::test]
    async fn bounded_publication_pages_restore_exact_bytes_and_own_the_original_reservation() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let bytes = (0..530_017).map(|i| (i % 251) as u8).collect::<Vec<_>>();
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let artifact = services
            .publish_authorized_content(&bytes, &digest, "application/octet-stream", "pages")
            .await
            .unwrap();
        let mut next = None;
        let mut restored = Vec::new();
        let mut pages = 0;
        loop {
            let page = services
                .read_authorized_publication_page(
                    &artifact.selector,
                    &digest,
                    "pages",
                    &[],
                    65536,
                    next.as_ref(),
                )
                .await
                .unwrap();
            assert_eq!(page.start, restored.len() as u64);
            assert_eq!(page.total_bytes, 530_017);
            assert!(page.body.len() <= 65536);
            assert!(resident_active(&services) > 0);
            restored.extend_from_slice(&page.body);
            next = page.continuation.clone();
            drop(page);
            assert_eq!(resident_active(&services), 0);
            pages += 1;
            if next.is_none() {
                break;
            }
        }
        assert_eq!(pages, 9);
        assert_eq!(restored, bytes);
    }

    #[tokio::test]
    async fn bounded_publication_rejects_changed_source_scope_and_invalid_selection_before_io() {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let bytes = b"original authorised source";
        let digest = format!("{:x}", Sha256::digest(bytes));
        let artifact = services
            .publish_authorized_content(bytes, &digest, "text/plain", "source")
            .await
            .unwrap();
        let page = services
            .read_authorized_publication_page(&artifact.selector, &digest, "source", &[], 4, None)
            .await
            .unwrap();
        let mut cursor = page.continuation.clone().unwrap();
        drop(page);
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        services
            .artifact_store()
            .set_public_read_hook(Arc::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        assert!(services
            .read_authorized_publication_page(&artifact.selector, &digest, "foreign", &[], 4, None)
            .await
            .is_err());
        assert!(services
            .read_authorized_publication_page(&artifact.selector, "wrong", "source", &[], 4, None)
            .await
            .is_err());
        assert!(services
            .read_authorized_publication_page(
                &artifact.selector,
                &digest,
                "source",
                &[],
                65537,
                None
            )
            .await
            .is_err());
        cursor.source.sha256 = "sha256:other".into();
        assert!(services
            .read_authorized_publication_page(
                &artifact.selector,
                &digest,
                "source",
                &[],
                4,
                Some(&cursor)
            )
            .await
            .is_err());
        cursor.source = artifact.clone();
        cursor.next_byte = artifact.bytes + 1;
        assert!(services
            .read_authorized_publication_page(
                &artifact.selector,
                &digest,
                "source",
                &[],
                4,
                Some(&cursor)
            )
            .await
            .is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert_eq!(resident_active(&services), 0);
        let page = services
            .read_authorized_publication_page(
                &artifact.selector,
                &digest,
                "foreign",
                &["session:source".into()],
                4,
                None,
            )
            .await
            .unwrap();
        assert_eq!(&*page.body, b"orig");
    }

    #[test]
    fn provider_reload_preserves_the_original_context_budget_ceiling() {
        use crate::execution_core::graph::{ExecutionResourceKind, ResourceQuota};
        let services = crate::RuntimeServices::in_memory().unwrap();
        services
            .resource_manager()
            .update_quota(
                &ExecutionResourceKind::ContextBytes,
                ResourceQuota::new(1, 2 * 1024 * 1024, 2 * 1024 * 1024).unwrap(),
            )
            .unwrap();
        services
            .replace_provider_resource_config(crate::ProviderResourceConfig::default())
            .unwrap();
        let current = services
            .resource_manager()
            .snapshot(&ExecutionResourceKind::ContextBytes)
            .unwrap();
        assert_eq!(current.target, 2 * 1024 * 1024);
        assert_eq!(current.maximum, 2 * 1024 * 1024);
    }

    #[tokio::test]
    async fn required_publication_overflow_refuses_before_materializing_but_pages_remain_readable()
    {
        let services = crate::RuntimeServices::in_memory().unwrap();
        let bytes = vec![b'x'; 9 * 1024 * 1024];
        let digest = format!("{:x}", Sha256::digest(&bytes));
        let artifact = services
            .publish_authorized_content(&bytes, &digest, "text/plain", "large")
            .await
            .unwrap();
        let hits = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        services
            .artifact_store()
            .set_public_read_hook(Arc::new(move || {
                counter.fetch_add(1, Ordering::SeqCst);
            }));
        let error = services
            .read_authorized_publication(&artifact.selector, &digest, "large", &[])
            .await
            .err()
            .unwrap();
        assert!(
            matches!(error, PublicationReadError::Capacity(_)),
            "{error}"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        let page = services
            .read_authorized_publication_page(&artifact.selector, &digest, "large", &[], 16, None)
            .await
            .unwrap();
        assert_eq!(&*page.body, b"xxxxxxxxxxxxxxxx");
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_or_panicked_publication_io_releases_only_after_physical_exit() {
        for panic_after_release in [false, true] {
            let services = crate::RuntimeServices::in_memory().unwrap();
            let bytes = b"held physical read";
            let digest = format!("{:x}", Sha256::digest(bytes));
            let artifact = services
                .publish_authorized_content(bytes, &digest, "text/plain", "hold")
                .await
                .unwrap();
            let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
            let tx = Mutex::new(Some(entered_tx));
            let release = Arc::new(std::sync::Barrier::new(2));
            let barrier = release.clone();
            services
                .artifact_store()
                .set_public_read_hook(Arc::new(move || {
                    tx.lock().unwrap().take().unwrap().send(()).unwrap();
                    barrier.wait();
                    assert!(!panic_after_release, "controlled read panic");
                }));
            let owner = services.clone();
            let reference = artifact.clone();
            let read = tokio::spawn(async move {
                owner
                    .read_authorized_publication_page(
                        &reference.selector,
                        &digest,
                        "hold",
                        &[],
                        8,
                        None,
                    )
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(3), entered_rx)
                .await
                .unwrap()
                .unwrap();
            assert!(resident_active(&services) > 0);
            read.abort();
            assert!(read.await.unwrap_err().is_cancelled());
            assert!(
                resident_active(&services) > 0,
                "cancelled async waiter must not free live IO capacity"
            );
            assert!(
                services
                    .artifact_store()
                    .delete(&artifact, "session:hold")
                    .is_err(),
                "physical read pin prevents deletion"
            );
            release.wait();
            tokio::time::timeout(std::time::Duration::from_secs(3), async {
                while resident_active(&services) != 0 {
                    services.resource_manager().wait_for_change().await;
                }
            })
            .await
            .unwrap();
            services
                .artifact_store()
                .delete(&artifact, "session:hold")
                .unwrap();
        }
    }

    #[test]
    fn own843_journal_source_view_fences_replace_read_and_head_fault_without_forwarding() {
        let (store, backend) = JournalFault::new_journal();
        let before = backend.source_mutation_fence().unwrap();
        let original = store.append(RuntimeEventInput {
            stream_id: "843-journal-view".into(), scope: RuntimeEventScope::Session,
            kind: "843.view".into(), status: None, actor: None, refs: Vec::new(),
            payload: serde_json::json!({"original":true}),
        }).unwrap();
        assert_eq!(backend.source_mutation_fence().unwrap(), before);
        let mut changed = original.clone();
        changed.payload = serde_json::json!({"original":false});
        backend.replace_read(&original.stream_id, Some(vec![changed.clone()]));
        let replaced = backend.source_mutation_fence().unwrap();
        assert_ne!(replaced, before);
        assert_eq!(backend.list_stream(&original.stream_id).unwrap(), vec![changed]);
        assert!(backend.history_reads().1 > 0);
        assert!(backend.compare_and_put_projection_checkpoint_fenced(
            "843-journal-fence", original.commit_cursor, 0, &serde_json::json!({}), 0, &before,
        ).is_err());
        backend.replace_read(&original.stream_id, None);
        let restored = backend.source_mutation_fence().unwrap();
        assert_ne!(restored, before);
        let inner = backend.inner.source_mutation_fence().unwrap();
        assert!(backend.compare_and_put_projection_checkpoint_fenced(
            "843-journal-fence", original.commit_cursor, 0, &serde_json::json!({}), 0, &inner,
        ).is_err());
        backend.set_mode(1);
        assert!(backend.stream_revision(&original.stream_id).is_err());
        assert!(backend.compare_and_put_projection_checkpoint_fenced(
            "843-journal-fence", original.commit_cursor, 0, &serde_json::json!({}), 0, &restored,
        ).is_err());
        backend.set_mode(0);
        let current = backend.source_mutation_fence().unwrap();
        backend.compare_and_put_projection_checkpoint_fenced(
            "843-journal-fence", original.commit_cursor, 0, &serde_json::json!({}), 0, &current,
        ).unwrap();
    }

    impl RuntimeEventStoreBackend for JournalFault {
        fn bounded_event_by_idempotency_key(
            &self, stream: &str, key: &str, through: u64, max_bytes: usize,
        ) -> RuntimeEventStoreResult<crate::RuntimeBoundedEventLookup> {
            self.inner.bounded_event_by_idempotency_key(stream, key, through, max_bytes)
        }

        fn source_mutation_fence(&self) -> RuntimeEventStoreResult<crate::RuntimeSourceMutationFence> {
            let view = self.source_view.lock().unwrap();
            let inner = self.inner.source_mutation_fence()?;
            Ok(crate::RuntimeSourceMutationFence {
                incarnation: serde_json::to_string(&(&view.0, view.1, inner.incarnation))?,
                mutation_epoch: inner.mutation_epoch,
            })
        }

        fn compare_and_put_projection_checkpoint_fenced(
            &self, id: &str, cursor: u64, expected: u64,
            payload: &serde_json::Value, at: u64, source: &crate::RuntimeSourceMutationFence,
        ) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint> {
            let view = self.source_view.lock().unwrap();
            let inner = self.inner.source_mutation_fence()?;
            let current = crate::RuntimeSourceMutationFence {
                incarnation: serde_json::to_string(&(&view.0, view.1, &inner.incarnation))?,
                mutation_epoch: inner.mutation_epoch,
            };
            if current != *source {
                return Err(RuntimeEventStoreError::Corrupt("journal source view fence changed".into()));
            }
            self.inner.compare_and_put_projection_checkpoint_fenced(
                id, cursor, expected, payload, at, &inner,
            )
        }

        fn append_transaction(
            &self,
            request: AppendTransactionRequest,
        ) -> RuntimeEventStoreResult<AppendTransactionReceipt> {
            self.attempted.lock().unwrap().push(request.clone());
            match self.mode.load(Ordering::SeqCst) {
                2 => Err(RuntimeEventStoreError::Corrupt(
                    "injected journal failure".into(),
                )),
                3 => {
                    let stream = &request.expected_streams[0];
                    self.inner.append_transaction(AppendTransactionRequest {
                        transaction_id: "interleaved-writer".into(),
                        expected_streams: vec![stream.clone()],
                        events: vec![RuntimeEventInput {
                            stream_id: stream.stream_id.clone(),
                            scope: RuntimeEventScope::Session,
                            kind: "test.concurrent_writer".into(),
                            status: None,
                            actor: None,
                            refs: Vec::new(),
                            payload: serde_json::json!({}),
                        }
                        .into()],
                    })?;
                    self.inner.append_transaction(request)
                }
                4 => {
                    self.inner.append_transaction(request)?;
                    Err(RuntimeEventStoreError::Corrupt(
                        "injected uncertain commit".into(),
                    ))
                }
                5 => Err(RuntimeEventStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "injected journal timeout",
                ))),
                6 => Err(RuntimeEventStoreError::Io(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "injected journal cancellation",
                ))),
                _ => self.inner.append_transaction(request),
            }
        }
        fn stream_revision(&self, stream_id: &str) -> RuntimeEventStoreResult<u64> {
            let result = (|| {
            if self.mode.load(Ordering::SeqCst) == 1 {
                Err(RuntimeEventStoreError::Corrupt(
                    "injected head failure".into(),
                ))
            } else {
                self.inner.stream_revision(stream_id)
            }
            })();
            self.record_source_read_763("stream_revision", stream_id, &[]);
            result
        }
        fn all_events(&self, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String> {
            self.inner.all_events(limit)
        }
        fn list_stream(&self, stream_id: &str) -> Result<Vec<DurableRuntimeEvent>, String> {
            let result = (|| {
            self.full_reads.fetch_add(1, Ordering::SeqCst);
            if let Some((stream, events)) = self.read_override.lock().unwrap().as_ref() {
                if stream == stream_id {
                    return Ok(events.clone());
                }
            }
            self.inner.list_stream(stream_id)
            })();
            self.record_source_read_763("list_stream", stream_id, result.as_ref().map(Vec::as_slice).unwrap_or(&[]));
            result
        }
        fn position_page(
            &self,
            scope: RuntimeEventScope,
            identity: &str,
            kind: Option<&str>,
            after: Option<(u64, u32)>,
            through_cursor: u64,
            max_events: usize,
            max_bytes: usize,
        ) -> Result<Vec<DurableRuntimeEvent>, String> {
            let events = self.inner.position_page(
                scope,
                identity,
                kind,
                after,
                through_cursor,
                max_events,
                max_bytes,
            )?;
            self.page_events.fetch_add(events.len(), Ordering::SeqCst);
            if events.is_empty() && self.projection_cursor_race.swap(0, Ordering::SeqCst) == 1 {
                self.inner
                    .append_transaction(AppendTransactionRequest {
                        transaction_id: "projection-cursor-race-395".into(),
                        expected_streams: vec![ExpectedStreamRevision {
                            stream_id: "projection-cursor-race-395".into(),
                            expected_revision: 0,
                        }],
                        events: vec![RuntimeEventInput {
                            stream_id: "projection-cursor-race-395".into(),
                            scope: RuntimeEventScope::Session,
                            kind: "test.unrelated_projection_commit".into(),
                            status: None,
                            actor: None,
                            refs: Vec::new(),
                            payload: serde_json::json!({"source": "395"}),
                        }
                        .into()],
                    })
                    .map_err(|error| error.to_string())?;
            }
            Ok(events)
        }
        fn list_stream_after(
            &self,
            stream_id: &str,
            after_sequence: u64,
            through_sequence: u64,
            max_events: usize,
            max_bytes: usize,
        ) -> Result<Vec<DurableRuntimeEvent>, String> {
            let result = (|| {
            let overridden = self
                .read_override
                .lock()
                .unwrap()
                .as_ref()
                .filter(|(stream, _)| stream == stream_id)
                .map(|(_, events)| events.clone());
            let Some(mut events) = overridden else {
                return self.inner.list_stream_after(
                    stream_id,
                    after_sequence,
                    through_sequence,
                    max_events,
                    max_bytes,
                );
            };
            i64::try_from(through_sequence).map_err(|_| "through_sequence overflow".to_string())?;
            i64::try_from(after_sequence).map_err(|_| "after_sequence overflow".to_string())?;
            if max_events == 0 || max_bytes == 0 || through_sequence <= after_sequence {
                return Ok(Vec::new());
            }
            events.sort_by_key(|event| event.sequence);
            let budget = max_bytes.min(1024 * 1024);
            let mut total = 0usize;
            let mut selected = Vec::new();
            for event in events
                .into_iter()
                .filter(|event| {
                    event.sequence > after_sequence && event.sequence <= through_sequence
                })
                .take(max_events.min(100))
            {
                let bytes = serde_json::to_vec(&event)
                    .map_err(|error| error.to_string())?
                    .len();
                if bytes > budget.saturating_sub(total) {
                    if selected.is_empty() {
                        return Err(RuntimeEventStoreError::HistoryBudgetExceeded(
                            event.event_id.clone(),
                        )
                        .to_string());
                    }
                    break;
                }
                total += bytes;
                selected.push(event);
            }
            Ok(selected)
            })();
            self.record_source_read_763("list_stream_after", stream_id,
                result.as_ref().map(Vec::as_slice).unwrap_or(&[]));
            result
        }
        fn event_by_idempotency_key(
            &self,
            stream_id: &str,
            idempotency_key: &str,
        ) -> RuntimeEventStoreResult<Option<RuntimeEventRecord>> {
            let result = self.inner.event_by_idempotency_key(stream_id, idempotency_key);
            let events = result.as_ref().ok().and_then(Option::as_ref)
                .map(std::slice::from_ref).unwrap_or(&[]);
            self.record_source_read_763("event_by_idempotency_key", stream_id, events);
            result
        }
        delegate_ports! {
            fn prepared_content_root(&self, origin: &crate::PreparedArtifactOrigin, artifact: &harness_contract::context::ArtifactRef) -> RuntimeEventStoreResult<crate::PreparedContentRoot>;
            fn transaction_stream_revision_exact(&self, transaction_id: &str, stream_id: &str) -> RuntimeEventStoreResult<Option<crate::CommittedStreamRevision>>;
            fn recent_event_lines(&self, stream_id: &str, references: &[RuntimeEventRef], through_cursor: u64, max_events: usize, max_bytes: usize) -> Result<Vec<harness_contract::mission::MissionControlEventLine>, String>;
            fn list_stream_before(&self, stream_id: &str, through_sequence: u64, max_events: usize, max_bytes: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn count_stream_kinds(&self, stream_id: &str, kinds: &[&str], through_sequence: u64) -> Result<u64, String>;
            fn append(&self, input: RuntimeEventInput) -> Result<DurableRuntimeEvent, String>;
            fn append_transaction_with_terminal(&self, request: AppendTransactionRequest, terminal: SessionTerminalInput) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn consume_verified_decision_lease(&self, lease_id: &str, principal_id: &str, review_id: &str, action: &str, scope: &str, evidence_digest: &str, credential_epoch: u64, consumed_at_ms: u64) -> RuntimeEventStoreResult<()>;
            fn append_transaction_with_verified_decision_lease(&self, request: AppendTransactionRequest, lease: &crate::VerifiedDecisionLease) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn append_batch_if_revision(&self, stream_id: String, expected_revision: u64, transaction_id: String, events: Vec<RuntimeTransactionEventInput>) -> RuntimeEventStoreResult<AppendTransactionReceipt>;
            fn events_after_cursor(&self, cursor: u64, max_commits: usize) -> RuntimeEventStoreResult<Vec<CommittedEventBatch>>;
            fn projection_scan_page(&self, cursor: u64, interest: &crate::RuntimeProjectionInterest, max_commits: usize, max_events: usize, max_bytes: usize) -> RuntimeEventStoreResult<crate::RuntimeProjectionScanPage>;
            fn projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<Option<RuntimeProjectionCheckpoint>>;
            fn projection_checkpoints_with_prefix(&self, prefix: &str) -> RuntimeEventStoreResult<Vec<RuntimeProjectionCheckpoint>>;
            fn put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn compare_and_put_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn compare_and_repair_projection_checkpoint(&self, projection_id: &str, source_cursor: u64, expected_revision: u64, payload: &serde_json::Value, updated_at_ms: u64) -> RuntimeEventStoreResult<RuntimeProjectionCheckpoint>;
            fn delete_projection_checkpoint(&self, projection_id: &str) -> RuntimeEventStoreResult<bool>;
            fn list_stream_page_desc(&self, stream_id: &str, limit: usize, offset: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn stream_event_count(&self, stream_id: &str) -> Result<usize, String>;
            fn execution_events_for_session(&self, session_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn events_for_root_execution(&self, root_execution_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn events_for_root_execution_kind(&self, root_execution_id: &str, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn events_for_activity(&self, activity_id: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn list_scope(&self, scope: RuntimeEventScope, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn list_scope_page_asc(&self, scope: RuntimeEventScope, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn list_scope_stream_prefix_page_asc(&self, scope: RuntimeEventScope, stream_prefix: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn list_scope_kind_page_asc(&self, scope: RuntimeEventScope, kind: &str, after_position: Option<(u64, u32)>, limit: usize) -> Result<Vec<DurableRuntimeEvent>, String>;
            fn stream_ids_for_scope(&self, scope: RuntimeEventScope) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<String>>;
            fn stream_ids_for_scope_kind_at_sequence_page(&self, scope: RuntimeEventScope, kind: &str, sequence: u64, after: Option<(u64, String)>, limit: usize) -> RuntimeEventStoreResult<Vec<(String, u64)>>;
            fn latest_stream_statuses_for_scope_kind_at_sequence(&self, scope: RuntimeEventScope, kind: &str, sequence: u64) -> RuntimeEventStoreResult<Vec<(String, Option<String>)>>;
            fn latest_for_stream(&self, stream_id: &str) -> Result<Option<DurableRuntimeEvent>, String>;
            fn latest_for_stream_kind(&self, stream_id: &str, kind: &str) -> Result<Option<DurableRuntimeEvent>, String>;
            fn enqueue_session_terminal(&self, terminal_id: &str, message_id: &str, session_id: &str, commit_cursor: u64, payload_ref: &str) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn claim_session_terminals(&self, worker_id: &str, now_ms: u64, lease_ms: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal(&self, terminal_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn session_terminal_for_execution(&self, session_id: &str, execution_id: &str, turn_id: &str) -> RuntimeEventStoreResult<Option<RuntimeSessionOutboxRecord>>;
            fn has_unsettled_session_terminals(&self, session_id: &str) -> RuntimeEventStoreResult<bool>;
            fn materialized_session_terminals_after(&self, session_id: &str, after_commit_cursor: u64, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn session_terminal_health(&self) -> RuntimeEventStoreResult<RuntimeSessionOutboxHealth>;
            fn blocked_session_terminals(&self, limit: usize) -> RuntimeEventStoreResult<Vec<RuntimeSessionOutboxRecord>>;
            fn retry_session_terminal(&self, terminal_id: &str, actor: &str, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn adopt_session_terminal_fence(&self, request: &RuntimeSessionTerminalFenceAdoption) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn ack_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn suppress_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, reason: &str, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
            fn fail_session_terminal(&self, terminal_id: &str, worker_id: &str, expected_revision: u64, class: RuntimeSessionOutboxFailureClass, error: &str, retry_at_ms: u64, max_attempts: u32, now_ms: u64) -> RuntimeEventStoreResult<RuntimeSessionOutboxRecord>;
        }
    }

    #[tokio::test]
    async fn r1_publication_fresh_object_journal_failure_is_not_success() {
        for mode in [0, 1, 2, 3, 4, 5, 6] {
            let root = tempfile::tempdir().unwrap();
            let (journal, backend) = JournalFault::new_journal();
            let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
                root.path().join("blobs"),
            ));
            let services = crate::RuntimeServices::builder(root.path().join("home"), root.path())
                .runtime_event_store(journal)
                .artifact_store(artifacts.clone())
                .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
                .build()
                .unwrap();
            backend.set_mode(mode);
            let bytes = b"fresh journal fault boundaries";
            let digest = format!("{:x}", Sha256::digest(bytes));
            let before = artifacts.stats().unwrap().artifacts;
            let outcome = services
                .publish_authorized_content(bytes, &digest, "text/plain", "fault")
                .await;
            assert_eq!(outcome.is_ok(), mode == 0, "mode {mode}");
            if let Err(error) = &outcome {
                let boundary = match mode {
                    1 => "precreation head failed",
                    3 => "CAS failed; uncertified orphan retained",
                    _ => "journal failed or uncertain; orphan retained",
                };
                assert!(error.contains(boundary), "mode {mode}: {error}");
            }
            assert_eq!(
                artifacts.stats().unwrap().artifacts,
                before + u64::from(mode != 1)
            );
            let attempted = backend.attempted.lock().unwrap().clone();
            assert_eq!(attempted.len(), usize::from(mode != 1));
            if let Some(request) = attempted.first() {
                assert_eq!(request.expected_streams[0].expected_revision, 0);
                let orphan: ArtifactRef =
                    serde_json::from_value(request.events[0].event.payload["artifact"].clone())
                        .unwrap();
                assert_eq!(artifacts.resolve(&orphan.selector).unwrap(), orphan);
                let recorded = backend.inner.list_stream("session:fault").unwrap();
                assert_eq!(
                    recorded
                        .iter()
                        .filter(|event| event.kind == FRESH_OBJECT_KIND)
                        .count(),
                    usize::from(mode == 0 || mode == 4)
                );
                backend.set_mode(0);
                let retry = services
                    .publish_authorized_content(bytes, &digest, "text/plain", "fault")
                    .await
                    .unwrap();
                assert_ne!(retry.selector, orphan.selector);
                let head = services
                    .event_store()
                    .stream_revision("session:fault")
                    .unwrap();
                services
                    .read_authorized_publication(&retry.selector, &digest, "fault", &[])
                    .await
                    .unwrap();
                assert_eq!(
                    services
                        .event_store()
                        .stream_revision("session:fault")
                        .unwrap(),
                    head
                );
            }
        }
        let services = crate::RuntimeServices::in_memory().unwrap();
        let before = services
            .event_store()
            .stream_revision("session:store-failure")
            .unwrap();
        let artifacts = services.artifact_store().stats().unwrap().artifacts;
        assert!(services
            .publish_authorized_content(b"body", "wrong-digest", "text/plain", "store-failure")
            .await
            .is_err());
        assert_eq!(
            services.artifact_store().stats().unwrap().artifacts,
            artifacts
        );
        assert_eq!(
            services
                .event_store()
                .stream_revision("session:store-failure")
                .unwrap(),
            before
        );
        let root = tempfile::tempdir().unwrap();
        let artifacts = Arc::new(
            crate::ArtifactStore::for_test(
                root.path().join("tiny"),
                crate::ArtifactStoreConfig {
                    compact_threshold_bytes: 1,
                    max_object_bytes: 1,
                    ..crate::ArtifactStoreConfig::default()
                },
            )
            .unwrap(),
        );
        let services = crate::RuntimeServices::builder(root.path().join("home"), root.path())
            .runtime_event_store(Arc::new(RuntimeEventStore::for_test()))
            .artifact_store(artifacts.clone())
            .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
            .build()
            .unwrap();
        let digest = format!("{:x}", Sha256::digest(b"too large"));
        assert!(services
            .publish_authorized_content(b"too large", &digest, "text/plain", "store-failure")
            .await
            .is_err());
        assert_eq!(artifacts.stats().unwrap().artifacts, 0);
        assert_eq!(
            services
                .event_store()
                .stream_revision("session:store-failure")
                .unwrap(),
            0
        );
    }
}
