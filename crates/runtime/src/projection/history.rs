use super::*;
use crate::DurableRuntimeEvent;
use harness_contract::projection::{ExecutionActivityProjection, ExecutionScopeProjection};
use serde::{Deserialize, Serialize};

const HISTORY_STATE_BYTES: usize = 4 * 1024 * 1024;
const HISTORY_REQUEST_RECORDS: usize = MAX_DELTA_BATCHES;
const HISTORY_REQUEST_BYTES: usize = 1024 * 1024;
pub(super) const INCOMPLETE: &str = "history incomplete: snapshot continuation required";

#[path = "history_prefix.rs"]
mod prefix;
pub(super) use prefix::HistoryPrefix;

struct PendingCheckpoint {
    schema: u64,
    source_fence: Option<crate::RuntimeSourceMutationFence>,
    projection_id: String,
    content_scope: String,
    identity: String,
    through: u64,
    expected_revision: u64,
    encoded: serde_json::Value,
}

struct PreparedCheckpointPayload {
    payload: serde_json::Value,
    proof: Option<crate::runtime_event_store::content::PreparedHistoryCheckpoint>,
}

pub(super) struct RequestBudget {
    records: usize,
    records_by_key: BTreeMap<(String, String, u64), Option<std::sync::Arc<DurableRuntimeEvent>>>,
    records_by_sequence:
        BTreeMap<(String, u64), BTreeMap<u64, std::sync::Arc<DurableRuntimeEvent>>>,
    resolved_records: BTreeMap<String, (DurableRuntimeEvent, std::sync::Arc<DurableRuntimeEvent>)>,
    stream_heads: BTreeMap<(String, u64), u64>,
    bytes: usize,
    exhausted: bool,
    unknown_body_read: bool,
    charged_records: BTreeMap<String, (DurableRuntimeEvent, usize)>,
    raw_graph_records: BTreeMap<String, (DurableRuntimeEvent, std::sync::Arc<serde_json::Value>)>,
    checkpoints: BTreeMap<String, PendingCheckpoint>,
    prepaid_checkpoints: BTreeMap<String, PreparedCheckpointPayload>,
    prepaid_heads: BTreeSet<String>,
    pub(super) request_source: Option<crate::RuntimeSourceMutationFence>,
    incremental: bool,
    incomplete: bool,
    scope_progress: bool,
    checkpoint_base_bytes: usize,
    checkpoint_records: usize,
    #[cfg(test)]
    prefix_stop: Option<serde_json::Value>,
    #[cfg(test)]
    r662_page_cap_after: Option<((u64, u32), usize)>,
    #[cfg(test)]
    r671_page_error_after: Option<((u64, u32), String)>,
    prefix_fences: BTreeMap<String, u64>,
    terminal_work: Option<TerminalWork>,
    terminal_deferred: bool,
    terminal_rechecks: BTreeSet<String>,
}


impl Default for RequestBudget {
    fn default() -> Self {
        Self {
            records: HISTORY_REQUEST_RECORDS,
            records_by_key: BTreeMap::new(),
            records_by_sequence: BTreeMap::new(),
            resolved_records: BTreeMap::new(),
            stream_heads: BTreeMap::new(),
            bytes: HISTORY_REQUEST_BYTES,
            exhausted: false,
            unknown_body_read: false,
            charged_records: BTreeMap::new(),
            raw_graph_records: BTreeMap::new(),
            checkpoints: BTreeMap::new(),
            prepaid_checkpoints: BTreeMap::new(),
            prepaid_heads: BTreeSet::new(),
            request_source: None,
            incremental: false,
            incomplete: false,
            scope_progress: false,
            checkpoint_base_bytes: 0,
            checkpoint_records: prefix::COMMIT_RECORDS,
            #[cfg(test)]
            prefix_stop: None,
            #[cfg(test)]
            r662_page_cap_after: None,
            #[cfg(test)]
            r671_page_error_after: None,
            prefix_fences: BTreeMap::new(),
            terminal_work: None,
            terminal_deferred: false,
            terminal_rechecks: BTreeSet::new(),
        }
    }
}

impl RequestBudget {
    pub(super) fn restart_after_cut_race(&mut self) -> Result<(), RuntimeServicesError> {
        self.charge(1, 0)?;
        let records = self.records;
        let bytes = self.bytes;
        *self = Self::default();
        self.records = records;
        self.bytes = bytes;
        self.enable_incremental();
        Ok(())
    }

    pub(super) fn terminal_active(&self) -> bool {
        self.terminal_work.is_some()
    }
    pub(super) fn terminal_preflight(
        &mut self,
        records: usize,
        bytes: usize,
    ) -> Result<(), RuntimeServicesError> {
        if let Some(work) = &self.terminal_work {
            if self.records < records.saturating_add(self.checkpoint_records)
                || self.bytes < bytes.saturating_add(work.reserve)
            {
                self.terminal_deferred = true;
                return Err(RuntimeServicesError::Invariant(INCOMPLETE.into()));
            }
        }
        Ok(())
    }

    fn terminal_limit(&self) -> usize {
        self.terminal_work.as_ref().map_or(self.bytes, |work| {
            self.bytes.saturating_sub(work.reserve)
        })
    }

    fn terminal_memo_update(
        &mut self, services: &RuntimeServices, memo: TerminalMemo,
    ) -> Result<(), RuntimeServicesError> {
        let work = self.terminal_work.as_ref().ok_or_else(checkpoint_error)?;
        let mut state = work.template.clone();
        state.terminal_pending = Some(memo.clone());
        let reserve = prefix::commit_reserve(services, &work.graph, &state, &work.prefix, &work.content_scope)?;
        let records = prefix::commit_record_reserve(&work.graph, &state, &work.prefix, &work.content_scope)?;
        if reserve > self.bytes || self.records < records {
            self.terminal_deferred = true;
            return Err(RuntimeServicesError::Invariant(INCOMPLETE.into()));
        }
        let work = self.terminal_work.as_mut().ok_or_else(checkpoint_error)?;
        self.checkpoint_records = records;
        work.reserve = reserve;
        work.memo = memo;
        Ok(())
    }

    fn terminal_remember(
        &mut self,
        services: &RuntimeServices,
        record: &DurableRuntimeEvent,
    ) -> Result<(), RuntimeServicesError> {
        if self.terminal_work.as_ref().is_none_or(|work| {
            work.memo.records.contains_key(&record.event_id)
        }) {
            return Ok(());
        }
        let mut memo = self.terminal_work.as_ref().ok_or_else(checkpoint_error)?.memo.clone();
        memo.records.insert(record.event_id.clone(), record.clone());
        self.terminal_memo_update(services, memo)
    }

    fn terminal_absent(&mut self, services: &RuntimeServices, stream: &str, key: &str) -> Result<(), RuntimeServicesError> {
        let identity = (stream.to_owned(), key.to_owned());
        if self.terminal_work.as_ref().is_some_and(|work| !work.memo.absent.contains(&identity)) {
            let mut memo = self.terminal_work.as_ref().ok_or_else(checkpoint_error)?.memo.clone();
            memo.absent.insert(identity);
            self.terminal_memo_update(services, memo)?;
        }
        Ok(())
    }

    fn terminal_resolved(
        &mut self, services: &RuntimeServices, original: &DurableRuntimeEvent, resolved: &DurableRuntimeEvent,
    ) -> Result<(), RuntimeServicesError> {
        if self.terminal_work.as_ref().is_none_or(|work| work.memo.resolved.contains_key(&original.event_id)) {
            return Ok(());
        }
        self.terminal_remember(services, original)?;
        let mut memo = self.terminal_work.as_ref().ok_or_else(checkpoint_error)?.memo.clone();
        memo.resolved.insert(original.event_id.clone(), (original.clone(), resolved.clone()));
        self.terminal_memo_update(services, memo)
    }

    fn begin_terminal(
        &mut self,
        state: &HistoryState,
        slot: &str,
        through: u64,
        reserve: usize,
        graph: &harness_contract::execution_graph::ExecutionGraphProjection,
        prefix: &HistoryPrefix,
        content_scope: &str,
    ) -> Result<(), RuntimeServicesError> {
        if self.terminal_work.is_some() || state.source_fence.is_none() || state.source_fence != self.request_source {
            return Err(checkpoint_error());
        }
        let identity = effect_proof_identity(state, slot)?;
        let mut memo = state.terminal_pending.clone().filter(|memo| {
            memo.slot == slot && memo.identity == identity && memo.through <= through
        }).unwrap_or_else(|| TerminalMemo {
            slot: slot.into(), identity: identity.clone(), through,
            ..Default::default()
        });
        if memo.through != through {
            memo.absent.clear();
            memo.fences.retain(|(_, _, fence)| fence.is_some());
            memo.through = through;
        }
        for (event_id, record) in &memo.records {
            if record.event_id.is_empty() || &record.event_id != event_id || record.commit_cursor > through {
                return Err(checkpoint_error());
            }
            self.remember_source_record(record, through)?;
        }
        for (stream, key) in &memo.absent {
            let identity = (stream.clone(), key.clone(), through);
            if self.records_by_key.get(&identity).is_some_and(Option::is_some) {
                return Err(checkpoint_error());
            }
            self.records_by_key.insert(identity, None);
        }
        for (event_id, (original, resolved)) in &memo.resolved {
            if memo.records.get(event_id) != Some(original)
                || original.event_id != *event_id || resolved.event_id != *event_id
            {
                return Err(checkpoint_error());
            }
            self.resolved_records.insert(event_id.clone(), (original.clone(), std::sync::Arc::new(resolved.clone())));
        }
        if memo.physical.keys().any(|event_id| !memo.records.contains_key(event_id)) {
            return Err(checkpoint_error());
        }
        self.terminal_deferred = false;
        let mut template = state.clone();
        template.terminal_pending = None;
        self.terminal_work = Some(TerminalWork {
            memo, reserve, template, graph: graph.clone(), prefix: prefix.clone(), content_scope: content_scope.into(),
        });
        Ok(())
    }

    fn finish_terminal(&mut self) -> Result<(TerminalMemo, bool), RuntimeServicesError> {
        let work = self.terminal_work.take().ok_or_else(checkpoint_error)?;
        let deferred = std::mem::take(&mut self.terminal_deferred);
        Ok((work.memo, deferred))
    }

    pub(super) fn terminal_fence(
        &mut self,
        services: &RuntimeServices,
        transaction: &str,
        stream: &str,
    ) -> Result<Option<crate::CommittedStreamRevision>, RuntimeServicesError> {
        if let Some(work) = &self.terminal_work {
            if let Some((_, _, fence)) = work.memo.fences.iter()
                .find(|(tx, source, _)| tx == transaction && source == stream)
            {
                return Ok(fence.clone());
            }
        }
        let extent = stream.len().saturating_add(transaction.len()).saturating_add(256);
        self.terminal_preflight(1, extent)?;
        self.charge(1, 0)?;
        let fence = services.event_store().transaction_stream_revision_exact(transaction, stream)
            .map_err(RuntimeServicesError::EventStore)?;
        self.charge_value(&fence)?;
        if self.terminal_work.is_some() {
            let mut memo = self.terminal_work.as_ref().ok_or_else(checkpoint_error)?.memo.clone();
            memo.fences.push((transaction.into(), stream.into(), fence.clone()));
            self.terminal_memo_update(services, memo)?;
        }
        Ok(fence)
    }

    pub(super) fn terminal_physical(&self, event_id: &str) -> Option<bool> {
        self.terminal_work.as_ref().and_then(|work| work.memo.physical.get(event_id).copied())
    }

    pub(super) fn terminal_physical_verified(
        &mut self, services: &RuntimeServices, event_id: &str, closed: bool,
    ) -> Result<(), RuntimeServicesError> {
        if self.terminal_work.is_some() && self.terminal_physical(event_id).is_none() {
            let mut memo = self.terminal_work.as_ref().ok_or_else(checkpoint_error)?.memo.clone();
            memo.physical.insert(event_id.into(), closed);
            self.terminal_memo_update(services, memo)?;
        }
        Ok(())
    }

    pub(super) fn prepay_terminal_recheck(
        &mut self, read: &settlement::ClosureRead,
    ) -> Result<bool, RuntimeServicesError> {
        let work = read.recheck_work()?;
        let records = work.iter().filter(|(identity, _, _)| !self.terminal_rechecks.contains(identity))
            .map(|(_, records, _)| records).copied().sum::<usize>();
        let bytes = work.iter().filter(|(identity, _, _)| !self.terminal_rechecks.contains(identity))
            .map(|(_, _, bytes)| bytes).copied().sum::<usize>();
        if self.records < records.saturating_add(self.checkpoint_records) || self.bytes < bytes {
            return Ok(false);
        }
        self.charge(records, bytes)?;
        self.terminal_rechecks.extend(work.into_iter().map(|(identity, _, _)| identity));
        Ok(true)
    }

    pub(super) fn take_terminal_recheck(&mut self, identity: &str) -> bool {
        self.terminal_rechecks.remove(identity)
    }

    pub(super) fn enable_incremental(&mut self) {
        self.incremental = true;
    }

    pub(super) fn is_incomplete(&self) -> bool {
        self.incomplete
    }

    pub(super) fn has_pending_checkpoints(&self) -> bool {
        !self.checkpoints.is_empty()
    }

    pub(super) fn has_pending_history_checkpoint(&self, execution_id: &str) -> bool {
        self.checkpoints.contains_key(&format!("projection-history:{execution_id}"))
    }

    #[cfg(test)]
    pub(super) fn consumed_work(&self) -> (usize, usize) {
        (HISTORY_REQUEST_RECORDS - self.records, HISTORY_REQUEST_BYTES - self.bytes)
    }

    pub(super) fn page_limits(&self) -> (usize, usize) {
        (
            self.records.min(100).max(1),
            self.bytes.min(HISTORY_REQUEST_BYTES).max(1),
        )
    }

    pub(super) fn stream_head(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
        through: u64,
    ) -> Result<u64, RuntimeServicesError> {
        self.charge(0, 0)?;
        let identity = (stream.to_string(), through);
        if let Some(head) = self.stream_heads.get(&identity) {
            return Ok(*head);
        }
        let head = self.uncached_stream_head(services, stream)?;
        self.stream_heads.insert(identity, head);
        Ok(head)
    }

    pub(super) fn uncached_stream_head(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
    ) -> Result<u64, RuntimeServicesError> {
        self.terminal_preflight(1, stream.len().saturating_add(20))?;
        self.charge(1, stream.len())?;
        let head = services.event_store().stream_revision(stream)?;
        self.charge_value(&head)?;
        Ok(head)
    }

    pub(super) fn canonical_record(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
        key: &str,
        through: u64,
    ) -> Result<Option<std::sync::Arc<DurableRuntimeEvent>>, RuntimeServicesError> {
        self.charge(0, 0)?;
        let identity = (stream.to_string(), key.to_string(), through);
        if let Some(record) = self.records_by_key.get(&identity) {
            let record = record.clone();
            if let Some(record) = &record {
                self.terminal_remember(services, record)?;
            } else {
                self.terminal_absent(services, stream, key)?;
            }
            return Ok(record);
        }
        let lookup_bytes = stream.len().saturating_add(key.len()).saturating_add(8);
        self.terminal_preflight(3, lookup_bytes.saturating_add(256))?;
        if self.records < 3 {
            self.charge(3, 0)?;
        }
        self.charge(1, stream.len().saturating_add(key.len()).saturating_add(8))?;
        let limit = self.terminal_limit();
        let record = match services.event_store().bounded_event_by_idempotency_key(stream, key, through, limit)? {
            crate::RuntimeBoundedEventLookup::Absent => None,
            crate::RuntimeBoundedEventLookup::Deferred { required_bytes } => {
                if required_bytes <= limit {
                    return Err(RuntimeServicesError::Invariant("invalid bounded canonical deferral".into()));
                }
                if self.terminal_work.is_some() {
                    self.terminal_deferred = true;
                    return Err(RuntimeServicesError::Invariant(INCOMPLETE.into()));
                }
                self.charge(0, required_bytes)?;
                return Err(checkpoint_error());
            }
            crate::RuntimeBoundedEventLookup::Record { record, encoded_bytes } => {
                // The lookup already charged its operation unit; the returned
                // record is charged once by charge_record, exactly like a
                // listed record. Charging it twice here drained the shared
                // request budget before the caller could finish the scope.
                let metadata = serde_json::to_vec(&record).map_err(|_| checkpoint_error())?.len();
                self.charge_record(&record)?;
                if metadata != encoded_bytes || metadata > limit || record.stream_id != stream
                    || record.idempotency_key.as_deref() != Some(key) || record.commit_cursor > through {
                    return Err(RuntimeServicesError::Invariant("bounded canonical event identity or extent mismatch".into()));
                }
                Some(std::sync::Arc::new(*record))
            }
        };
        self.records_by_key.insert(identity, record.clone());
        if let Some(record) = &record {
            self.remember_sequence(record, through)?;
            self.terminal_remember(services, record)?;
        } else {
            self.terminal_absent(services, stream, key)?;
        }
        Ok(record)
    }

    fn admit_canonical_record(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
        key: &str,
        through: u64,
        reserve: usize,
    ) -> Result<bool, RuntimeServicesError> {
        self.charge(0, 0)?;
        let identity = (stream.to_owned(), key.to_owned(), through);
        if self.records_by_key.contains_key(&identity) {
            return Ok(true);
        }
        let lookup_bytes = stream.len().saturating_add(key.len()).saturating_add(8);
        if self.records <= self.checkpoint_records + 3 || self.bytes <= reserve.saturating_add(lookup_bytes) {
            return Ok(false);
        }
        self.charge(1, lookup_bytes)?;
        let limit = self.bytes.saturating_sub(reserve);
        match services.event_store().bounded_event_by_idempotency_key(stream, key, through, limit)? {
            crate::RuntimeBoundedEventLookup::Deferred { required_bytes } => {
                if required_bytes <= limit {
                    return Err(RuntimeServicesError::Invariant("invalid bounded source deferral".into()));
                }
                Ok(false)
            }
            crate::RuntimeBoundedEventLookup::Absent => {
                self.records_by_key.insert(identity, None);
                Ok(true)
            }
            crate::RuntimeBoundedEventLookup::Record { record, encoded_bytes } => {
                // Same shared-counter rule as the canonical lookup above: one
                // unit for the lookup, one for the record inside charge_record.
                let actual = self.charge_record(&record)?;
                if actual != encoded_bytes || actual > limit || record.stream_id != stream
                    || record.idempotency_key.as_deref() != Some(key) || record.commit_cursor > through
                {
                    return Err(RuntimeServicesError::Invariant("bounded canonical source identity or extent mismatch".into()));
                }
                let record = std::sync::Arc::new(*record);
                self.remember_sequence(&record, through)?;
                self.records_by_key.insert(identity, Some(record));
                Ok(true)
            }
        }
    }

    fn admit_effect_head(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
        through: u64,
        reserve: usize,
    ) -> Result<bool, RuntimeServicesError> {
        self.charge(0, 0)?;
        if self.stream_heads.contains_key(&(stream.to_owned(), through)) {
            return Ok(true);
        }
        if self.records <= self.checkpoint_records || self.bytes < reserve.saturating_add(stream.len()).saturating_add(20) {
            return Ok(false);
        }
        self.stream_head(services, stream, through)?;
        Ok(true)
    }

    fn remember_sequence(
        &mut self,
        record: &DurableRuntimeEvent,
        through: u64,
    ) -> Result<(), RuntimeServicesError> {
        if record.commit_cursor > through {
            return Err(RuntimeServicesError::Invariant("canonical event is ahead of request cut".into()));
        }
        let stream = self.records_by_sequence.entry((record.stream_id.clone(), through)).or_default();
        if let Some(previous) = stream.get(&record.sequence) {
            if previous.as_ref() != record {
                return Err(RuntimeServicesError::Invariant("canonical stream sequence identity changed".into()));
            }
        } else {
            stream.insert(record.sequence, std::sync::Arc::new(record.clone()));
        }
        Ok(())
    }

    pub(super) fn remember_record(
        &mut self,
        record: &DurableRuntimeEvent,
        through: u64,
    ) -> Result<(), RuntimeServicesError> {
        self.charge(0, 0)?;
        if record.scope == RuntimeEventScope::ExecutionGraph || record.kind == "goal.observation" {
            return Ok(());
        }
        self.remember_source_record(record, through)
    }

    fn remember_source_record(
        &mut self,
        record: &DurableRuntimeEvent,
        through: u64,
    ) -> Result<(), RuntimeServicesError> {
        self.charge(0, 0)?;
        self.remember_sequence(record, through)?;
        let Some(key) = record.idempotency_key.as_ref() else {
            return Ok(());
        };
        if record.commit_cursor > through {
            return Err(RuntimeServicesError::Invariant(
                "canonical event is ahead of request cut".into(),
            ));
        }
        let identity = (record.stream_id.clone(), key.clone(), through);
        if let Some(previous) = self.records_by_key.get(&identity) {
            if previous.as_deref() != Some(record) {
                return Err(RuntimeServicesError::Invariant(
                    "canonical event changed within request".into(),
                ));
            }
        } else {
            self.records_by_key
                .insert(identity, Some(std::sync::Arc::new(record.clone())));
        }
        Ok(())
    }

    pub(super) fn canonical_stream_page(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
        after: u64,
        head: u64,
        through: u64,
    ) -> Result<Vec<std::sync::Arc<DurableRuntimeEvent>>, RuntimeServicesError> {
        self.charge(0, 0)?;
        if after >= head {
            return Ok(Vec::new());
        }
        let empty = BTreeMap::new();
        let cached = self.records_by_sequence.get(&(stream.to_string(), through)).unwrap_or(&empty);
        let mut page = Vec::new();
        let mut next = after
            .checked_add(1)
            .ok_or_else(|| RuntimeServicesError::Invariant("stream position overflow".into()))?;
        while next <= head && page.len() < 100 {
            let Some(record) = cached.get(&next) else {
                break;
            };
            page.push(record.clone());
            next = next.checked_add(1).ok_or_else(|| {
                RuntimeServicesError::Invariant("stream position overflow".into())
            })?;
        }
        if !page.is_empty() {
            for record in &page {
                self.terminal_remember(services, record)?;
            }
            return Ok(page);
        }
        let until_known = cached
            .range(next..)
            .next()
            .and_then(|(sequence, _)| sequence.checked_sub(next))
            .and_then(|distance| usize::try_from(distance).ok());
        self.terminal_preflight(2, 1024)?;
        let (mut records, mut bytes) = self.page_limits();
        if self.terminal_work.is_some() {
            records = 1;
            bytes = self.terminal_limit();
        }
        let until_known = until_known.unwrap_or(records);
        self.charge(1, 0)?;
        let result = services
            .event_store()
            .list_stream_after(stream, after, head, records.min(until_known).max(1), bytes);
        let records = match result {
            Ok(records) => records,
            Err(message) if self.terminal_work.is_some()
                && message.starts_with("HistoryBudgetExceeded: ")
                && bytes < HISTORY_REQUEST_BYTES =>
            {
                self.terminal_deferred = true;
                return Err(RuntimeServicesError::Invariant(INCOMPLETE.into()));
            }
            Err(message) => return Err(error(message)),
        };
        let mut page = Vec::with_capacity(records.len());
        for record in records {
            self.charge_record(&record)?;
            if record.commit_cursor <= through {
                self.remember_record(&record, through)?;
                self.terminal_remember(services, &record)?;
            }
            page.push(std::sync::Arc::new(record));
        }
        Ok(page)
    }

    #[track_caller]
    pub(super) fn charge(
        &mut self,
        records: usize,
        bytes: usize,
    ) -> Result<(), RuntimeServicesError> {
        if self.exhausted || records > self.records || bytes > self.bytes {
            self.exhausted = true;
            self.checkpoints.clear();
            self.prepaid_checkpoints.clear();
            self.prepaid_heads.clear();
            self.request_source = None;
            self.records_by_key.clear();
            self.records_by_sequence.clear();
            self.resolved_records.clear();
            self.charged_records.clear();
            self.raw_graph_records.clear();
            self.stream_heads.clear();
            self.prefix_fences.clear();
            self.terminal_work = None;
            self.terminal_rechecks.clear();
            return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
                format!("projection request work: requested_records={records}, remaining_records={}, requested_bytes={bytes}, remaining_bytes={}, caller={}", self.records, self.bytes, std::panic::Location::caller()),
            )
            .into());
        }
        self.records -= records;
        self.bytes -= bytes;
        Ok(())
    }

    #[track_caller]
    pub(super) fn charge_value<T: Serialize>(
        &mut self,
        value: &T,
    ) -> Result<usize, RuntimeServicesError> {
        let bytes = serde_json::to_vec(value)
            .map_err(|_| RuntimeServicesError::Invariant("projection work encoding failed".into()))?
            .len();
        self.charge(0, bytes)?;
        Ok(bytes)
    }

    fn charge_envelope(
        &mut self,
        envelope: &serde_json::Value,
    ) -> Result<usize, RuntimeServicesError> {
        if let Some(bytes) = envelope["bytes"].as_u64() {
            if bytes > HISTORY_REQUEST_BYTES as u64 {
                self.charge(0, HISTORY_REQUEST_BYTES + 1)?;
            }
            if envelope["body"]["kind"].as_str() != Some("inline") {
                let bytes = usize::try_from(bytes).map_err(|_| {
                    crate::RuntimeEventStoreError::HistoryBudgetExceeded(
                        "projection body size".into(),
                    )
                })?;
                self.charge(0, bytes)?;
                return Ok(bytes);
            }
        } else if envelope["body"]["kind"].as_str() != Some("inline") {
            if self.unknown_body_read {
                self.charge(0, HISTORY_REQUEST_BYTES + 1)?;
            }
            self.unknown_body_read = true;
            // Unknown external extents must reserve the entire legal body before IO.
            self.charge(0, HISTORY_REQUEST_BYTES)?;
            return Ok(HISTORY_REQUEST_BYTES);
        }
        Ok(0)
    }

    #[track_caller]
    pub(super) fn charge_record(
        &mut self,
        record: &DurableRuntimeEvent,
    ) -> Result<usize, RuntimeServicesError> {
        self.charge(0, 0)?;
        if let Some((original, prepaid)) = self.charged_records.get(&record.event_id) {
            if original != record {
                return Err(RuntimeServicesError::Invariant(
                    "charged event identity changed within request".into(),
                ));
            }
            return Ok(*prepaid);
        }
        self.charge(1, 0)?;
        let metadata = self.charge_value(record)?;
        let body = if record.scope == RuntimeEventScope::ExecutionGraph {
            self.charge_envelope(&record.payload)?
        } else if record.kind == "goal.observation" {
            self.charge_envelope(&record.payload["observation_body"])?
        } else {
            0
        };
        let prepaid = metadata + body;
        self.charged_records
            .insert(record.event_id.clone(), (record.clone(), prepaid));
        Ok(prepaid)
    }

    pub(super) fn resolve_record(
        &mut self,
        services: &RuntimeServices,
        record: DurableRuntimeEvent,
    ) -> Result<DurableRuntimeEvent, RuntimeServicesError> {
        self.charge(0, 0)?;
        if let Some((original, resolved)) = self.resolved_records.get(&record.event_id) {
            if original != &record {
                return Err(RuntimeServicesError::Invariant(
                    "resolved event identity changed within request".into(),
                ));
            }
            let resolved = std::sync::Arc::clone(resolved);
            self.charge(0, 0)?;
            if !self.charged_records.contains_key(&record.event_id) {
                // The checkpoint memo prepared this body in an earlier request
                // and already paid for it, so this request bills only the
                // record identity and its metadata - the same units the
                // canonical walk pays before it resolves. Defer before
                // charging when the shared budget cannot hold them, so a
                // restore can never overdraw the request.
                let metadata = serde_json::to_vec(&record)
                    .map_err(|_| checkpoint_error())?
                    .len();
                self.terminal_preflight(0, metadata.saturating_add(512))?;
                self.charge(1, 0)?;
                self.charge_value(&record)?;
                self.charged_records
                    .insert(record.event_id.clone(), (record.clone(), metadata));
            }
            let resolved = resolved.as_ref().clone();
            self.terminal_resolved(services, &record, &resolved)?;
            return Ok(resolved);
        }
        let body = record.payload["bytes"].as_u64().unwrap_or(0);
        if self.terminal_work.is_some()
            && record.scope == RuntimeEventScope::ExecutionGraph
            && record.payload["body"]["kind"].as_str() != Some("inline")
            && record.payload["bytes"].as_u64().is_none()
        {
            return Err(checkpoint_error());
        }
        let extent = usize::try_from(body)
            .map_err(|_| checkpoint_error())?
            .saturating_add(
                serde_json::to_vec(&record)
                    .map_err(|_| checkpoint_error())?
                    .len(),
            );
        self.terminal_preflight(0, extent.saturating_add(512))?;
        let prepaid = self.charge_record(&record)?;
        self.resolve_prepaid_record(services, &record, prepaid)
    }

    pub(super) fn resolve_prepaid_record(
        &mut self,
        services: &RuntimeServices,
        record: &DurableRuntimeEvent,
        prepaid: usize,
    ) -> Result<DurableRuntimeEvent, RuntimeServicesError> {
        self.charge(0, 0)?;
        if let Some((original, resolved)) = self.resolved_records.get(&record.event_id) {
            if original != record {
                return Err(RuntimeServicesError::Invariant(
                    "resolved event identity changed within request".into(),
                ));
            }
            let resolved = resolved.as_ref().clone();
            self.terminal_resolved(services, record, &resolved)?;
            return Ok(resolved);
        }
        let envelope = if record.kind == "goal.observation" {
            &record.payload["observation_body"]
        } else {
            &record.payload
        };
        let body = envelope["bytes"].as_u64().unwrap_or(0);
        if self.terminal_work.is_some()
            && (record.scope == RuntimeEventScope::ExecutionGraph
                || record.kind == "goal.observation")
            && envelope["body"]["kind"].as_str() != Some("inline")
            && envelope["bytes"].as_u64().is_none()
        {
            return Err(checkpoint_error());
        }
        let extent = usize::try_from(body)
            .map_err(|_| checkpoint_error())?
            .saturating_add(
                serde_json::to_vec(record)
                    .map_err(|_| checkpoint_error())?
                    .len(),
            );
        self.terminal_preflight(0, extent.saturating_add(1024))?;
        let event = if record.scope == RuntimeEventScope::ExecutionGraph {
            let raw = self.resolve_raw_graph_record(services, record)?;
            services
                .event_store()
                .hydrate_graph_record(record.clone(), raw)
                .map_err(error)?
        } else {
            services
                .event_store()
                .resolve_read_record_within(
                    record.clone(),
                    crate::RuntimeEventStore::MAX_HYDRATED_CONTENT_BYTES,
                )
                .map_err(error)?
        };
        let actual = serde_json::to_vec(&event)
            .map_err(|_| {
                RuntimeServicesError::Invariant("projection event encoding failed".into())
            })?
            .len();
        self.charge(0, actual.saturating_sub(prepaid))?;
        self.resolved_records.insert(
            record.event_id.clone(),
            (record.clone(), std::sync::Arc::new(event.clone())),
        );
        self.terminal_resolved(services, record, &event)?;
        Ok(event)
    }

    /// Raw graph content and hydrated events have distinct memo entries.
    pub(super) fn resolve_raw_graph_record(
        &mut self,
        services: &RuntimeServices,
        record: &DurableRuntimeEvent,
    ) -> Result<serde_json::Value, RuntimeServicesError> {
        self.charge(0, 0)?;
        if let Some((original, raw)) = self.raw_graph_records.get(&record.event_id) {
            if original != record {
                return Err(RuntimeServicesError::Invariant(
                    "raw graph identity changed within request".into(),
                ));
            }
            return Ok(raw.as_ref().clone());
        }
        let prepaid = self.charge_record(record)?;
        self.terminal_preflight(0, 512)?;
        let raw = services
            .event_store()
            .resolve_graph_record(record)
            .map_err(error)?;
        let size = serde_json::to_vec(&raw)
            .map_err(|_| checkpoint_error())?
            .len();
        if size > HISTORY_REQUEST_BYTES {
            return Err(checkpoint_error());
        }
        self.raw_graph_records.insert(
            record.event_id.clone(),
            (record.clone(), std::sync::Arc::new(raw.clone())),
        );
        Ok(raw)
    }

    pub(super) fn read_auth_can_scan(&self) -> bool {
        !self.exhausted && self.records >= 96 && self.bytes >= 256 * 1024
    }

    /// Caller limits constrain the physical lookup, including cache hits.
    /// Cursor filtering is deliberately the consumer's responsibility.
    pub(super) fn limited_source_page(
        &mut self,
        services: &RuntimeServices,
        stream: &str,
        after: u64,
        head: u64,
        limit: usize,
        bytes: usize,
        through: u64,
    ) -> Result<Vec<std::sync::Arc<DurableRuntimeEvent>>, RuntimeServicesError> {
        self.charge(0, 0)?;
        if after >= head {
            return Ok(Vec::new());
        }
        if limit == 0 || bytes == 0 {
            return Err(checkpoint_error());
        }
        let limit = limit.min(self.records.saturating_sub(1));
        let bytes = bytes.min(self.bytes);
        if limit == 0 || bytes == 0 {
            self.charge(HISTORY_REQUEST_RECORDS + 1, 0)?;
        }
        self.terminal_preflight(2, bytes.min(1024))?;
        self.charge(1, 0)?;
        let records = services
            .event_store()
            .list_stream_after(stream, after, head, limit, bytes)
            .map_err(error)?;
        if records.len() > limit {
            return Err(checkpoint_error());
        }
        let mut next = after;
        let mut encoded = 0usize;
        let mut output = Vec::new();
        for record in records {
            next = next.checked_add(1).ok_or_else(checkpoint_error)?;
            encoded = encoded.saturating_add(
                serde_json::to_vec(&record)
                    .map_err(|_| checkpoint_error())?
                    .len(),
            );
            if record.stream_id != stream
                || record.sequence != next
                || record.sequence > head
                || encoded > bytes
            {
                return Err(checkpoint_error());
            }
            self.charge_record(&record)?;
            if record.commit_cursor <= through {
                self.remember_sequence(&record, through)?;
            }
            output.push(std::sync::Arc::new(record));
        }
        Ok(output)
    }

    pub(super) fn read_auth_fence(
        &mut self,
        services: &RuntimeServices,
        transaction: &str,
        stream: &str,
        through: u64,
    ) -> Result<Option<crate::CommittedStreamRevision>, RuntimeServicesError> {
        let mut expected = None;
        for (record, _) in self.charged_records.values() {
            if record.transaction_id == transaction
                && record.commit_cursor <= through
                && matches!(
                    record.kind.as_str(),
                    "execution.read.intent" | "execution.effect.intent"
                )
                && record.payload["root_effect_owner"]["binding"]["root_execution_id"].as_str()
                    == Some(stream)
            {
                let revision = record.payload["root_effect_owner"]["graph_revision"]
                    .as_u64()
                    .ok_or_else(checkpoint_error)?;
                if expected
                    .replace(revision)
                    .is_some_and(|old| old != revision)
                {
                    return Err(checkpoint_error());
                }
            }
        }
        let expected = expected.ok_or_else(|| {
            RuntimeServicesError::Invariant("read fence lacks its bounded original intent".into())
        })?;
        let fence = self.terminal_fence(services, transaction, stream)?;
        if fence
            .as_ref()
            .is_some_and(|f| f.stream_id != stream || f.expected_revision != expected)
        {
            return Err(checkpoint_error());
        }
        Ok(fence)
    }
    pub(super) fn capture_source_fence(
        &mut self,
        services: &RuntimeServices,
    ) -> Result<crate::RuntimeSourceMutationFence, RuntimeServicesError> {
        self.charge(0, 0)?;
        if let Some(source) = &self.request_source {
            return Ok(source.clone());
        }
        let source = self.read_source_fence(services)?;
        self.request_source = Some(source.clone());
        Ok(source)
    }

    pub(super) fn require_captured_source(
        &mut self,
        services: &RuntimeServices,
    ) -> Result<(), RuntimeServicesError> {
        self.charge(0, 0)?;
        if let Some(source) = self.request_source.clone() {
            self.require_source_fence(services, &source)?;
        }
        Ok(())
    }

    pub(super) fn cached_source_current(
        &mut self,
        services: &RuntimeServices,
        execution: &str,
        through: u64,
    ) -> Result<bool, RuntimeServicesError> {
        let source = self.capture_source_fence(services)?;
        self.charge(1, 0)?;
        let checkpoint = services.event_store().projection_checkpoint(
            &format!("projection-history:{execution}"),
        )?;
        self.charge_value(&checkpoint)?;
        let expected = serde_json::to_value(&source).map_err(|_| checkpoint_error())?;
        let valid = checkpoint.is_some_and(|saved| saved.source_cursor <= through
            && saved.payload["schema"] == 6 && saved.payload["incomplete"] == false
            && saved.payload["source_fence"] == expected);
        if valid {
            self.require_source_fence(services, &source)?;
        }
        Ok(valid)
    }

    fn read_source_fence(
        &mut self,
        services: &RuntimeServices,
    ) -> Result<crate::RuntimeSourceMutationFence, RuntimeServicesError> {
        self.charge(1, 0)?;
        let fence = services.event_store().source_mutation_fence()?;
        self.charge_value(&fence)?;
        if fence.incarnation.is_empty() {
            return Err(RuntimeServicesError::Invariant("empty source incarnation".into()));
        }
        Ok(fence)
    }

    fn require_source_fence(
        &mut self,
        services: &RuntimeServices,
        expected: &crate::RuntimeSourceMutationFence,
    ) -> Result<(), RuntimeServicesError> {
        if self.read_source_fence(services)? != *expected {
            return Err(RuntimeServicesError::Invariant("history source mutation fence changed".into()));
        }
        Ok(())
    }

    fn prepare_checkpoint_payload(
        &mut self,
        services: &RuntimeServices,
        pending: &PendingCheckpoint,
    ) -> Result<PreparedCheckpointPayload, RuntimeServicesError> {
        let identity = format!("{}:{}:{}", pending.projection_id, pending.through, pending.identity);
        if pending.schema == 6 {
            let (records, bytes) = crate::runtime_event_store::content::history_checkpoint_metadata_reserve(&pending.encoded)
                .map_err(error)?;
            self.charge(records, bytes)?;
            let proof = services.event_store().prepare_history_checkpoint(
                &pending.projection_id, pending.through, pending.expected_revision, &identity,
                Self::checkpoint_payload(pending, serde_json::Value::Null)?, &pending.encoded,
            )?;
            let payload = proof.payload().clone();
            let fence = pending.source_fence.as_ref().ok_or_else(checkpoint_error)?;
            self.charge(1, serde_json::to_vec(fence).map_err(|_| checkpoint_error())?.len())?;
            self.charge_value(&payload)?;
            if payload["body"]["bytes"].as_u64() != Some(serde_json::to_vec(&pending.encoded)
                .map_err(|_| checkpoint_error())?.len() as u64)
            {
                return Err(checkpoint_error());
            }
            return Ok(PreparedCheckpointPayload { payload, proof: Some(proof) });
        }
        let body = services.event_store().prepare_content(
            &pending.content_scope, &identity, &pending.encoded,
            &crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id: pending.projection_id.clone(), source_cursor: pending.through,
            },
        ).map_err(error)?;
        Ok(PreparedCheckpointPayload { payload: Self::checkpoint_payload(pending, body)?, proof: None })
    }

    fn checkpoint_payload(
        pending: &PendingCheckpoint,
        body: serde_json::Value,
    ) -> Result<serde_json::Value, RuntimeServicesError> {
        let mut payload =
            serde_json::json!({"schema":pending.schema,"scope":pending.content_scope,"body":body});
        if pending.schema == 6 {
            let fence = pending.source_fence.as_ref().ok_or_else(|| RuntimeServicesError::Invariant(
                "schema6 source fence missing".into()))?;
            payload["source_fence"] = serde_json::to_value(fence).map_err(|_| checkpoint_error())?;
            payload["target_cursor"] = serde_json::json!(pending.through);
            payload["verified_through"] = pending.encoded["state"]["through"].clone();
            payload["incomplete"] = serde_json::json!(pending.encoded["state"]["incomplete"]
                .as_bool()
                .unwrap_or(false));
        }
        Ok(payload)
    }

    fn publication_records(pending: &PendingCheckpoint) -> Result<usize, RuntimeServicesError> {
        let (records, _) = crate::runtime_event_store::content::history_checkpoint_metadata_reserve(&pending.encoded)
            .map_err(error)?;
        let stage_source_fence = 1;
        let encoded_state = 1;
        let graph_head_checks = 2;
        let prepared_payload_fence = 1;
        let scope_and_consumer_fences = 2;
        Ok(records.saturating_add(stage_source_fence + encoded_state + graph_head_checks
            + prepared_payload_fence + scope_and_consumer_fences))
    }

    fn publication_reserve(
        services: &RuntimeServices,
        pending: &PendingCheckpoint,
        stream: &str,
    ) -> Result<usize, RuntimeServicesError> {
        let encoded = serde_json::to_vec(&pending.encoded).map_err(|_| checkpoint_error())?.len();
        let body = services.event_store().prepared_content_wire_bytes(&pending.content_scope, &pending.encoded)
            .map_err(error)?;
        let wrapper = serde_json::to_vec(&Self::checkpoint_payload(pending, serde_json::Value::Null)?)
            .map_err(|_| checkpoint_error())?.len().saturating_sub(4).saturating_add(body);
        let fence = serde_json::to_vec(pending.source_fence.as_ref().ok_or_else(checkpoint_error)?)
            .map_err(|_| checkpoint_error())?.len();
        let (_, metadata_bytes) = crate::runtime_event_store::content::history_checkpoint_metadata_reserve(&pending.encoded)
            .map_err(error)?;
        Ok(encoded.saturating_add(metadata_bytes).saturating_add(wrapper)
            .saturating_add(fence.saturating_mul(4))
            .saturating_add(stream.len().saturating_add(20).saturating_mul(2)))
    }

    fn prepay_checkpoint_publication(
        &mut self,
        services: &RuntimeServices,
        pending: &PendingCheckpoint,
        stream: &str,
    ) -> Result<(), RuntimeServicesError> {
        let payload = self.prepare_checkpoint_payload(services, pending)?;
        if !self.prepaid_heads.contains(stream) {
            self.charge(1, stream.len().saturating_add(20))?;
            self.prepaid_heads.insert(stream.to_owned());
        }
        self.prepaid_checkpoints
            .insert(pending.projection_id.clone(), payload);
        Ok(())
    }

    pub(super) fn publish(
        &mut self,
        services: &RuntimeServices,
    ) -> Result<(), RuntimeServicesError> {
        self.charge(0, 0)?;
        let mut prepared = Vec::new();
        for pending in std::mem::take(&mut self.checkpoints).into_values() {
            let payload = match self.prepaid_checkpoints.remove(&pending.projection_id) {
                Some(payload) if pending.schema == 6 => payload,
                _ => self.prepare_checkpoint_payload(services, &pending)?,
            };
            prepared.push((pending, payload));
        }
        for (stream, revision) in self.prefix_fences.clone() {
            let head = if self.prepaid_heads.remove(&stream) {
                services.event_store().stream_revision(&stream)?
            } else {
                self.uncached_stream_head(services, &stream)?
            };
            if head != revision {
                return Err(RuntimeServicesError::Invariant("history prefix source changed before publication".into()));
            }
        }
        self.charge(0, 0)?;
        for (pending, payload) in prepared {
            let result = if let Some(fence) = pending.source_fence.as_ref() {
                if pending.schema == 6 {
                    services.event_store().publish_prepared_history_checkpoint(
                        payload.proof.ok_or_else(checkpoint_error)?, fence,
                    )
                } else {
                    services.event_store().compare_and_put_projection_checkpoint_fenced(
                        &pending.projection_id, pending.through, pending.expected_revision,
                        &payload.payload, 0, fence,
                    )
                }
            } else if pending.schema == 6 {
                return Err(RuntimeServicesError::Invariant("schema6 source fence missing".into()));
            } else {
                services.event_store().compare_and_put_projection_checkpoint(
                    &pending.projection_id, pending.through, pending.expected_revision, &payload.payload, 0,
                )
            };
            match result {
                Ok(_) => {}
                Err(crate::RuntimeEventStoreError::StaleRevision { .. }) if pending.schema != 6 => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct NodeFacts {
    pub first: Option<u64>,
    pub last: Option<u64>,
    pub cursor: u64,
    pub label: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(super) struct HistoryState {
    pub identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_fence: Option<crate::RuntimeSourceMutationFence>,
    pub position: Option<(u64, u32)>,
    pub through: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<HistoryPrefix>,
    #[serde(skip)]
    pub incomplete: bool,
    pub first: Option<u64>,
    pub last: Option<u64>,
    pub nodes: BTreeMap<String, NodeFacts>,
    pub activities: BTreeMap<String, ExecutionActivityProjection>,
    pub inputs: BTreeSet<(String, String)>,
    pub health: BTreeMap<String, ProjectionEntity>,
    pub entities_full: BTreeMap<String, BTreeMap<String, ProjectionEntity>>,
    pub entities_summary: BTreeMap<String, BTreeMap<String, ProjectionEntity>>,
    pub strategy: Vec<DurableRuntimeEvent>,
    pub detail_inputs:
        BTreeMap<String, harness_contract::projection::ExecutionActivityContentProjection>,
    pub detail_outputs:
        BTreeMap<String, harness_contract::projection::ExecutionActivityContentProjection>,
    pub detail_entities: BTreeMap<String, BTreeMap<String, ProjectionEntity>>,
    pub cancellation: Option<harness_contract::turn::CancellationReceipt>,
    pub bindings: BTreeMap<String, (String, crate::execution_core::graph::ExecutionNodeBinding)>,
    pub graph_state: Option<harness_contract::execution_graph::ExecutionGraph>,
    pub start_tickets: BTreeMap<String, crate::execution_core::graph::NodeExecutionTicket>,
    pub start_specs: BTreeMap<String, harness_contract::execution_graph::ExecutionNodeSpec>,
    pub start_positions: BTreeMap<String, (u64, u32)>,
    #[serde(skip)]
    pub effect_evidence_refs: Vec<String>,
    #[serde(skip)]
    pub effect_missing_receipt: bool,
    #[serde(skip)]
    pub effects_checked_through: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub effect_proofs: BTreeMap<String, HistoryEffectProof>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub terminal_proofs: BTreeMap<String, TerminalBindingProof>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_pending: Option<TerminalMemo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TerminalBindingProof {
    pub(super) identity: String,
    pub(super) through: u64,
    pub(super) read: settlement::ClosureRead,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TerminalMemo {
    pub(super) slot: String,
    pub(super) identity: String,
    pub(super) through: u64,
    records: BTreeMap<String, DurableRuntimeEvent>,
    absent: BTreeSet<(String, String)>,
    resolved: BTreeMap<String, (DurableRuntimeEvent, DurableRuntimeEvent)>,
    fences: Vec<(String, String, Option<crate::CommittedStreamRevision>)>,
    physical: BTreeMap<String, bool>,
}

struct TerminalWork {
    memo: TerminalMemo,
    reserve: usize,
    template: HistoryState,
    graph: harness_contract::execution_graph::ExecutionGraphProjection,
    prefix: HistoryPrefix,
    content_scope: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HistoryEffectProof {
    pub(super) identity: String,
    pub(super) through: u64,
    pub(super) references: Vec<String>,
    pub(super) missing_receipt: bool,
    pub(super) receipt_present: bool,
}

pub(super) fn effect_proof_identity(
    state: &HistoryState,
    slot: &str,
) -> Result<String, RuntimeServicesError> {
    let binding = state.bindings.get(slot).ok_or_else(checkpoint_error)?;
    let ticket = state.start_tickets.get(slot).ok_or_else(checkpoint_error)?;
    let spec = state.start_specs.get(slot).ok_or_else(checkpoint_error)?;
    let position = state.start_positions.get(slot).ok_or_else(checkpoint_error)?;
    let encoded = serde_json::to_vec(&(1u32, &state.identity, slot, binding, ticket, spec, position))
        .map_err(|_| checkpoint_error())?;
    Ok(format!("sha256:{:x}", Sha256::digest(encoded)))
}

impl HistoryState {
    pub(super) fn machine_progress(&self) -> Option<harness_contract::projection::HistoryVerificationProgress> {
        let prefix = self.prefix.as_ref()?;
        let encoded = serde_json::to_vec(self.source_fence.as_ref()?).ok()?;
        let mut digest = Sha256::new();
        digest.update(b"cowd-history-source-fence-v1\0");
        digest.update(encoded);
        let progress = prefix.machine_progress(format!("sha256:{:x}", digest.finalize()), self.position, self.through);
        progress.is_valid().then_some(progress)
    }

    pub(super) fn progress(&self) -> serde_json::Value {
        let mut progress = self.prefix.as_ref().map_or_else(
            || serde_json::json!({"status": if self.incomplete { "not_started" } else { "complete" }, "verified_through": self.through, "position": self.position}),
            |prefix| prefix.progress(self.through, self.position, &self.effect_proofs, self.bindings.len()),
        );
        if self.prefix.is_some() {
            let target = progress["target_cursor"].as_u64().unwrap_or(0);
            let verified = self.terminal_proofs.values().filter(|proof| proof.through == target).count();
            progress["terminal_bindings_total"] = serde_json::json!(self.bindings.len());
            progress["terminal_bindings_verified"] = serde_json::json!(verified);
            progress["terminal_pending"] = serde_json::json!(self.terminal_pending.as_ref().map(|memo| {
                serde_json::json!({"slot":memo.slot,"cut":memo.through,"records":memo.records.len()})
            }));
            if progress["phase"] == "effects_pending"
                && progress["effect_bindings_verified"].as_u64() == Some(self.bindings.len() as u64)
                && (verified < self.bindings.len() || self.terminal_pending.is_some())
            {
                progress["phase"] = serde_json::json!("terminal_binding");
            }
        }
        progress
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCheckpoint {
    payloads: Vec<String>,
    state: serde_json::Value,
}

fn checkpoint_error() -> RuntimeServicesError {
    RuntimeServicesError::Invariant("invalid projection checkpoint payload pool".into())
}

#[cfg(test)]
pub(crate) fn own843_checkpoint_state(schema: u64, value: serde_json::Value) -> serde_json::Value {
    serde_json::to_value(decode_checkpoint(schema, value).expect("original checkpoint codec must validate")).unwrap()
}

fn visit_checkpoint_payloads(
    state: &mut serde_json::Value,
    mut visit: impl FnMut(&mut serde_json::Value) -> Result<(), RuntimeServicesError>,
) -> Result<(), RuntimeServicesError> {
    let state = state.as_object_mut().ok_or_else(checkpoint_error)?;
    if let Some(graph) = state
        .get_mut("graph_state")
        .filter(|graph| !graph.is_null())
    {
        for node in graph
            .get_mut("nodes")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or_else(checkpoint_error)?
        {
            visit(node.get_mut("payload_ref").ok_or_else(checkpoint_error)?)?;
        }
    }
    for name in ["start_specs", "start_tickets"] {
        for entry in state
            .get_mut(name)
            .and_then(serde_json::Value::as_object_mut)
            .ok_or_else(checkpoint_error)?
            .values_mut()
        {
            visit(entry.get_mut("payload_ref").ok_or_else(checkpoint_error)?)?;
        }
    }
    Ok(())
}

fn encode_checkpoint(state: &HistoryState) -> Result<serde_json::Value, RuntimeServicesError> {
    let mut state = serde_json::to_value(state).map_err(|_| checkpoint_error())?;
    if serde_json::to_vec(&state)
        .map_err(|_| checkpoint_error())?
        .len()
        > HISTORY_STATE_BYTES
    {
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            "projection semantic state".into(),
        )
        .into());
    }
    let mut payloads = Vec::new();
    let mut indices = BTreeMap::new();
    visit_checkpoint_payloads(&mut state, |slot| {
        let payload = slot.as_str().ok_or_else(checkpoint_error)?;
        let index = if let Some(index) = indices.get(payload) {
            *index
        } else {
            let index = payloads.len();
            indices.insert(payload.to_string(), index);
            payloads.push(payload.to_string());
            index
        };
        *slot = serde_json::json!(index);
        Ok(())
    })?;
    let encoded = serde_json::to_value(HistoryCheckpoint { payloads, state })
        .map_err(|_| checkpoint_error())?;
    if serde_json::to_vec(&encoded)
        .map_err(|_| checkpoint_error())?
        .len()
        > HISTORY_STATE_BYTES
    {
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            "projection checkpoint state".into(),
        )
        .into());
    }
    Ok(encoded)
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PackedHistoryCheckpoint {
    state: HistoryCheckpointIdentity,
    codec: String,
    decoded_bytes: usize,
    sha256: String,
    body: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCheckpointIdentity {
    identity: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_fence: Option<crate::RuntimeSourceMutationFence>,
    through: u64,
    #[serde(default, skip_serializing_if = "is_false")]
    incomplete: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn encode_storage_checkpoint(state: &HistoryState) -> Result<serde_json::Value, RuntimeServicesError> {
    use base64::Engine;
    let decoded = serde_json::to_vec(&encode_checkpoint(state)?).map_err(|_| checkpoint_error())?;
    if decoded.len() > HISTORY_STATE_BYTES {
        return Err(checkpoint_error());
    }
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut encoder, &decoded).map_err(|_| checkpoint_error())?;
    let compressed = encoder.finish().map_err(|_| checkpoint_error())?;
    let packed = serde_json::to_value(PackedHistoryCheckpoint {
        state: HistoryCheckpointIdentity { source_fence: state.source_fence.clone(), identity: state.identity.clone(), through: state.through, incomplete: state.prefix.is_some() },
        codec: "zlib-json-v1".into(),
        decoded_bytes: decoded.len(),
        sha256: format!("sha256:{:x}", Sha256::digest(&decoded)),
        body: base64::engine::general_purpose::STANDARD.encode(compressed),
    }).map_err(|_| checkpoint_error())?;
    if serde_json::to_vec(&packed).map_err(|_| checkpoint_error())?.len() > HISTORY_STATE_BYTES {
        return Err(checkpoint_error());
    }
    Ok(packed)
}

fn decode_storage_checkpoint(value: serde_json::Value) -> Result<HistoryState, RuntimeServicesError> {
    use base64::Engine;
    if serde_json::to_vec(&value).map_err(|_| checkpoint_error())?.len() > HISTORY_STATE_BYTES {
        return Err(checkpoint_error());
    }
    let packed: PackedHistoryCheckpoint = serde_json::from_value(value).map_err(|_| checkpoint_error())?;
    if packed.codec != "zlib-json-v1" || packed.decoded_bytes == 0 || packed.decoded_bytes > HISTORY_STATE_BYTES {
        return Err(checkpoint_error());
    }
    let compressed = base64::engine::general_purpose::STANDARD.decode(&packed.body).map_err(|_| checkpoint_error())?;
    let mut decoder = flate2::read::ZlibDecoder::new(compressed.as_slice());
    let mut decoded = vec![0; packed.decoded_bytes];
    std::io::Read::read_exact(&mut decoder, &mut decoded).map_err(|_| checkpoint_error())?;
    let mut extra = [0u8; 1];
    if std::io::Read::read(&mut decoder, &mut extra).map_err(|_| checkpoint_error())? != 0
        || decoder.total_in() != compressed.len() as u64
        || format!("sha256:{:x}", Sha256::digest(&decoded)) != packed.sha256
    {
        return Err(checkpoint_error());
    }
    let text = std::str::from_utf8(&decoded).map_err(|_| checkpoint_error())?;
    let value = harness_contract::step::parse_strict_json(text).map_err(|_| checkpoint_error())?;
    let restored = decode_checkpoint(4, value)?;
    if restored.source_fence != packed.state.source_fence
        || restored.identity != packed.state.identity || restored.through != packed.state.through
        || restored.prefix.is_some() != packed.state.incomplete {
        return Err(checkpoint_error());
    }
    Ok(restored)
}

fn decode_checkpoint(
    schema: u64,
    value: serde_json::Value,
) -> Result<HistoryState, RuntimeServicesError> {
    if matches!(schema, 5 | 6) {
        return decode_storage_checkpoint(value);
    }
    if schema == 3 {
        if serde_json::to_vec(&value)
            .map_err(|_| checkpoint_error())?
            .len()
            > HISTORY_STATE_BYTES
        {
            return Err(checkpoint_error());
        }
        return serde_json::from_value(value).map_err(|_| checkpoint_error());
    }
    if schema != 4 {
        return Err(checkpoint_error());
    }
    let HistoryCheckpoint {
        payloads,
        mut state,
    } = serde_json::from_value(value).map_err(|_| checkpoint_error())?;
    if payloads.iter().collect::<BTreeSet<_>>().len() != payloads.len() {
        return Err(checkpoint_error());
    }
    let lengths = payloads
        .iter()
        .map(|payload| serde_json::to_vec(payload).map(|bytes| bytes.len()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| checkpoint_error())?;
    let mut expanded_bytes = serde_json::to_vec(&state)
        .map_err(|_| checkpoint_error())?
        .len();
    let mut used = BTreeSet::new();
    visit_checkpoint_payloads(&mut state, |slot| {
        let index = slot
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .filter(|index| *index < payloads.len())
            .ok_or_else(checkpoint_error)?;
        let reference_bytes = serde_json::to_vec(slot)
            .map_err(|_| checkpoint_error())?
            .len();
        expanded_bytes = expanded_bytes
            .checked_sub(reference_bytes)
            .and_then(|bytes| bytes.checked_add(lengths[index]))
            .ok_or_else(checkpoint_error)?;
        used.insert(index);
        Ok(())
    })?;
    if used.len() != payloads.len() {
        return Err(checkpoint_error());
    }
    if expanded_bytes > HISTORY_STATE_BYTES {
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            "projection semantic state".into(),
        )
        .into());
    }
    visit_checkpoint_payloads(&mut state, |slot| {
        let index = slot
            .as_u64()
            .and_then(|index| usize::try_from(index).ok())
            .filter(|index| *index < payloads.len())
            .ok_or_else(checkpoint_error)?;
        *slot = serde_json::Value::String(payloads[index].clone());
        Ok(())
    })?;
    serde_json::from_value(state).map_err(|_| checkpoint_error())
}

pub(super) fn error(message: String) -> RuntimeServicesError {
    if message.starts_with("HistoryBudgetExceeded:") {
        crate::RuntimeEventStoreError::HistoryBudgetExceeded(message).into()
    } else {
        RuntimeServicesError::Invariant(message)
    }
}

pub(super) fn check_bytes<T: Serialize>(value: &T) -> Result<(), RuntimeServicesError> {
    if serde_json::to_vec(value)
        .map_err(|_| RuntimeServicesError::Invariant("projection encoding failed".into()))?
        .len()
        > 1024 * 1024
    {
        return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
            "projection result".into(),
        )
        .into());
    }
    Ok(())
}

impl HistoryState {
    pub(super) fn fold(
        &mut self,
        event: &DurableRuntimeEvent,
        graph: &harness_contract::execution_graph::ExecutionGraphProjection,
        root_scope: &ExecutionScopeProjection,
    ) -> Result<(), String> {
        if event.scope == RuntimeEventScope::ExecutionGraph {
            use crate::execution_core::graph::{ExecutionGraphEvent, NodeExecutionTicket};
            let value: ExecutionGraphEvent = serde_json::from_value(event.payload.clone())
                .map_err(|_| "invalid current Graph history fact".to_string())?;
            if value.kind() != event.kind || event.schema_version != 2 {
                return Err("unsupported current Graph history source".into());
            }
            let next = value.project(self.graph_state.clone())?;
            if next.id != graph.graph_id || event.stream_id != next.id {
                return Err("current Graph history identity mismatch".into());
            }
            if let Some(previous) = self.graph_state.as_ref() {
                if previous.revision.checked_add(1) != Some(next.revision) {
                    return Err("current Graph history revision mismatch".into());
                }
            } else if !matches!(&value, ExecutionGraphEvent::Planned { .. }) || next.revision != 1 {
                return Err("current Graph history has no initial planned fact".into());
            }
            let bindings = match &value {
                ExecutionGraphEvent::NodeTransitioned {
                    node_id,
                    binding: Some(binding),
                    ..
                } => BTreeMap::from([(node_id.clone(), binding.clone())]),
                ExecutionGraphEvent::NodesStarted { bindings, .. }
                | ExecutionGraphEvent::Checkpoint { bindings, .. } => bindings.clone(),
                // A recovery event re-admits nodes and carries their leases under
                // `lease_closures`, not `bindings`. Without this arm a recovered
                // attempt advance looks unbound and the projection rejects a valid
                // graph with the start-binding invariant.
                ExecutionGraphEvent::Recovered { lease_closures, .. } => lease_closures
                    .iter()
                    .filter(|closure| {
                        next.recovery_cursor
                            .node_attempts
                            .get(&closure.node_id)
                            .copied()
                            .is_some_and(|attempt| attempt == closure.binding.attempt)
                    })
                    .map(|closure| (closure.node_id.clone(), closure.binding.clone()))
                    .collect(),
                _ => BTreeMap::new(),
            };
            for (node_id, binding) in &bindings {
                let key = format!(
                    "{node_id}:{}:{}",
                    binding.attempt, binding.ticket_idempotency_key
                );
                if let Some((slot, current)) =
                    self.bindings.iter().find(|(_, (current_node, current))| {
                        current_node == node_id && current.attempt == binding.attempt
                    })
                {
                    if current != &(node_id.clone(), binding.clone()) {
                        return Err("current Graph binding fact changed".into());
                    }
                    let spec = self
                        .start_specs
                        .get(slot)
                        .ok_or("historical start spec missing")?;
                    binding.validate_identity(spec)?;
                    continue;
                }
                if self.bindings.contains_key(&key) {
                    return Err("current Graph binding fact changed".into());
                }
                let previous = self
                    .graph_state
                    .as_ref()
                    .ok_or("initial graph cannot contain a start")?;
                binding.validate_start(previous, node_id)?;
                let spec = next
                    .nodes
                    .iter()
                    .find(|node| node.id == *node_id)
                    .ok_or("historical start node missing")?;
                if previous.nodes.iter().find(|node| node.id == *node_id) != Some(spec)
                    || previous.service_class != next.service_class
                    || next.node_statuses.get(node_id) != Some(&ExecutionNodeStatus::Running)
                    || next.recovery_cursor.node_attempts.get(node_id) != Some(&binding.attempt)
                {
                    return Err("current Graph start does not match event-time state".into());
                }
                let ticket = NodeExecutionTicket {
                    graph_id: next.id.clone(),
                    node_id: node_id.clone(),
                    executor_kind: binding.executor_kind.clone(),
                    service_class: next.service_class,
                    attempt: binding.attempt,
                    idempotency_key: binding.ticket_idempotency_key.clone(),
                    payload_ref: spec.payload_ref.clone(),
                };
                self.start_tickets.insert(key.clone(), ticket);
                self.start_specs.insert(key.clone(), spec.clone());
                self.start_positions
                    .insert(key.clone(), (event.commit_cursor, event.transaction_index));
                self.bindings
                    .insert(key, (node_id.clone(), binding.clone()));
            }
            // `delta.recovery_cursor.node_attempts` is a cumulative slot map, not an
            // increment: a projection resumed mid-stream (or a cold read starting at a
            // non-zero cursor) has no previous graph state, so every already-known
            // attempt would look like "advanced in this event". Adopt the map as the
            // baseline instead of judging it; the started path above still proves every
            // binding it admits, so the write-side fence is unchanged.
            if let Some(previous_state) = self.graph_state.as_ref() {
                for (node_id, attempt) in &next.recovery_cursor.node_attempts {
                    let previous_attempt = previous_state
                        .recovery_cursor
                        .node_attempts
                        .get(node_id)
                        .copied()
                        .unwrap_or(0);
                    if *attempt > previous_attempt
                        && !bindings
                            .get(node_id)
                            .is_some_and(|binding| binding.attempt == *attempt)
                    {
                        return Err(
                            "current Graph attempt advanced without its start binding".into(),
                        );
                    }
                }
            }
            self.graph_state = Some(next);
        }
        if let Some(binding) = event.activity_binding() {
            let identity = &binding.activity_id;
            if !self.detail_inputs.contains_key(identity) {
                if let Some(input) = super::activity_content_projection(
                    std::slice::from_ref(event),
                    &[
                        "input",
                        "input_preview",
                        "request",
                        "objective",
                        "payload_ref",
                        "constraints",
                        "depends_on",
                        "input_refs",
                    ],
                    false,
                    None,
                ) {
                    self.detail_inputs.insert(identity.clone(), input);
                }
            }
            if let Some(output) = super::activity_content_projection(
                std::slice::from_ref(event),
                &[
                    "output",
                    "output_preview",
                    "result",
                    "result_summary",
                    "summary",
                    "outcome",
                    "returned",
                    "error",
                    "failure",
                    "full_output_ref",
                    "output_ref",
                ],
                true,
                None,
            ) {
                self.detail_outputs.insert(identity.clone(), output);
            }
            let entity = snapshot::entity_from_runtime_event("evidence", event.clone(), true);
            self.detail_entities
                .entry(identity.clone())
                .or_default()
                .insert(entity.id.clone(), entity);
        }
        self.first = Some(
            self.first
                .map_or(event.created_at_ms, |first| first.min(event.created_at_ms)),
        );
        self.last = Some(
            self.last
                .map_or(event.created_at_ms, |last| last.max(event.created_at_ms)),
        );
        for node in &graph.nodes {
            if !activity::event_refers_to_node(event, &node.node_id) {
                continue;
            }
            let node = self.nodes.entry(node.node_id.clone()).or_default();
            node.first = Some(
                node.first
                    .map_or(event.created_at_ms, |first| first.min(event.created_at_ms)),
            );
            node.last = Some(
                node.last
                    .map_or(event.created_at_ms, |last| last.max(event.created_at_ms)),
            );
            node.cursor = node.cursor.max(event.commit_cursor);
            if node.label.is_none() {
                node.label = activity::agent_binding_display_label(&[event]);
            }
        }
        if let Some(candidate) = activity::event_activity(event, root_scope) {
            if let Some(current) = self.activities.get_mut(&candidate.activity_id) {
                activity::merge_activity(current, candidate);
            } else {
                self.activities
                    .insert(candidate.activity_id.clone(), candidate);
            }
        }
        if event.kind.starts_with("tool.invocation.") {
            if let Some(binding) = event.activity_binding() {
                if let Some(references) = event.payload["input_refs"].as_array() {
                    for reference in references
                        .iter()
                        .filter_map(serde_json::Value::as_str)
                        .filter(|reference| !reference.trim().is_empty())
                    {
                        self.inputs
                            .insert((binding.activity_id.clone(), reference.into()));
                    }
                }
            }
        }
        for entity in
            reducer_support::activity_binding_health_entities(std::slice::from_ref(event), true)
        {
            self.health.insert(entity.id.clone(), entity);
        }
        let categories = [
            (
                "usage",
                event.scope == RuntimeEventScope::Tool
                    || event.scope == RuntimeEventScope::ExecutionNode
                    || event.kind.contains("usage"),
            ),
            (
                "context",
                event.kind.contains("context") || event.kind.contains("memory"),
            ),
            (
                "evidence",
                event.kind != crate::execution_core::OUTCOME_EVENT_KIND
                    && !event.kind.starts_with("resource.admission.")
                    && !snapshot::is_strategy_event(&event.kind)
                    && (!event.refs.is_empty() || event.kind.contains("evidence")),
            ),
            ("admission", event.kind.starts_with("resource.admission.")),
            (
                "outcome",
                event.kind == crate::execution_core::OUTCOME_EVENT_KIND,
            ),
            (
                "recovery",
                event.scope == RuntimeEventScope::Recovery || event.kind.contains("recovery"),
            ),
        ];
        for (category, include) in categories {
            if !include {
                continue;
            }
            for (full, target) in [
                (true, &mut self.entities_full),
                (false, &mut self.entities_summary),
            ] {
                let entity = snapshot::entity_from_runtime_event(category, event.clone(), full);
                let entities = target.entry(category.into()).or_default();
                if entities
                    .get(&entity.id)
                    .is_none_or(|current| current.revision <= entity.revision)
                {
                    entities.insert(entity.id.clone(), entity);
                }
            }
        }
        if snapshot::is_strategy_event(&event.kind) {
            if snapshot::strategy_scope(event).is_some()
                && event.payload["execution_graph_ref"].as_str() == Some(graph.graph_id.as_str())
                && event.payload["session_ref"].as_str() == root_scope.session_id.as_deref()
                && event.payload["turn_ref"].as_str() == root_scope.turn_id.as_deref()
            {
                if event.kind == "runtime.strategy.selected" {
                    self.strategy.clear();
                }
                if self
                    .strategy
                    .first()
                    .and_then(snapshot::strategy_scope)
                    .is_none_or(|key| snapshot::strategy_scope(event).as_ref() == Some(&key))
                {
                    self.strategy.push(event.clone());
                }
            }
        }
        Ok(())
    }
}

pub(super) fn load(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    through: u64,
) -> Result<HistoryState, RuntimeServicesError> {
    let mut budget = RequestBudget::default();
    let state = load_budgeted(services, scope, graph, through, false, &mut budget)?;
    budget.publish(services)?;
    Ok(state)
}

fn load_budgeted(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    graph: &harness_contract::execution_graph::ExecutionGraphProjection,
    through: u64,
    historical: bool,
    budget: &mut RequestBudget,
) -> Result<HistoryState, RuntimeServicesError> {
    if !historical {
        if !budget.incremental {
            budget.enable_incremental();
        }
        return prefix::load(services, scope, graph, through, budget);
    }
    let source_fence = budget.capture_source_fence(services)?;
    budget.require_source_fence(services, &source_fence)?;
    budget.charge(1, 0)?;
    let root_scope = activity::execution_scope(services, scope, graph);
    let identity = format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&root_scope).map_err(|_| {
            RuntimeServicesError::Invariant("projection scope encoding failed".into())
        })?)
    );
    let projection_id = format!("projection-history:{}", graph.graph_id);
    let checkpoint = services
        .event_store()
        .projection_checkpoint(&projection_id)?;
    let checkpoint_source = if checkpoint.as_ref().is_some_and(|saved| saved.payload["schema"] == 6) {
        Some(source_fence.clone())
    } else {
        None
    };
    let content_scope = format!(
        "session:{}",
        scope
            .session_id
            .as_deref()
            .ok_or(RuntimeServicesError::ProjectionAccessDenied)?
    );
    let mut state = HistoryState {
        source_fence: Some(source_fence.clone()),
        identity: identity.clone(),
        ..HistoryState::default()
    };
    let mut current_checkpoint = false;
    if let Some(saved) = checkpoint.as_ref().filter(|saved| {
        saved.source_cursor <= through
            && matches!(saved.payload["schema"].as_u64(), Some(3 | 4 | 5 | 6))
            && saved.payload["body"]["bytes"]
                .as_u64()
                .is_some_and(|bytes| bytes <= HISTORY_STATE_BYTES as u64)
    }) {
        let metadata = budget.charge_value(saved)?;
        let body = budget.charge_envelope(&saved.payload["body"])?;
        let restored = services
            .event_store()
            .resolve_content(
                &content_scope,
                &saved.payload["body"],
                &crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: projection_id.clone(),
                    source_cursor: saved.source_cursor,
                },
            )
            .map_err(error);
        let restored = match restored {
            Ok(value) => {
                let actual = serde_json::to_vec(&value)
                    .map_err(|_| {
                        RuntimeServicesError::Invariant(
                            "projection checkpoint encoding failed".into(),
                        )
                    })?
                    .len();
                budget.charge(0, actual.saturating_sub(metadata + body))?;
                decode_checkpoint(saved.payload["schema"].as_u64().unwrap_or(0), value)
            }
            Err(
                error @ RuntimeServicesError::EventStore(
                    crate::RuntimeEventStoreError::HistoryBudgetExceeded(_),
                ),
            ) => return Err(error),
            Err(error) => Err(error),
        };
        let restored = match restored {
            Err(error) if saved.payload["schema"] == 6 => return Err(error),
            other => other,
        };
        if let Ok(restored) = restored {
            if saved.payload["schema"] == 6 {
                let fence = restored.source_fence.as_ref().ok_or_else(|| RuntimeServicesError::Invariant(
                    "complete history source fence missing".into()))?;
                if checkpoint_source.as_ref() != Some(fence)
                    || saved.payload["source_fence"] != serde_json::to_value(fence).map_err(|_| checkpoint_error())? {
                    return Err(RuntimeServicesError::Invariant("history source fence identity mismatch".into()));
                }
                budget.require_source_fence(services, fence)?;
            }
            if restored.prefix.is_some() {
                return Err(RuntimeServicesError::Invariant(INCOMPLETE.into()));
            }
            if saved.payload["schema"] == 6
                && (saved.payload["target_cursor"] != saved.source_cursor
                    || saved.payload["verified_through"] != restored.through
                    || saved.payload["incomplete"] != false)
            {
                return Err(RuntimeServicesError::Invariant("history checkpoint coverage mismatch".into()));
            }
            if saved.payload["schema"] == 6
                && restored.source_fence.as_ref() == Some(&source_fence)
                && restored.identity == identity
                && restored.through == saved.source_cursor
                && restored.graph_state.as_ref().is_some_and(|state| {
                    state.id == graph.graph_id && state.revision <= graph.revision
                })
                && restored.start_tickets.len() == restored.bindings.len()
                && restored.start_specs.len() == restored.bindings.len()
                && restored.start_positions.len() == restored.bindings.len()
                && restored
                    .position
                    .is_none_or(|position| position.0 <= restored.through)
            {
                current_checkpoint = restored.through == through;
                state = restored;
            } else if saved.payload["schema"] == 6 {
                return Err(RuntimeServicesError::Invariant("history checkpoint identity mismatch".into()));
            }
        }
    }
    if current_checkpoint {
        if let Some(fence) = &state.source_fence {
            budget.require_source_fence(services, fence)?;
        }
        return Ok(state);
    }
    let mut after = state.position;
    loop {
        budget.charge(1, 0)?;
        let page = services
            .event_store()
            .position_page(
                RuntimeEventScope::ExecutionGraph,
                &graph.graph_id,
                None,
                after,
                through,
                budget.records.min(100).max(1),
                budget.bytes.min(1024 * 1024).max(1),
            )
            .map_err(error)?;
        if page.is_empty() {
            break;
        }
        for record in page {
            let position = (record.commit_cursor, record.transaction_index);
            if after.is_some_and(|after| position <= after) || record.commit_cursor > through {
                return Err(RuntimeServicesError::Invariant(
                    "projection position is not monotonic".into(),
                ));
            }
            if !activity::event_belongs_to_graph(&record, graph) || !scope.contains_event(&record) {
                budget.charge_record(&record)?;
                if record.scope == RuntimeEventScope::ExecutionGraph {
                    return Err(RuntimeServicesError::Invariant(
                        "authoritative Graph history scope mismatch".into(),
                    ));
                }
                after = Some(position);
                continue;
            }
            let event = budget.resolve_record(services, record.clone())?;
            budget.remember_source_record(&record, through)?;
            budget.remember_record(&event, through)?;
            if event.kind == "session.cancellation_committed" {
                let mut receipt: harness_contract::turn::CancellationReceipt =
                    serde_json::from_value(event.payload.clone()).map_err(|_| {
                        RuntimeServicesError::Invariant("invalid cancellation fact".into())
                    })?;
                if receipt.execution_id == graph.graph_id
                    && Some(receipt.session_id.as_str()) == scope.session_id.as_deref()
                    && Some(receipt.turn_id.as_str()) == scope.turn_id.as_deref()
                {
                    receipt.journal_sequence = event.commit_cursor;
                    receipt.projection_revision = event.sequence;
                    state.cancellation = Some(receipt);
                }
            }
            state.fold(&event, graph, &root_scope).map_err(error)?;
            if serde_json::to_vec(&state)
                .map_err(|_| {
                    RuntimeServicesError::Invariant(
                        "projection history state encoding failed".into(),
                    )
                })?
                .len()
                > HISTORY_STATE_BYTES
            {
                return Err(crate::RuntimeEventStoreError::HistoryBudgetExceeded(
                    "projection semantic state".into(),
                )
                .into());
            }
            after = Some(position);
        }
    }
    state.position = after;
    state.through = through;
    Ok(state)
}

pub(super) fn load_scope(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    through: u64,
) -> Result<BTreeMap<String, HistoryState>, RuntimeServicesError> {
    let mut budget = RequestBudget::default();
    let states = load_scope_budgeted(services, scope, through, false, &mut budget)?;
    budget.publish(services)?;
    Ok(states)
}

pub(super) fn load_scope_budgeted(
    services: &RuntimeServices,
    scope: &ExecutionProjectionScope,
    through: u64,
    historical: bool,
    budget: &mut RequestBudget,
) -> Result<BTreeMap<String, HistoryState>, RuntimeServicesError> {
    let mut states = BTreeMap::new();
    for execution in &scope.execution_ids {
        if budget.is_incomplete() {
            states.insert(execution.clone(), HistoryState { incomplete: true, ..Default::default() });
            continue;
        }
        // The prefix stage refuses to publish a prefix prepared from a graph
        // revision that has since moved. That refusal is correct, but it is a
        // race, not a verdict: a reader that gives up here reports a projection
        // that does not exist (the Gateway maps it to 404) for an execution
        // that is merely being written to right now. Re-read the graph and
        // prepare the prefix for the CURRENT head instead.
        //
        // The bound is what keeps this honest: if the head keeps moving past
        // the bound, the projection still fails with the original error.
        let mut attempts = 0_u8;
        let state = loop {
            let graph = services.graph_state_store().projection(execution)?;
            match load_budgeted(services, scope, &graph, through, historical, budget) {
                Ok(state) => break state,
                Err(error) if is_stale_graph_source(&error) && attempts < GRAPH_SOURCE_REREAD_LIMIT => {
                    attempts = attempts.saturating_add(1);
                }
                Err(error) => return Err(error),
            }
        };
        states.insert(execution.clone(), state);
        budget.scope_progress = true;
    }
    budget.require_captured_source(services)?;
    Ok(states)
}

/// How many times a projection may re-read its graph when the head moved while
/// the prefix was being prepared.
const GRAPH_SOURCE_REREAD_LIMIT: u8 = 3;

/// The prefix stage's refusal to publish a prefix prepared from a graph
/// revision that has since moved. It is matched by message, not by status,
/// because the store surfaces it as a plain invariant string. Every other
/// error - including every other invariant - propagates unchanged.
fn is_stale_graph_source(error: &RuntimeServicesError) -> bool {
    matches!(error, RuntimeServicesError::Invariant(message)
        if message.contains("history prefix: graph source changed"))
}

pub(super) fn is_graph_ahead_of_cut(error: &RuntimeServicesError) -> bool {
    matches!(error, RuntimeServicesError::Invariant(message)
        if message == "history prefix: graph ahead of requested cut")
}

#[cfg(test)]
mod canonical_request_413_tests {
    use super::*;

    #[test]
    fn cut_race_retry_keeps_work_budget_and_discards_attempt_state() {
        let mut budget = RequestBudget::default();
        budget.enable_incremental();
        budget.charge(4, 128).unwrap();
        budget.stream_heads.insert(("old".into(), 1), 1);
        budget.incomplete = true;
        budget.scope_progress = true;
        budget.restart_after_cut_race().unwrap();
        assert_eq!(budget.consumed_work(), (5, 128));
        assert!(budget.stream_heads.is_empty());
        assert!(!budget.incomplete && !budget.scope_progress);
        assert!(budget.incremental);
        assert!(is_graph_ahead_of_cut(&RuntimeServicesError::Invariant(
            "history prefix: graph ahead of requested cut".into())));
        assert!(!is_graph_ahead_of_cut(&RuntimeServicesError::Invariant(
            "history source mutation fence changed".into())));
    }

    #[tokio::test]
    async fn r1_703_verified_graph_body_reuse_preserves_raw_identity_cut_and_poisoning() {
        let services = RuntimeServices::in_memory().unwrap();
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("body reuse ".repeat(1000));
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let graph = services.commit_service().register_graph(graph).unwrap().graph;
        let raw = services.event_store().latest_for_stream(&graph.id).unwrap().unwrap();
        let through = services.event_store().durable_commit_cursor().unwrap();
        let mut budget = RequestBudget::default();
        let first = budget.resolve_record(&services, raw.clone()).unwrap();
        budget.remember_source_record(&raw, through).unwrap();
        let remaining = (budget.records, budget.bytes);
        let original = budget.canonical_record(&services, &raw.stream_id, raw.idempotency_key.as_deref().unwrap(), through).unwrap().unwrap();
        let second = budget.resolve_prepaid_record(&services, &original, 0).unwrap();
        assert_eq!(first, second);
        assert_eq!((budget.records, budget.bytes), remaining);
        assert_eq!(budget.resolved_records.len(), 1);
        let mut changed = raw.clone();
        changed.payload["bytes"] = serde_json::json!(0);
        assert!(budget.resolve_prepaid_record(&services, &changed, 0).is_err());
        assert!(budget.canonical_record(&services, &raw.stream_id, raw.idempotency_key.as_deref().unwrap(), raw.commit_cursor - 1).unwrap().is_none());
        assert!(budget.charge(0, HISTORY_REQUEST_BYTES + 1).is_err());
        assert!(budget.resolved_records.is_empty());
        assert!(budget.records_by_sequence.is_empty());
        assert!(budget.resolve_prepaid_record(&services, &raw, 0).is_err());
        let mut recovered = RequestBudget::default();
        assert_eq!(recovered.resolve_record(&services, raw).unwrap(), first);
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn own843_exact_resolved_hit_charges_metadata_not_unread_body_and_keeps_poison() {
        let services = RuntimeServices::in_memory().unwrap();
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new(
            "843 exact original body ".repeat(16 * 1024),
        );
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let graph = services.commit_service().register_graph(graph).unwrap().graph;
        let raw = services.event_store().latest_for_stream(&graph.id).unwrap().unwrap();
        assert_eq!(raw.payload["body"]["kind"], "artifact_ref");
        let mut budget = RequestBudget::default();
        let first = budget.resolve_record(&services, raw.clone()).unwrap();
        let before = budget.consumed_work();
        let metadata = serde_json::to_vec(&raw).unwrap().len();
        let second = budget.resolve_record(&services, raw.clone()).unwrap();
        assert_eq!(second, first);
        assert_eq!(budget.consumed_work(), before);
        assert!(metadata > 0);
        for field in ["payload", "stream", "key", "cursor"] {
            let mut foreign = raw.clone();
            match field {
                "payload" => foreign.payload["bytes"] = serde_json::json!(0),
                "stream" => foreign.stream_id.push_str("-foreign"),
                "key" => foreign.idempotency_key = Some("foreign".into()),
                _ => foreign.commit_cursor += 1,
            }
            assert!(budget.resolve_record(&services, foreign).is_err(), "{field}");
        }
        assert!(budget.charge(0, HISTORY_REQUEST_BYTES + 1).is_err());
        assert!(budget.resolve_record(&services, raw.clone()).is_err());
        assert!(budget.resolved_records.is_empty());
        assert!(budget.publish(&services).is_err());
        let mut independent = RequestBudget::default();
        assert_eq!(independent.resolve_record(&services, raw).unwrap(), first);
        assert!(independent.consumed_work().1 > metadata);
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn head_431_actual_metadata_exhaustion_rejects_second_read_and_publication() {
        let services = RuntimeServices::in_memory().unwrap();
        let stream = "head-budget-431";
        let mut budget = RequestBudget::default();
        budget.bytes = stream.len() * 2 + 1;
        assert_eq!(budget.uncached_stream_head(&services, stream).unwrap(), 0);
        assert!(matches!(
            budget.uncached_stream_head(&services, stream),
            Err(RuntimeServicesError::EventStore(
                crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)
            ))
        ));
        assert!(budget.publish(&services).is_err());
        assert!(budget.checkpoints.is_empty() && budget.stream_heads.is_empty());
        let mut budget = RequestBudget::default();
        budget.bytes = stream.len();
        assert!(budget.stream_head(&services, stream, 0).is_err());
        assert!(budget.stream_heads.is_empty());
        assert!(budget.stream_head(&services, stream, 0).is_err());
    }

    #[tokio::test]
    async fn canonical_430_stream_pages_reuse_only_complete_prefixes_and_charge_real_gaps() {
        let services = RuntimeServices::in_memory().unwrap();
        let stream = "execution-agent-receipts:stream-page-430";
        let entries = (1..=3)
            .map(|sequence| crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: stream.into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "request.memo.test".into(),
                    status: None,
                    actor: None,
                    refs: Vec::new(),
                    payload: serde_json::json!({"original":sequence}),
                },
                idempotency_key: Some(format!("original-{sequence}")),
                schema_version: 1,
            })
            .collect();
        services
            .event_store()
            .append_batch_if_revision(stream, 0, "stream-page-430", entries)
            .unwrap();
        let through = services.event_store().durable_commit_cursor().unwrap();
        let mut budget = RequestBudget::default();
        budget
            .canonical_record(&services, stream, "original-2", through)
            .unwrap()
            .unwrap();
        let first = budget
            .canonical_stream_page(&services, stream, 0, 3, through)
            .unwrap();
        assert_eq!(
            first
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1]
        );
        let charged = (budget.records, budget.bytes);
        let prefix = budget
            .canonical_stream_page(&services, stream, 0, 3, through)
            .unwrap();
        assert_eq!(
            prefix
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!((budget.records, budget.bytes), charged);
        let tail = budget
            .canonical_stream_page(&services, stream, 2, 3, through)
            .unwrap();
        assert_eq!(tail[0].sequence, 3);
        assert!(budget.records < charged.0 && budget.bytes < charged.1);
        let charged = (budget.records, budget.bytes);
        assert_eq!(
            budget
                .canonical_stream_page(&services, stream, 0, 3, through)
                .unwrap()
                .len(),
            3
        );
        assert_eq!((budget.records, budget.bytes), charged);
        let beyond = budget.records.saturating_add(1);
        assert!(budget.charge(beyond, 0).is_err());
        assert!(budget
            .canonical_stream_page(&services, stream, 0, 3, through)
            .is_err());
    }

    #[tokio::test]
    async fn canonical_memo_preserves_absence_cut_and_poisoning_without_checkpoint_writes() {
        let services = RuntimeServices::in_memory().unwrap();
        let stream = "execution-effect:canonical-memo-413";
        let old_cut = services.event_store().durable_commit_cursor().unwrap();
        let mut budget = RequestBudget::default();
        assert!(budget
            .canonical_record(&services, stream, "key-413", old_cut)
            .unwrap()
            .is_none());
        let allowance = (budget.records, budget.bytes);
        services
            .event_store()
            .append_batch_if_revision(
                stream,
                0,
                "canonical-memo-413-tx",
                vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream.into(),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "cache.transport.test".into(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::json!({"value":"original"}),
                    },
                    idempotency_key: Some("key-413".into()),
                    schema_version: 1,
                }],
            )
            .unwrap();
        assert!(budget
            .canonical_record(&services, stream, "key-413", old_cut)
            .unwrap()
            .is_none());
        assert_eq!((budget.records, budget.bytes), allowance);
        let cut = services.event_store().durable_commit_cursor().unwrap();
        let first = budget
            .canonical_record(&services, stream, "key-413", cut)
            .unwrap()
            .unwrap();
        let allowance = (budget.records, budget.bytes);
        let second = budget
            .canonical_record(&services, stream, "key-413", cut)
            .unwrap()
            .unwrap();
        assert!(std::sync::Arc::ptr_eq(&first, &second));
        assert_eq!((budget.records, budget.bytes), allowance);
        assert!(budget.charge(0, HISTORY_REQUEST_BYTES + 1).is_err());
        assert!(budget
            .canonical_record(&services, stream, "key-413", cut)
            .is_err());
        assert!(budget.publish(&services).is_err());
        services.shutdown_execution().await;
    }

    #[tokio::test]
    async fn checkpoint_codec_keeps_uncached_head_fence_and_cut_scoped_negative_memo() {
        let services = RuntimeServices::in_memory().unwrap();
        let stream = "execution-effect:checkpoint-551-fence";
        let key = "checkpoint-551-fence-key";
        let old_cut = services.event_store().durable_commit_cursor().unwrap();
        let mut budget = RequestBudget::default();
        assert_eq!(budget.stream_head(&services, stream, old_cut).unwrap(), 0);
        assert!(budget
            .canonical_record(&services, stream, key, old_cut)
            .unwrap()
            .is_none());
        services
            .event_store()
            .append_batch_if_revision(
                stream,
                0,
                "checkpoint-551-fence-tx",
                vec![crate::RuntimeTransactionEventInput {
                    event: crate::RuntimeEventInput {
                        stream_id: stream.into(),
                        scope: RuntimeEventScope::ExecutionNode,
                        kind: "cache.transport.test".into(),
                        status: None,
                        actor: None,
                        refs: Vec::new(),
                        payload: serde_json::json!({"value":"original"}),
                    },
                    idempotency_key: Some(key.into()),
                    schema_version: 1,
                }],
            )
            .unwrap();
        assert_eq!(budget.stream_head(&services, stream, old_cut).unwrap(), 0);
        assert_eq!(budget.uncached_stream_head(&services, stream).unwrap(), 1);
        assert!(budget
            .canonical_record(&services, stream, key, old_cut)
            .unwrap()
            .is_none());
        let current_cut = services.event_store().durable_commit_cursor().unwrap();
        assert!(current_cut > old_cut);
        let record = budget
            .canonical_record(&services, stream, key, current_cut)
            .unwrap()
            .unwrap();
        assert_eq!(record.payload["value"], "original");
        let mut drifted = record.as_ref().clone();
        drifted.payload["value"] = serde_json::json!("different");
        assert!(budget.remember_record(&drifted, current_cut).is_err());
        services.shutdown_execution().await;
    }
}

#[cfg(test)]
mod checkpoint_pool_551_tests {
    use super::*;
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec};

    fn repeated_payload_state() -> HistoryState {
        let mut graph = ExecutionGraph::new("checkpoint-551");
        graph.id = "checkpoint-551-graph".into();
        graph.objective = "non-payload-state".repeat(12_000);
        let mut state = HistoryState::default();
        for index in 0..18 {
            let payload = format!(
                " {{\"bound\":\"{}\",\"identity\":{}}} \n",
                "文\\\"\n".repeat(850),
                index % 9
            );
            let mut spec = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                payload.clone(),
            );
            spec.id = format!("checkpoint-551-node-{index}");
            spec.idempotency_key = format!("checkpoint-551-key-{index}");
            if index < 17 {
                state.start_specs.insert(spec.id.clone(), spec.clone());
                state.start_tickets.insert(
                    spec.id.clone(),
                    crate::execution_core::graph::NodeExecutionTicket {
                        graph_id: graph.id.clone(),
                        node_id: spec.id.clone(),
                        executor_kind: spec.executor_kind.clone(),
                        service_class: graph.service_class,
                        attempt: 1,
                        idempotency_key: spec.idempotency_key.clone(),
                        payload_ref: payload,
                    },
                );
            }
            graph.nodes.push(spec);
        }
        state.identity = "checkpoint-551-identity".into();
        state.through = 203;
        state.graph_state = Some(graph);
        state
    }

    #[test]
    fn r1_703_storage_codec_preserves_full_semantics_and_actual_wire_budget() {
        let state = repeated_payload_state();
        let legacy = encode_checkpoint(&state).unwrap();
        let packed = encode_storage_checkpoint(&state).unwrap();
        let restored = decode_checkpoint(5, packed.clone()).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), serde_json::to_value(&state).unwrap());
        let mut legacy_budget = RequestBudget::default();
        let mut packed_budget = RequestBudget::default();
        let legacy_bytes = legacy_budget.charge_value(&legacy).unwrap();
        let packed_bytes = packed_budget.charge_value(&packed).unwrap();
        assert!(packed_bytes < legacy_bytes);
        assert_eq!(packed_budget.bytes, HISTORY_REQUEST_BYTES - packed_bytes);
        let common_tail = HISTORY_REQUEST_BYTES - legacy_bytes + 1;
        assert!(legacy_budget.charge(0, common_tail).is_err());
        packed_budget.charge(0, common_tail).unwrap();
        assert!(legacy_budget.charge(0, 0).is_err());
        assert!(packed_budget.charge(0, HISTORY_REQUEST_BYTES).is_err());
        assert_eq!(HISTORY_REQUEST_BYTES, 1024 * 1024);
        assert_eq!(HISTORY_REQUEST_RECORDS, 256);
        assert_eq!(HISTORY_STATE_BYTES, 4 * 1024 * 1024);
    }

    #[test]
    fn r1_703_storage_codec_rejects_corruption_bombs_trailing_and_source_spoofing() {
        use base64::Engine;
        let original = encode_storage_checkpoint(&repeated_payload_state()).unwrap();
        for field in ["codec", "decoded_bytes", "sha256", "body", "identity", "through", "unknown"] {
            let mut bad = original.clone();
            match field {
                "codec" => bad[field] = serde_json::json!("foreign"),
                "decoded_bytes" => bad[field] = serde_json::json!(HISTORY_STATE_BYTES + 1),
                "sha256" => bad[field] = serde_json::json!("sha256:wrong"),
                "body" => {
                    let mut compressed = base64::engine::general_purpose::STANDARD.decode(bad[field].as_str().unwrap()).unwrap();
                    compressed.push(0);
                    bad[field] = serde_json::json!(base64::engine::general_purpose::STANDARD.encode(compressed));
                }
                "identity" => bad["state"][field] = serde_json::json!("foreign"),
                "through" => bad["state"][field] = serde_json::json!(999),
                _ => bad[field] = serde_json::json!(true),
            }
            assert!(decode_checkpoint(5, bad).is_err(), "fault {field}");
        }
        let mut short = original;
        short["decoded_bytes"] = serde_json::json!(1);
        assert!(decode_checkpoint(5, short).is_err());
    }

    #[tokio::test]
    async fn r1_703_storage_checkpoint_cold_restore_keeps_origin_cut_and_global_budget() {
        use crate::RuntimeEventStoreBackend;

        for external in [false, true] {
            let temporary = tempfile::tempdir().unwrap();
            let workspace = temporary.path().join("workspace");
            std::fs::create_dir_all(&workspace).unwrap();
            let backend = std::sync::Arc::new(
                crate::runtime_event_store::EphemeralRuntimeEventStore::new(),
            );
            let services = RuntimeServices::test_builder(temporary.path().join("home"), &workspace)
                .runtime_event_store(std::sync::Arc::new(crate::RuntimeEventStore::from_backend(
                    backend.clone(),
                )))
                .build()
                .unwrap();
            let mut state = repeated_payload_state();
            if external {
                state.graph_state.as_mut().unwrap().objective = (0..4096)
                    .map(|index| format!("{:x}", Sha256::digest(format!("checkpoint-723-{index}").as_bytes())))
                    .collect::<Vec<_>>()
                    .join("");
            }
            let encoded = encode_storage_checkpoint(&state).unwrap();
            let encoded_bytes = serde_json::to_vec(&encoded).unwrap().len();
            assert_eq!(encoded_bytes > 16 * 1024, external);
            let mut writer = RequestBudget::default();
            let projection_id = "projection-history:checkpoint-703";
            let content_scope = "session:checkpoint-703";
            let origin = crate::PreparedArtifactOrigin::CheckpointBody {
                projection_id: projection_id.into(),
                source_cursor: state.through,
            };
            writer.checkpoints.insert(projection_id.into(), PendingCheckpoint {
                source_fence: None,
                schema: 5,
                projection_id: projection_id.into(),
                content_scope: content_scope.into(),
                identity: state.identity.clone(),
                through: state.through,
                expected_revision: 0,
                encoded,
            });
            writer.publish(&services).unwrap();
            let saved = services.event_store().projection_checkpoint(projection_id).unwrap().unwrap();
            assert_eq!(saved.payload["schema"], 5);
            assert_eq!(saved.payload["body"]["bytes"], encoded_bytes);
            assert_eq!(saved.payload["body"]["body"]["kind"],
                if external { "artifact_ref" } else { "inline" });
            services.event_store().validate_checkpoint_content(
                projection_id, state.through, &saved.payload,
            ).unwrap();
            assert!(services.event_store().validate_checkpoint_content(
                projection_id, state.through + 1, &saved.payload,
            ).is_err());
            for _ in 0..2 {
                let mut reader = RequestBudget::default();
                reader.charge_value(&saved).unwrap();
                reader.charge_envelope(&saved.payload["body"]).unwrap();
                let value = services.event_store().resolve_content(
                    content_scope, &saved.payload["body"], &origin,
                ).unwrap();
                assert_eq!(serde_json::to_value(decode_checkpoint(5, value).unwrap()).unwrap(),
                    serde_json::to_value(&state).unwrap());
                assert!(reader.charge(0, HISTORY_REQUEST_BYTES).is_err());
                assert!(reader.publish(&services).is_err());
            }
            let mut corrupt = saved.payload["body"].clone();
            corrupt["sha256"] = serde_json::json!(format!("sha256:{}", "0".repeat(64)));
            assert!(services.event_store().resolve_content(content_scope, &corrupt, &origin).is_err());
            if external {
                let artifact: harness_contract::context::ArtifactRef =
                    serde_json::from_value(saved.payload["body"]["body"]["artifact"].clone()).unwrap();
                assert_eq!(backend.prepared_content_root(&origin, &artifact).unwrap(),
                    crate::PreparedContentRoot::Checkpoint);
                let foreign_source = crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: projection_id.into(),
                    source_cursor: state.through + 1,
                };
                let foreign_projection = crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: "projection-history:foreign".into(),
                    source_cursor: state.through,
                };
                for (scope, expected_origin) in [
                    ("session:foreign", &origin),
                    (content_scope, &foreign_source),
                    (content_scope, &foreign_projection),
                ] {
                    assert!(services.event_store().resolve_content(
                        scope, &saved.payload["body"], expected_origin,
                    ).is_err());
                }
                assert_eq!(backend.prepared_content_root(&foreign_source, &artifact).unwrap(),
                    crate::PreparedContentRoot::Blocked);
                let mut foreign_scope = artifact.clone();
                foreign_scope.visibility_scope = "session:foreign".into();
                assert_eq!(backend.prepared_content_root(&origin, &foreign_scope).unwrap(),
                    crate::PreparedContentRoot::Blocked);
                let mut unknown_schema = saved.payload.clone();
                unknown_schema["schema"] = serde_json::json!(6);
                assert!(services.event_store().validate_checkpoint_content(
                    projection_id, state.through, &unknown_schema,
                ).is_err());
                backend.compare_and_put_projection_checkpoint(
                    projection_id, state.through, saved.revision, &unknown_schema, 0,
                ).unwrap();
                assert_eq!(backend.prepared_content_root(&origin, &artifact).unwrap(),
                    crate::PreparedContentRoot::Blocked);
                services.event_store().delete_projection_checkpoint(projection_id).unwrap();
                assert_eq!(backend.prepared_content_root(&origin, &artifact).unwrap(),
                    crate::PreparedContentRoot::Absent);
            }
            services.shutdown_execution().await;
        }
    }

    #[test]
    fn checkpoint_payload_pool_preserves_exact_tickets_specs_and_opaque_bytes() {
        let state = repeated_payload_state();
        let original = serde_json::to_value(&state).unwrap();
        let encoded = encode_checkpoint(&state).unwrap();
        assert_eq!(encoded["payloads"].as_array().unwrap().len(), 9);
        assert!(
            serde_json::to_vec(&encoded).unwrap().len() + 200_000
                < serde_json::to_vec(&original).unwrap().len()
        );
        let restored = decode_checkpoint(4, encoded).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), original);
        let legacy = decode_checkpoint(3, original.clone()).unwrap();
        assert_eq!(serde_json::to_value(legacy).unwrap(), original);
    }

    #[test]
    fn checkpoint_payload_pool_rejects_bad_references_and_unknown_wire_fields() {
        let original = encode_checkpoint(&repeated_payload_state()).unwrap();
        for invalid in [
            serde_json::json!(-1),
            serde_json::json!(0.5),
            serde_json::json!(999),
            serde_json::json!("0"),
            serde_json::Value::Null,
        ] {
            let mut encoded = original.clone();
            encoded["state"]["graph_state"]["nodes"][0]["payload_ref"] = invalid;
            assert!(decode_checkpoint(4, encoded).is_err());
        }
        let mut extra = original.clone();
        extra["unknown"] = serde_json::json!(true);
        assert!(decode_checkpoint(4, extra).is_err());
        let mut duplicate = original.clone();
        let first = duplicate["payloads"][0].clone();
        duplicate["payloads"].as_array_mut().unwrap().push(first);
        assert!(decode_checkpoint(4, duplicate).is_err());
        let mut unused = original.clone();
        unused["payloads"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!("unused"));
        assert!(decode_checkpoint(4, unused).is_err());
        assert!(decode_checkpoint(5, original).is_err());
    }

    #[test]
    fn checkpoint_payload_pool_checks_expanded_extent_before_copying_references() {
        let mut encoded = encode_checkpoint(&repeated_payload_state()).unwrap();
        encoded["payloads"][0] = serde_json::json!("large-payload".repeat(55_000));
        assert!(serde_json::to_vec(&encoded).unwrap().len() < HISTORY_REQUEST_BYTES);
        assert!(matches!(
            decode_checkpoint(4, encoded),
            Err(RuntimeServicesError::EventStore(
                crate::RuntimeEventStoreError::HistoryBudgetExceeded(_)
            ))
        ));
    }

    #[test]
    fn checkpoint_payload_pool_keeps_request_limits_and_accounts_real_wire_bytes() {
        let state = repeated_payload_state();
        let encoded = encode_checkpoint(&state).unwrap();
        let original = serde_json::to_value(state).unwrap();
        let mut compact = RequestBudget::default();
        let mut legacy = RequestBudget::default();
        compact.charge_value(&encoded).unwrap();
        legacy.charge_value(&original).unwrap();
        assert_eq!(compact.records, HISTORY_REQUEST_RECORDS);
        assert_eq!(
            compact.bytes,
            HISTORY_REQUEST_BYTES - serde_json::to_vec(&encoded).unwrap().len()
        );
        assert_eq!(
            legacy.bytes,
            HISTORY_REQUEST_BYTES - serde_json::to_vec(&original).unwrap().len()
        );
        let required_tail_and_authentication = 500_000;
        compact
            .charge(100, required_tail_and_authentication)
            .unwrap();
        assert!(legacy
            .charge(100, required_tail_and_authentication)
            .is_err());
        assert!(legacy.charge(0, 0).is_err());
        assert!(compact.charge(0, HISTORY_REQUEST_BYTES).is_err());
        assert!(compact.charge(0, 0).is_err());
        assert_eq!(HISTORY_REQUEST_BYTES, 1024 * 1024);
        assert_eq!(HISTORY_STATE_BYTES, 4 * 1024 * 1024);
        assert_eq!(HISTORY_REQUEST_RECORDS, MAX_DELTA_BATCHES);
    }

    #[tokio::test]
    async fn checkpoint_payload_pool_publishes_through_original_content_cas_and_origin() {
        let services = RuntimeServices::in_memory().unwrap();
        let state = repeated_payload_state();
        let original = serde_json::to_value(&state).unwrap();
        let encoded = encode_checkpoint(&state).unwrap();
        let mut budget = RequestBudget::default();
        budget.checkpoints.insert(
            "projection-history:checkpoint-551".into(),
            PendingCheckpoint {
                source_fence: None,
                schema: 4,
                projection_id: "projection-history:checkpoint-551".into(),
                content_scope: "session:checkpoint-551".into(),
                identity: state.identity.clone(),
                through: 203,
                expected_revision: 0,
                encoded,
            },
        );
        budget.publish(&services).unwrap();
        let saved = services
            .event_store()
            .projection_checkpoint("projection-history:checkpoint-551")
            .unwrap()
            .unwrap();
        assert_eq!(saved.source_cursor, 203);
        assert_eq!(saved.payload["schema"], 4);
        assert_eq!(saved.payload["body"]["body"]["kind"], "artifact_ref");
        assert!(services
            .event_store()
            .validate_checkpoint_content("projection-history:checkpoint-551", 203, &saved.payload,)
            .is_ok());
        for schema in [2, 6] {
            let mut invalid = saved.payload.clone();
            invalid["schema"] = serde_json::json!(schema);
            assert!(services
                .event_store()
                .validate_checkpoint_content("projection-history:checkpoint-551", 203, &invalid,)
                .is_err());
        }
        assert!(services
            .event_store()
            .validate_checkpoint_content("projection-history:checkpoint-551", 204, &saved.payload,)
            .is_err());
        let mut reader = RequestBudget::default();
        reader.charge_value(&saved).unwrap();
        reader.charge_envelope(&saved.payload["body"]).unwrap();
        let value = services
            .event_store()
            .resolve_content(
                "session:checkpoint-551",
                &saved.payload["body"],
                &crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: "projection-history:checkpoint-551".into(),
                    source_cursor: 203,
                },
            )
            .unwrap();
        let restored = decode_checkpoint(4, value).unwrap();
        assert_eq!(serde_json::to_value(restored).unwrap(), original);
        assert!(services
            .event_store()
            .resolve_content(
                "session:checkpoint-551",
                &saved.payload["body"],
                &crate::PreparedArtifactOrigin::CheckpointBody {
                    projection_id: "projection-history:foreign".into(),
                    source_cursor: 203,
                },
            )
            .is_err());
        services.shutdown_execution().await;
    }
}

#[cfg(test)]
mod r685_source_fence_identity_tests {
    use super::*;

    #[test]
    fn progress_identity_is_stable_and_bound_to_actual_source_fence() {
        let fence = crate::RuntimeSourceMutationFence { incarnation: "private-incarnation".into(), mutation_epoch: 7 };
        let mut state = HistoryState { source_fence: Some(fence), ..Default::default() };
        // Use the original prefix constructor from the sibling test module.
        state.prefix = Some(prefix::r685_test_prefix(12));
        let first = state.machine_progress().unwrap();
        let token = first.source_fence_identity.as_str();
        assert!(token.starts_with("sha256:") && token.len() == 71);
        assert!(!token.contains("private-incarnation"));
        assert_eq!(first, state.machine_progress().unwrap());
        state.source_fence.as_mut().unwrap().mutation_epoch += 1;
        assert_ne!(token, state.machine_progress().unwrap().source_fence_identity);
        state.source_fence = None;
        assert!(state.machine_progress().is_none());
    }
}

#[cfg(test)]
mod graph_source_reread_tests {
    use super::*;

    /// The prefix stage's refusal to publish a prefix prepared from a graph
    /// revision that has since moved is a RACE, not a verdict. A projection
    /// read re-reads the graph and prepares the prefix for the CURRENT head,
    /// bounded, instead of reporting a projection that does not exist.
    ///
    /// The re-read bound and the refusal predicate are the two properties that
    /// keep this honest, so they are asserted directly: a different invariant
    /// must still propagate (nothing is swallowed), the stale one is the only
    /// one retried, and the bound is finite.
    #[test]
    fn r1_585_only_a_moved_graph_head_is_reread_and_everything_else_propagates() {
        let stale = RuntimeServicesError::Invariant("history prefix: graph source changed".into());
        assert!(
            is_stale_graph_source(&stale),
            "the moved-head refusal is the retried one: {stale}"
        );
        for message in [
            "history prefix: graph ahead of requested cut",
            "history prefix: cannot persist a prefix without original planned Graph",
            "history prefix made no progress",
            "history prefix commit publication budget: required_records=3",
            "some unrelated invariant",
        ] {
            let error = RuntimeServicesError::Invariant(message.into());
            assert!(
                !is_stale_graph_source(&error),
                "this invariant must propagate unchanged: {error}"
            );
        }
        assert!(
            GRAPH_SOURCE_REREAD_LIMIT > 0 && GRAPH_SOURCE_REREAD_LIMIT < 16,
            "the re-read must be bounded: {GRAPH_SOURCE_REREAD_LIMIT}"
        );
    }

    /// A projection read still succeeds for an execution whose graph is
    /// registered and quiet, i.e. the re-read loop does not change the normal
    /// path.
    #[tokio::test]
    async fn r1_585_quiet_graph_projection_is_unchanged_by_the_reread_loop() {
        let services = RuntimeServices::in_memory().expect("runtime services");
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("quiet graph ".repeat(40));
        crate::test_support::attach_execution_graph_lineage(&mut graph);
        let graph = services.commit_service().register_graph(graph).expect("register").graph;
        let through = services.event_store().durable_commit_cursor().expect("cursor");
        let projection = harness_contract::execution_graph::project_execution_graph(&graph);
        let mut budget = RequestBudget::default();
        let scope = ExecutionProjectionScope::load_budgeted(
            &services,
            &graph.id,
            &projection,
            true,
            through,
            &mut budget,
        )
        .expect("scope");
        let states = load_scope_budgeted(&services, &scope, through, false, &mut budget)
            .expect("a quiet graph projects normally");
        assert!(!states[&graph.id].incomplete, "the projection is complete");
        services.shutdown_execution().await;
    }
}
