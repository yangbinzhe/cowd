use std::collections::VecDeque;
use std::sync::Mutex;

use crate::{HistoryView, PromptAssembly, ProviderContextInventory};

#[derive(Debug, Clone, PartialEq, Eq)]
struct RequestCompilationKey {
    context_fingerprint: u64,
    history_revision: u64,
    tool_schema_fingerprint: u64,
    permission_fingerprint: u64,
    provider_registry_revision: u64,
    model_fingerprint: u64,
}

#[derive(Debug, Clone)]
struct CachedRequestBasis {
    key: RequestCompilationKey,
    fixed_input_tokens: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RequestCompilerStats {
    pub compilations: u64,
    pub cache_hits: u64,
    pub cache_entries: usize,
}

#[derive(Debug, Clone)]
pub struct PreparedRequestBasis {
    pub history: HistoryView,
    pub fixed_input_tokens: u64,
    pub cache_hit: bool,
}

#[derive(Debug)]
pub struct PreparedRequestCompiler {
    capacity: usize,
    cache: Mutex<VecDeque<CachedRequestBasis>>,
    stats: Mutex<RequestCompilerStats>,
}

impl PreparedRequestCompiler {
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            cache: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
            stats: Mutex::new(RequestCompilerStats::default()),
        }
    }

    #[must_use]
    pub fn prepare(
        &self,
        prompt: &PromptAssembly,
        history: &HistoryView,
        inventory: ProviderContextInventory,
        permission_fingerprint: u64,
        model: &str,
    ) -> PreparedRequestBasis {
        let key = RequestCompilationKey {
            context_fingerprint: prompt.revision_fingerprint(),
            history_revision: history.cursor().revision,
            tool_schema_fingerprint: inventory.schema_fingerprint,
            permission_fingerprint,
            provider_registry_revision: inventory.provider_registry_revision,
            model_fingerprint: model_protocol::fingerprint::stable_hash_bytes(model.as_bytes()),
        };
        let mut cache = self
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(index) = cache.iter().position(|entry| entry.key == key) {
            if let Some(entry) = cache.remove(index) {
                let fixed_input_tokens = entry.fixed_input_tokens;
                cache.push_front(entry);
                let mut stats = self
                    .stats
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                stats.cache_hits = stats.cache_hits.saturating_add(1);
                stats.cache_entries = cache.len();
                return PreparedRequestBasis {
                    history: history.clone(),
                    fixed_input_tokens,
                    cache_hit: true,
                };
            }
        }

        let fixed_input_tokens = prompt
            .trusted_system_token_estimate()
            .saturating_add(history.weight().tokens)
            .saturating_add(inventory.tool_schema_tokens);
        cache.push_front(CachedRequestBasis {
            key,
            fixed_input_tokens,
        });
        while cache.len() > self.capacity {
            cache.pop_back();
        }
        let mut stats = self
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        stats.compilations = stats.compilations.saturating_add(1);
        stats.cache_entries = cache.len();
        PreparedRequestBasis {
            history: history.clone(),
            fixed_input_tokens,
            cache_hit: false,
        }
    }

    #[must_use]
    pub fn stats(&self) -> RequestCompilerStats {
        *self
            .stats
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConversationMessage, SessionHistory, SessionHistoryConfig};

    #[test]
    fn cache_uses_model_visible_schema_digest_not_control_revisions() {
        let compiler = PreparedRequestCompiler::new(16);
        let prompt = PromptAssembly::new(vec!["system".to_string()]);
        let mut session_history = SessionHistory::new(SessionHistoryConfig::default());
        session_history.append(ConversationMessage::user_text("hello"));
        let history = session_history.snapshot();
        let inventory = ProviderContextInventory {
            tool_count: 1,
            tool_schema_tokens: 20,
            catalog_revision: 1,
            exposure_revision: 1,
            schema_fingerprint: 1,
            provider_registry_revision: 1,
        };
        assert!(
            !compiler
                .prepare(&prompt, &history, inventory, 1, "model-a")
                .cache_hit
        );
        assert!(
            compiler
                .prepare(&prompt, &history, inventory, 1, "model-a")
                .cache_hit
        );
        assert!(
            compiler
                .prepare(
                    &prompt,
                    &history,
                    ProviderContextInventory {
                        exposure_revision: 2,
                        ..inventory
                    },
                    1,
                    "model-a",
                )
                .cache_hit
        );
        assert!(
            !compiler
                .prepare(&prompt, &history, inventory, 2, "model-a")
                .cache_hit
        );
        assert!(
            !compiler
                .prepare(&prompt, &history, inventory, 1, "model-b")
                .cache_hit
        );
        assert!(
            !compiler
                .prepare(
                    &PromptAssembly::new(vec!["changed-system".to_string()]),
                    &history,
                    inventory,
                    1,
                    "model-a",
                )
                .cache_hit
        );
        session_history.append(ConversationMessage::user_text("changed history"));
        assert!(
            !compiler
                .prepare(
                    &prompt,
                    &session_history.snapshot(),
                    inventory,
                    1,
                    "model-a",
                )
                .cache_hit
        );
        assert!(
            compiler
                .prepare(
                    &prompt,
                    &history,
                    ProviderContextInventory {
                        catalog_revision: 2,
                        ..inventory
                    },
                    1,
                    "model-a",
                )
                .cache_hit
        );
        assert!(
            !compiler
                .prepare(
                    &prompt,
                    &history,
                    ProviderContextInventory {
                        schema_fingerprint: 2,
                        ..inventory
                    },
                    1,
                    "model-a",
                )
                .cache_hit
        );
        assert!(
            !compiler
                .prepare(
                    &prompt,
                    &history,
                    ProviderContextInventory {
                        provider_registry_revision: 2,
                        ..inventory
                    },
                    1,
                    "model-a",
                )
                .cache_hit
        );
        assert_eq!(compiler.stats().cache_hits, 3);
        assert_eq!(compiler.stats().compilations, 7);
    }
}
