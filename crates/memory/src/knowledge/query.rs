//! Bounded reads from the original Knowledge owner. Scope precedes admission;
//! continuations carry the same original source revisions, never a fresh head.
use std::collections::BTreeMap;

use harness_contract::knowledge::{KnowledgeCanonPack, KnowledgeNamespace, KnowledgePack};
use serde::{Deserialize, Serialize};

use super::KnowledgeStoreError;

pub const KNOWLEDGE_CANDIDATE_PAGE_SIZE: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KnowledgeCandidateScope {
    Namespace(KnowledgeNamespace),
    Activation { project_id: Option<String> },
}

impl KnowledgeCandidateScope {
    #[must_use]
    pub fn includes(&self, namespace: &KnowledgeNamespace) -> bool {
        match self {
            Self::Namespace(expected) => namespace == expected,
            Self::Activation { project_id } => match namespace {
                KnowledgeNamespace::Project(id) | KnowledgeNamespace::ProjectGroup(id) => {
                    project_id.as_deref() == Some(id.as_str())
                }
                _ => true,
            },
        }
    }

    #[must_use]
    pub fn revision_keys(&self) -> Vec<String> {
        match self {
            Self::Namespace(namespace) => vec![format!("ns:{}", namespace.key())],
            Self::Activation { project_id } => {
                let mut keys = vec!["act:shared".into()];
                if let Some(id) = project_id {
                    keys.push(format!("act:project:{id}"));
                    keys.push(format!("act:project_group:{id}"));
                }
                keys
            }
        }
    }
}

/// Original-store counters, scoped to the exact query inputs. A foreign project
/// cannot invalidate a continuation which never reads that project's payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeSourceRevision {
    pub store_id: String,
    pub counters: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KnowledgeCandidateCursor {
    pub scope: KnowledgeCandidateScope,
    pub revision: KnowledgeSourceRevision,
    pub after_pack_id: String,
}

impl KnowledgeCandidateCursor {
    pub fn validate(
        &self,
        scope: &KnowledgeCandidateScope,
        revision: &KnowledgeSourceRevision,
    ) -> Result<(), KnowledgeStoreError> {
        if &self.scope != scope || &self.revision != revision || self.after_pack_id.is_empty() {
            return Err(KnowledgeStoreError::StaleRead);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct KnowledgeCandidateSource {
    pub corpus_id: String,
    pub namespace: KnowledgeNamespace,
    pub source_ref: String,
}

#[derive(Debug, Clone)]
pub struct KnowledgeCandidatePage {
    pub sources: Vec<KnowledgeCandidateSource>,
    pub packs: Vec<KnowledgePack>,
    pub canon: Vec<KnowledgeCanonPack>,
    pub revision: KnowledgeSourceRevision,
    pub materialized_bytes: usize,
    pub next: Option<KnowledgeCandidateCursor>,
}

#[must_use]
pub fn candidate_revision_keys(namespace: &KnowledgeNamespace) -> [String; 2] {
    let key = namespace.key();
    let activation = match namespace {
        KnowledgeNamespace::Project(_) | KnowledgeNamespace::ProjectGroup(_) => {
            format!("act:{key}")
        }
        _ => "act:shared".into(),
    };
    [format!("ns:{key}"), activation]
}
