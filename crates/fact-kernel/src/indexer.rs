use std::collections::{BTreeMap, BTreeSet};

use crate::core::{FactId, FactRecord};
use crate::memory::RecallQuery;

#[derive(Debug, Clone)]
pub struct FactSearchHit {
    pub fact: FactRecord,
    pub score: u32,
}

#[derive(Debug, Clone, Default)]
pub struct FactIndex {
    facts: BTreeMap<String, FactRecord>,
    token_index: BTreeMap<String, BTreeSet<String>>,
    type_index: BTreeMap<String, BTreeSet<String>>,
}

impl FactIndex {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn rebuild(&mut self, facts: &[FactRecord]) {
        self.facts.clear();
        self.token_index.clear();
        self.type_index.clear();

        for fact in facts {
            self.index_fact(fact.clone());
        }
    }

    pub fn index_fact(&mut self, fact: FactRecord) {
        let id = fact.id.as_str().to_string();
        self.type_index
            .entry(normalize_token(&fact.fact_type))
            .or_default()
            .insert(id.clone());

        for token in tokenize(&fact.statement)
            .into_iter()
            .chain(tokenize(&fact.fact_type))
        {
            self.token_index
                .entry(token)
                .or_default()
                .insert(id.clone());
        }

        self.facts.insert(id, fact);
    }

    #[must_use]
    pub fn get(&self, id: &FactId) -> Option<&FactRecord> {
        self.facts.get(id.as_str())
    }

    #[must_use]
    pub fn search(&self, query: &RecallQuery) -> Vec<FactSearchHit> {
        let limit = query.limit.max(1);
        let tokens = tokenize(&query.query);

        let mut scores: BTreeMap<String, u32> = BTreeMap::new();
        for token in tokens {
            if let Some(ids) = self.token_index.get(&token) {
                for id in ids {
                    *scores.entry(id.clone()).or_default() += 10;
                }
            }
        }

        if scores.is_empty() && !query.query.trim().is_empty() {
            let needle = query.query.to_lowercase();
            for (id, fact) in &self.facts {
                if fact.statement.to_lowercase().contains(&needle)
                    || fact.fact_type.to_lowercase().contains(&needle)
                {
                    scores.insert(id.clone(), 5);
                }
            }
        }

        let mut hits = scores
            .into_iter()
            .filter_map(|(id, score)| {
                self.facts
                    .get(&id)
                    .cloned()
                    .map(|fact| FactSearchHit { fact, score })
            })
            .collect::<Vec<_>>();

        hits.sort_by(|a, b| {
            b.score
                .cmp(&a.score)
                .then_with(|| b.fact.confidence.cmp(&a.fact.confidence))
                .then_with(|| b.fact.updated_at.cmp(&a.fact.updated_at))
        });
        hits.truncate(limit);
        hits
    }

    #[must_use]
    pub fn by_type(&self, fact_type: &str, limit: usize) -> Vec<FactRecord> {
        let Some(ids) = self.type_index.get(&normalize_token(fact_type)) else {
            return Vec::new();
        };

        ids.iter()
            .filter_map(|id| self.facts.get(id).cloned())
            .take(limit.max(1))
            .collect()
    }
}

fn tokenize(value: &str) -> Vec<String> {
    value
        .split(|ch: char| !ch.is_alphanumeric())
        .map(normalize_token)
        .filter(|token| !token.is_empty())
        .collect()
}

fn normalize_token(value: &str) -> String {
    value.trim().to_lowercase()
}
