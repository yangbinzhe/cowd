use std::sync::Arc;
use crate::store::MemoryStore;

#[derive(Debug, Clone)]
pub struct Entity {
    pub id: String,
    pub name: String,
    pub entity_type: String,
}

#[derive(Debug, Clone)]
pub struct Relation {
    pub from_id: String,
    pub predicate: String,
    pub to_id: String,
}

pub struct KnowledgeGraph {
    #[allow(dead_code)] // crate deprecated; field reserved during migration
    store: Arc<MemoryStore>,
}

impl KnowledgeGraph {
    pub fn new(store: Arc<MemoryStore>) -> Self { Self { store } }

    pub fn add_entity(&self, name: &str, entity_type: &str) -> Entity {
        let id = uuid::Uuid::new_v4().to_string();
        Entity { id, name: name.to_string(), entity_type: entity_type.to_string() }
    }

    pub fn add_relation(&self, from: &str, predicate: &str, to: &str) -> Relation {
        Relation { from_id: from.to_string(), predicate: predicate.to_string(), to_id: to.to_string() }
    }
}
