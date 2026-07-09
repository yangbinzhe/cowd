use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
};

use crate::{
    ContextAuthority, ContextItem, ContextRole, ContextSourceKind, ContextSourceLifecycle,
    ContextVisibility,
};

use super::{EvolutionMemoryBridge, EvolutionMemoryRecord};

pub fn evolution_memory_context_items(
    config_home: impl AsRef<Path>,
    task: &str,
    goal_ids: &[String],
    limit: usize,
) -> Result<Vec<ContextItem>, String> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let path = config_home
        .as_ref()
        .join("evolution")
        .join("evolution-memory.jsonl");
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = fs::File::open(&path).map_err(|error| error.to_string())?;
    let mut records = Vec::new();
    for line in BufReader::new(file).lines() {
        let line = line.map_err(|error| error.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        let record = serde_json::from_str::<EvolutionMemoryRecord>(&line).map_err(|error| {
            format!(
                "invalid evolution memory record in {}: {error}",
                path.display()
            )
        })?;
        if EvolutionMemoryBridge::should_activate(&record, task, goal_ids) {
            records.push(record);
        }
    }
    records.sort_by(|left, right| {
        right
            .confidence
            .partial_cmp(&left.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(records
        .into_iter()
        .take(limit)
        .map(|record| context_item_from_record(&record, task, goal_ids))
        .collect())
}

fn context_item_from_record(
    record: &EvolutionMemoryRecord,
    task: &str,
    goal_ids: &[String],
) -> ContextItem {
    let activation_reason = activation_reason(record, task, goal_ids);
    let mut item = ContextItem::new(
        format!("evolution.memory.{}", record.record_id),
        ContextSourceKind::Memory,
        ContextRole::Orientation,
        format!(
            "# Evolution runtime memory\nkind={}\ncandidate_id={}\nversion_id={}\nsummary={}\nactivation_reason={}\nevidence_refs={}\npolicy={}",
            record.kind,
            record.candidate_id,
            record.version_id.as_deref().unwrap_or("-"),
            record.summary,
            activation_reason,
            if record.evidence_refs.is_empty() {
                "none".to_string()
            } else {
                record.evidence_refs.join(",")
            },
            if record.activation_policy.is_empty() {
                "none".to_string()
            } else {
                record.activation_policy.join(",")
            }
        ),
    );
    item.authority = ContextAuthority::Derived;
    item.visibility = ContextVisibility::Shared;
    item.source_lifecycle = ContextSourceLifecycle::Durable;
    item.source_id = Some(record.record_id.clone());
    item.source_version = record.version_id.clone();
    item.source_reason = Some(activation_reason);
    item.score = record.confidence as f32;
    item.evidence = record.evidence_refs.clone();
    item
}

fn activation_reason(record: &EvolutionMemoryRecord, task: &str, goal_ids: &[String]) -> String {
    let task = task.to_ascii_lowercase();
    if task.contains("evolution") || task.contains("进化") {
        return "task_mentions_evolution".to_string();
    }
    if task.contains("self") {
        return "task_mentions_self_improvement".to_string();
    }
    if let Some(goal) = record.goal_ids.iter().find(|goal| goal_ids.contains(goal)) {
        return format!("goal_match:{goal}");
    }
    "activation_policy_match".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::EvolutionMemoryScope;

    #[test]
    fn evolution_memory_activates_without_memory_manager() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("evolution");
        std::fs::create_dir_all(&root).unwrap();
        let record = EvolutionMemoryRecord {
            record_id: "memory-1".to_string(),
            kind: "adopted_policy".to_string(),
            candidate_id: "candidate-1".to_string(),
            version_id: Some("version-1".to_string()),
            source_eval: Some("comparison-1".to_string()),
            scope: EvolutionMemoryScope::for_goals(
                "runtime".to_string(),
                vec!["context_precision".to_string()],
            ),
            goal_ids: vec!["context_precision".to_string()],
            confidence: 0.9,
            staleness: 0.0,
            summary: "Use active context policy before serial probing.".to_string(),
            evidence_refs: vec!["promotion-1".to_string()],
            activation_policy: vec!["explicit_evolution_analysis".to_string()],
        };
        std::fs::write(
            root.join("evolution-memory.jsonl"),
            format!("{}\n", serde_json::to_string(&record).unwrap()),
        )
        .unwrap();

        let items = evolution_memory_context_items(
            tmp.path(),
            "请分析自我进化能力",
            &["context_precision".to_string()],
            4,
        )
        .unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].source, ContextSourceKind::Memory);
        assert!(items[0].content.contains("candidate_id=candidate-1"));
    }
}
