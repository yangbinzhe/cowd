//! Asynchronous Mission organization derived from canonical Root Tasks.

use std::sync::Arc;

use harness_contract::{
    mission::{
        MissionOrganizationAction, MissionOrganizationDecision, MissionOrganizationStatus,
        TaskMissionAssignmentCommand,
    },
    reality::EvidenceRef,
    task::{TaskAggregate, TaskKind, TaskMissionAssignment, TaskOrigin},
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::RuntimeServices;

const CLAIM_RECOVERY_AFTER_MS: u64 = 60_000;

#[derive(Clone)]
pub struct MissionOrganizer {
    services: Arc<RuntimeServices>,
}

impl MissionOrganizer {
    #[must_use]
    pub fn new(services: Arc<RuntimeServices>) -> Self {
        Self { services }
    }

    pub fn enqueue_root(
        &self,
        task: &TaskAggregate,
    ) -> Result<Option<MissionOrganizationDecision>, String> {
        if task.kind != TaskKind::Root
            || task.origin == TaskOrigin::System
            || task.mission_assignment == TaskMissionAssignment::ExplicitLocked
        {
            return Ok(None);
        }
        let now = now_ms();
        let default = self.services.mission_runtime().ensure_default_mission()?;
        let decision = MissionOrganizationDecision {
            decision_id: format!("mission-organization:{}", task.task_id),
            workspace_id: default.workspace_id,
            task_ids: vec![task.task_id.clone()],
            action: MissionOrganizationAction::KeepDefault,
            target_mission_id: default.mission_id,
            proposed_objective: None,
            status: MissionOrganizationStatus::Pending,
            reason: "root Task awaits bounded Mission organization".to_string(),
            candidate_count: 0,
            provider_invoked: false,
            provider_model: None,
            provider_input_tokens: 0,
            provider_output_tokens: 0,
            elapsed_ms: 0,
            rejected_reason: None,
            evidence_refs: vec![EvidenceRef::observed("task", task.task_id.clone())],
            attempt: 0,
            next_attempt_at_ms: now,
            claim_token: None,
            revision: 1,
            created_at_ms: now,
            updated_at_ms: now,
        };
        self.services
            .task_runtime_port()
            .save_organization_decision(&decision, None)
            .map(Some)
    }

    /// Recover Root Tasks created by every producer, including schedules and
    /// direct API calls. The backend query is bounded and excludes Tasks that
    /// already own a durable decision.
    pub fn enqueue_pending_roots(&self, limit: usize) -> Result<usize, String> {
        let candidates = self
            .services
            .task_runtime_port()
            .unorganized_candidates(limit)?;
        let mut enqueued = 0usize;
        for task in candidates {
            if self.enqueue_root(&task)?.is_some() {
                enqueued = enqueued.saturating_add(1);
            }
        }
        Ok(enqueued)
    }

    /// Process one durable decision. Deterministic matches avoid a Provider
    /// call; bounded ambiguous candidates are evaluated by the configured
    /// Provider outside the foreground Session execution graph.
    pub async fn run_once(
        &self,
        worker_id: &str,
        preferred_model: Option<&str>,
    ) -> Result<Option<MissionOrganizationDecision>, String> {
        if worker_id.trim().is_empty() {
            return Err("Mission organizer requires worker_id".to_string());
        }
        let now = now_ms();
        let task_port = self.services.task_runtime_port();
        let mut recoverable =
            task_port.organization_decisions(Some(MissionOrganizationStatus::Pending), 32)?;
        recoverable
            .extend(task_port.organization_decisions(Some(MissionOrganizationStatus::Failed), 32)?);
        recoverable.extend(
            task_port
                .organization_decisions(Some(MissionOrganizationStatus::Claimed), 32)?
                .into_iter()
                .filter(|decision| {
                    decision
                        .updated_at_ms
                        .saturating_add(CLAIM_RECOVERY_AFTER_MS)
                        <= now
                }),
        );
        recoverable.sort_by(|left, right| {
            left.next_attempt_at_ms
                .cmp(&right.next_attempt_at_ms)
                .then_with(|| left.created_at_ms.cmp(&right.created_at_ms))
                .then_with(|| left.decision_id.cmp(&right.decision_id))
        });
        let Some(pending) = recoverable
            .into_iter()
            .find(|decision| decision.next_attempt_at_ms <= now)
        else {
            return Ok(None);
        };
        let mut claimed = pending.clone();
        claimed.status = MissionOrganizationStatus::Claimed;
        claimed.claim_token = Some(format!("{worker_id}:{}", uuid::Uuid::new_v4()));
        claimed.attempt = claimed.attempt.saturating_add(1);
        claimed.revision = claimed.revision.saturating_add(1);
        claimed.updated_at_ms = now;
        let claimed = self
            .services
            .task_runtime_port()
            .save_organization_decision(&claimed, Some(pending.revision))?;
        match self.apply_claimed(claimed.clone(), preferred_model).await {
            Ok(mut applied) => {
                applied.status = MissionOrganizationStatus::Applied;
                applied.claim_token = None;
                applied.revision = applied.revision.saturating_add(1);
                applied.updated_at_ms = now_ms();
                self.services
                    .task_runtime_port()
                    .save_organization_decision(&applied, Some(claimed.revision))
                    .map(Some)
            }
            Err(error) => {
                let mut failed = claimed.clone();
                failed.status = MissionOrganizationStatus::Failed;
                failed.claim_token = None;
                failed.reason = error.clone();
                failed.next_attempt_at_ms = now_ms().saturating_add(30_000);
                failed.revision = failed.revision.saturating_add(1);
                failed.updated_at_ms = now_ms();
                let _ = self
                    .services
                    .task_runtime_port()
                    .save_organization_decision(&failed, Some(claimed.revision));
                Err(error)
            }
        }
    }

    async fn apply_claimed(
        &self,
        mut decision: MissionOrganizationDecision,
        preferred_model: Option<&str>,
    ) -> Result<MissionOrganizationDecision, String> {
        let started_at = std::time::Instant::now();
        let task_id = decision
            .task_ids
            .first()
            .ok_or_else(|| "organization decision has no Task".to_string())?;
        let task = self
            .services
            .task_aggregate_service()
            .get(task_id)?
            .ok_or_else(|| format!("organization Task `{task_id}` no longer exists"))?;
        if task.mission_assignment == TaskMissionAssignment::ExplicitLocked {
            decision.action = MissionOrganizationAction::KeepDefault;
            decision.reason = "Task acquired an explicit Mission lock".to_string();
            return Ok(decision);
        }
        let fingerprint = objective_fingerprint(&task.objective);
        let all_candidates = self
            .services
            .task_runtime_port()
            .organization_candidates(64)?;
        let mut matches = all_candidates
            .iter()
            .filter(|candidate| objective_fingerprint(&candidate.objective) == fingerprint)
            .cloned()
            .collect::<Vec<_>>();
        if matches.len() >= 2 {
            matches.sort_by(|left, right| left.task_id.cmp(&right.task_id));
            decision.candidate_count = matches.len();
            decision.reason = "exact normalized objective fingerprint matched".to_string();
            decision.elapsed_ms = started_at.elapsed().as_millis() as u64;
            return self.apply_cluster(decision, task, matches, fingerprint);
        }

        let mut semantic_candidates = all_candidates
            .into_iter()
            .filter(|candidate| candidate.task_id != task.task_id)
            .filter_map(|candidate| {
                let score = objective_similarity(&task.objective, &candidate.objective);
                (score >= 0.18).then_some((score, candidate))
            })
            .collect::<Vec<_>>();
        semantic_candidates.sort_by(|(left_score, left), (right_score, right)| {
            right_score
                .total_cmp(left_score)
                .then_with(|| right.updated_at_ms.cmp(&left.updated_at_ms))
                .then_with(|| left.task_id.cmp(&right.task_id))
        });
        semantic_candidates.truncate(12);
        decision.candidate_count = semantic_candidates.len();
        if semantic_candidates.is_empty() {
            decision.action = MissionOrganizationAction::KeepDefault;
            decision.reason = "no verified matching Root Task candidate".to_string();
            decision.elapsed_ms = started_at.elapsed().as_millis() as u64;
            return Ok(decision);
        }

        let registry = self.services.provider_registry();
        let snapshot = registry.pin();
        let model = preferred_model
            .filter(|model| snapshot.resolve(model).is_some())
            .map(str::to_string)
            .or_else(|| snapshot.all_models().into_iter().next());
        let Some(model) = model else {
            decision.action = MissionOrganizationAction::KeepDefault;
            decision.reason =
                "semantic candidates exist but no Provider model is configured".to_string();
            decision.rejected_reason = Some("provider_unavailable".to_string());
            decision.elapsed_ms = started_at.elapsed().as_millis() as u64;
            return Ok(decision);
        };

        let candidate_payload = semantic_candidates
            .iter()
            .map(|(score, candidate)| {
                serde_json::json!({
                    "task_id": candidate.task_id,
                    "objective": candidate.objective,
                    "mission_id": candidate.mission_id,
                    "similarity": score,
                })
            })
            .collect::<Vec<_>>();
        let client = crate::ProviderRuntimeClient::new_with_transport_and_template_cache(
            Arc::clone(registry),
            Arc::clone(self.services.provider_transport_pool()),
            Arc::clone(self.services.provider_template_cache()),
            model.clone(),
            Vec::new(),
        )?
        .with_emit_output(false);
        decision.provider_invoked = true;
        decision.provider_model = Some(model.clone());
        let completion = client
            .complete_control_analysis(
                &model,
                "You organize related Root Tasks into Missions. Return one strict JSON object only. Never invent Task or Mission IDs.",
                serde_json::json!({
                    "current": {"task_id": task.task_id, "objective": task.objective, "mission_id": task.mission_id},
                    "candidates": candidate_payload,
                    "schema": {
                        "action": "keep_default | join_existing | create_cluster",
                        "candidate_task_ids": ["existing task ids"],
                        "target_mission_id": "required only for join_existing",
                        "objective": "optional concise shared objective",
                        "reason": "short evidence-based reason"
                    }
                })
                .to_string(),
                768,
            )
            .await?;
        decision.provider_model = Some(completion.model.clone());
        decision.provider_input_tokens = u64::from(completion.input_tokens);
        decision.provider_output_tokens = u64::from(completion.output_tokens);
        if let Some(request_id) = completion.request_id {
            decision
                .evidence_refs
                .push(EvidenceRef::observed("provider_request", request_id));
        }
        let proposal = parse_provider_proposal(&completion.text)?;
        let allowed = semantic_candidates
            .iter()
            .map(|(_, candidate)| candidate.task_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if proposal
            .candidate_task_ids
            .iter()
            .any(|candidate| !allowed.contains(candidate.as_str()))
        {
            return Err("Mission organizer Provider proposed an unknown Task id".to_string());
        }
        decision.reason = proposal.reason;
        decision.elapsed_ms = started_at.elapsed().as_millis() as u64;
        match proposal.action {
            ProviderOrganizationAction::KeepDefault => {
                decision.action = MissionOrganizationAction::KeepDefault;
                decision.rejected_reason = Some("provider_kept_default".to_string());
                Ok(decision)
            }
            ProviderOrganizationAction::JoinExisting => {
                let mission_id = proposal
                    .target_mission_id
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        "join_existing proposal requires target_mission_id".to_string()
                    })?;
                let target = self
                    .services
                    .mission_runtime()
                    .aggregate(&mission_id)
                    .filter(|mission| !mission.status.is_terminal())
                    .ok_or_else(|| {
                        format!("Mission organizer target `{mission_id}` is unavailable")
                    })?;
                if !semantic_candidates
                    .iter()
                    .any(|(_, candidate)| candidate.mission_id == target.mission_id)
                {
                    return Err(
                        "join_existing target is not backed by a supplied candidate".to_string()
                    );
                }
                let mut selected = vec![task];
                selected.extend(
                    semantic_candidates
                        .into_iter()
                        .filter(|(_, candidate)| candidate.mission_id == mission_id)
                        .map(|(_, candidate)| candidate),
                );
                self.apply_assignment(
                    &mut decision,
                    selected,
                    mission_id,
                    MissionOrganizationAction::JoinExisting,
                    proposal.objective,
                )
            }
            ProviderOrganizationAction::CreateCluster => {
                let selected_ids = proposal
                    .candidate_task_ids
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>();
                let mut selected = vec![task.clone()];
                selected.extend(
                    semantic_candidates
                        .into_iter()
                        .filter(|(_, candidate)| selected_ids.contains(&candidate.task_id))
                        .map(|(_, candidate)| candidate),
                );
                if selected.len() < 2 {
                    return Err(
                        "create_cluster proposal requires at least one verified candidate"
                            .to_string(),
                    );
                }
                let cluster_key = selected
                    .iter()
                    .map(|task| task.task_id.as_str())
                    .collect::<Vec<_>>()
                    .join("\n");
                self.apply_cluster(
                    decision,
                    task,
                    selected,
                    objective_fingerprint(&cluster_key),
                )
                .map(|mut applied| {
                    if proposal.objective.is_some() {
                        applied.proposed_objective = proposal.objective;
                    }
                    applied
                })
            }
        }
    }

    fn apply_cluster(
        &self,
        mut decision: MissionOrganizationDecision,
        task: TaskAggregate,
        matches: Vec<TaskAggregate>,
        fingerprint: String,
    ) -> Result<MissionOrganizationDecision, String> {
        let mission_id = format!("mission:auto:{}", &fingerprint[..16]);
        self.services.mission_runtime().create_mission(
            mission_id.clone(),
            task.objective.clone(),
            decision.evidence_refs.clone(),
        )?;
        self.apply_assignment(
            &mut decision,
            matches,
            mission_id,
            MissionOrganizationAction::CreateCluster,
            Some(task.objective),
        )
    }

    fn apply_assignment(
        &self,
        decision: &mut MissionOrganizationDecision,
        matches: Vec<TaskAggregate>,
        mission_id: String,
        action: MissionOrganizationAction,
        proposed_objective: Option<String>,
    ) -> Result<MissionOrganizationDecision, String> {
        let command = TaskMissionAssignmentCommand {
            operation_id: format!("mission-organize-assign:{}", decision.decision_id),
            workspace_id: decision.workspace_id.clone(),
            task_ids: matches.iter().map(|task| task.task_id.clone()).collect(),
            target_mission_id: mission_id.clone(),
            assignment: TaskMissionAssignment::Automatic,
            actor: "runtime.mission_organizer".to_string(),
            expected_task_revisions: matches
                .iter()
                .map(|task| (task.task_id.clone(), task.revision))
                .collect(),
            evidence_refs: decision.evidence_refs.clone(),
        };
        let (command, preview) = self
            .services
            .task_runtime_port()
            .preview_mission_assignment(command)?;
        if preview.items.iter().any(|item| !item.allowed) {
            return Err("Mission organizer preview rejected one or more Tasks".to_string());
        }
        self.services
            .task_runtime_port()
            .assign_mission_batch(&command)?;
        decision.task_ids = command.task_ids;
        decision.action = action;
        decision.target_mission_id = mission_id;
        decision.proposed_objective = proposed_objective;
        Ok(decision.clone())
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ProviderOrganizationAction {
    KeepDefault,
    JoinExisting,
    CreateCluster,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProviderOrganizationProposal {
    action: ProviderOrganizationAction,
    #[serde(default)]
    candidate_task_ids: Vec<String>,
    #[serde(default)]
    target_mission_id: Option<String>,
    #[serde(default)]
    objective: Option<String>,
    reason: String,
}

fn parse_provider_proposal(value: &str) -> Result<ProviderOrganizationProposal, String> {
    let trimmed = value.trim();
    let json = if trimmed.starts_with('{') && trimmed.ends_with('}') {
        trimmed
    } else {
        let start = trimmed
            .find('{')
            .ok_or_else(|| "Mission organizer Provider returned no JSON object".to_string())?;
        let end = trimmed
            .rfind('}')
            .ok_or_else(|| "Mission organizer Provider returned incomplete JSON".to_string())?;
        &trimmed[start..=end]
    };
    let proposal: ProviderOrganizationProposal =
        serde_json::from_str(json).map_err(|error| format!("invalid organizer JSON: {error}"))?;
    if proposal.reason.trim().is_empty() {
        return Err("Mission organizer Provider reason must not be empty".to_string());
    }
    Ok(proposal)
}

fn objective_similarity(left: &str, right: &str) -> f64 {
    let tokens = |value: &str| {
        value
            .split(|character: char| !character.is_alphanumeric())
            .filter(|token| !token.is_empty())
            .map(str::to_lowercase)
            .collect::<std::collections::BTreeSet<_>>()
    };
    let word_score = {
        let left = tokens(left);
        let right = tokens(right);
        if left.is_empty() || right.is_empty() {
            0.0
        } else {
            let intersection = left.intersection(&right).count() as f64;
            let union = left.union(&right).count() as f64;
            intersection / union
        }
    };
    let character_ngrams = |value: &str| {
        let normalized = value
            .chars()
            .filter(|character| character.is_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect::<Vec<_>>();
        normalized
            .windows(2)
            .map(|window| window.iter().collect::<String>())
            .collect::<std::collections::BTreeSet<_>>()
    };
    let left = character_ngrams(left);
    let right = character_ngrams(right);
    let character_score = if left.is_empty() || right.is_empty() {
        0.0
    } else {
        let intersection = left.intersection(&right).count() as f64;
        let union = left.union(&right).count() as f64;
        intersection / union
    };
    word_score.max(character_score)
}

fn objective_fingerprint(value: &str) -> String {
    let normalized = value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    format!("{:x}", Sha256::digest(normalized.as_bytes()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_proposal_rejects_unknown_fields_and_empty_reason() {
        assert!(parse_provider_proposal(
            r#"{"action":"keep_default","candidate_task_ids":[],"reason":"no match","extra":true}"#,
        )
        .is_err());
        assert!(parse_provider_proposal(
            r#"{"action":"keep_default","candidate_task_ids":[],"reason":""}"#,
        )
        .is_err());
    }

    #[test]
    fn provider_proposal_accepts_one_fenced_json_object() {
        let proposal = parse_provider_proposal(
            "```json\n{\"action\":\"create_cluster\",\"candidate_task_ids\":[\"task-1\"],\"objective\":\"统一会话治理\",\"reason\":\"目标相同\"}\n```",
        )
        .expect("strict organizer proposal");
        assert!(matches!(
            proposal.action,
            ProviderOrganizationAction::CreateCluster
        ));
        assert_eq!(proposal.candidate_task_ids, vec!["task-1"]);
    }

    #[test]
    fn similarity_supports_chinese_objectives_without_whitespace() {
        let related = objective_similarity("统一会话治理与任务路由", "会话治理任务路由优化");
        let unrelated = objective_similarity("统一会话治理与任务路由", "供应链库存预测模型");
        assert!(related >= 0.18, "related score was {related}");
        assert!(related > unrelated, "{related} must exceed {unrelated}");
    }

    #[test]
    fn objective_fingerprint_normalizes_case_and_whitespace() {
        assert_eq!(
            objective_fingerprint("  Mission   Routing  "),
            objective_fingerprint("mission routing")
        );
    }

    #[test]
    fn bounded_recovery_enqueues_roots_from_non_chat_producers_once() {
        let services = Arc::new(RuntimeServices::in_memory().expect("runtime services"));
        let task_id = "task-scheduled-root";
        services
            .task_runtime_port()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: task_id.to_string(),
                mission_id: services.mission_runtime().default_mission_id().to_string(),
                kind: TaskKind::Root,
                origin: TaskOrigin::Schedule,
                origin_session_id: "session-schedule".to_string(),
                origin_turn_id: "turn-schedule".to_string(),
                root_task_id: task_id.to_string(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: TaskMissionAssignment::Default,
                mission_assigned_by: "test".to_string(),
                spec: harness_contract::task::TaskSpec::new("nightly governance"),
                evidence_refs: vec![EvidenceRef::observed("test", "schedule")],
            })
            .expect("create scheduled root");

        let organizer = MissionOrganizer::new(Arc::clone(&services));
        assert_eq!(organizer.enqueue_pending_roots(16).unwrap(), 1);
        assert_eq!(organizer.enqueue_pending_roots(16).unwrap(), 0);
        let decisions = services
            .task_runtime_port()
            .organization_decisions(None, 16)
            .unwrap();
        assert_eq!(decisions.len(), 1);
        assert_eq!(decisions[0].task_ids, vec![task_id]);
    }
}
