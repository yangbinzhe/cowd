//! Pre-registered real-provider evaluation for automatic strategy routing.
//!
//! The corpus, order, budgets and gates are fixed before any response is
//! observed. Direct, ParallelTools and Auto run against separate Gateway
//! processes built from the same binary; only the eval-only strategy override
//! differs. Failed and timed-out samples remain in the report.

use std::{
    collections::BTreeMap,
    fs,
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

use reqwest::{
    blocking::Client,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub const AUTO_STRATEGY_SEED: u64 = 20_260_716;
pub const DEFAULT_MAX_TOKENS: u64 = 20_000_000;
pub const DEFAULT_MAX_COST_USD_MILLI: u64 = 50_000;
const BUSINESS_SAMPLE_TOKEN_LEASE: u64 = 128_000;
const JUDGE_SAMPLE_TOKEN_LEASE: u64 = 32_000;
const DEFAULT_CONDITION_CONCURRENCY: usize = 2;
const TERMINAL_MESSAGE_VISIBILITY_GRACE: Duration = Duration::from_secs(5);
const FROZEN_CORPUS_SHA256: &str =
    "d8dc4ba671dacd7a12b41d0cbe17d1cb4f2d5f5055cb2b9e7cefab2bb8c22e3c";
const FROZEN_RUBRIC_SHA256: &str =
    "3c2672ad0038c5b63abc6d6f724380d3a339e5921559dcb0b5c39e1a63039eba";

#[derive(Debug, Clone)]
pub struct AutoStrategyPairedOptions {
    pub direct_url: String,
    pub parallel_url: String,
    pub auto_url: String,
    pub provider: String,
    pub judge_model: String,
    pub output: PathBuf,
    pub corpus: PathBuf,
    pub rubric: PathBuf,
    pub repetitions: usize,
    pub timeout: Duration,
    pub poll_interval: Duration,
    pub token: Option<String>,
    pub allow_real_model: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoStrategyCorpus {
    pub schema_version: u32,
    pub corpus_id: String,
    pub seed: u64,
    pub frozen_at: String,
    pub tasks: Vec<AutoStrategyTask>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoStrategyTask {
    pub task_id: String,
    pub expected_candidate: String,
    pub prompt: String,
    pub acceptance: Vec<String>,
    pub workspace_fixture: String,
    pub provider_constraint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mutation_fixture: Option<WorkspaceMutationFixture>,
    /// Internal evaluator traffic only. Frozen business corpus entries must
    /// never set this flag.
    #[serde(default)]
    pub judge_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceMutationFixture {
    pub target_path: String,
    pub initial_content: String,
    pub expected_content_template: String,
    pub protected_path: String,
    pub protected_content: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoStrategyRubric {
    pub schema_version: u32,
    pub rubric_id: String,
    pub judge_prompt_revision: String,
    pub criteria: Vec<AutoStrategyRubricCriterion>,
    pub failure_penalty_bp: u16,
    pub timeout_penalty_bp: u16,
    pub empty_result_penalty_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AutoStrategyRubricCriterion {
    pub criterion_id: String,
    pub weight_bp: u16,
    pub instruction: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Condition {
    Direct,
    ParallelTools,
    Auto,
}

impl Condition {
    const ALL: [Self; 3] = [Self::Direct, Self::ParallelTools, Self::Auto];

    const fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::ParallelTools => "parallel_tools",
            Self::Auto => "auto",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Sample {
    task_id: String,
    repetition: usize,
    warmup: bool,
    order_index: usize,
    condition: Condition,
    status: String,
    session_id: Option<String>,
    execution_graph_id: Option<String>,
    wall_ms: u64,
    critical_path_ms: u64,
    ttft_ms: u64,
    ttft_observed: bool,
    input_tokens: u64,
    output_tokens: u64,
    cached_tokens: u64,
    cost_usd_milli: u64,
    usage_observed: bool,
    cost_observed: bool,
    cost_source: Option<String>,
    selected_candidate: Option<String>,
    models_used: Vec<String>,
    duplicate_tool_calls: u64,
    tool_calls: u64,
    max_tool_concurrency_observed: u64,
    parallel_tool_batches: u64,
    evidence_overlap_bp: u16,
    evidence_overlap_observed: bool,
    merge_cost_ms: u64,
    team_materialized: bool,
    working_state_verified: bool,
    team_child_count: usize,
    team_agent_count: usize,
    parent_merge_count: u64,
    evaluation_token_limit: u64,
    evaluation_tokens_consumed: u64,
    evaluation_budget_observed: bool,
    evaluation_budget_breached: bool,
    evaluation_control_observed: bool,
    provider_constraint: String,
    workspace_fixture: String,
    workspace_reset_verified: bool,
    workspace_mutation_verified: bool,
    workspace_changed_paths: Vec<String>,
    write_attempt_paths: Vec<String>,
    workspace_mutation_error: Option<String>,
    response: String,
    projection: Value,
    error: Option<String>,
    quality_bp: u16,
    judge: Value,
}

#[derive(Debug, Clone, Copy, Default)]
struct JudgeUsage {
    tokens: u64,
    cost_usd_milli: u64,
    usage_observed: bool,
    cost_observed: bool,
}

pub fn run_auto_strategy_paired(options: AutoStrategyPairedOptions) -> Result<Value, String> {
    validate_options(&options)?;
    let (mut corpus, corpus_hash) = load_corpus(&options.corpus)?;
    let (rubric, rubric_hash) = load_rubric(&options.rubric)?;
    if corpus_hash != FROZEN_CORPUS_SHA256 || rubric_hash != FROZEN_RUBRIC_SHA256 {
        return Err(
            "auto strategy corpus/rubric digest differs from the pre-registered frozen assets"
                .to_string(),
        );
    }
    validate_assets(&corpus, &rubric)?;
    let diagnostic_task_id = std::env::var("COWD_AUTO_STRATEGY_DIAGNOSTIC_TASK_ID")
        .ok()
        .filter(|value| !value.trim().is_empty());
    if let Some(task_id) = diagnostic_task_id.as_deref() {
        corpus.tasks.retain(|task| task.task_id == task_id);
        if corpus.tasks.len() != 1 {
            return Err(format!(
                "diagnostic task `{task_id}` is not present exactly once in the frozen corpus"
            ));
        }
    }
    let max_tokens = lowered_budget("COWD_AUTO_STRATEGY_MAX_TOKENS", DEFAULT_MAX_TOKENS)?;
    let max_cost_usd_milli = lowered_budget(
        "COWD_AUTO_STRATEGY_MAX_COST_USD_MILLI",
        DEFAULT_MAX_COST_USD_MILLI,
    )?;
    let business_sample_token_lease = lowered_budget(
        "COWD_AUTO_STRATEGY_BUSINESS_SAMPLE_TOKEN_LEASE",
        BUSINESS_SAMPLE_TOKEN_LEASE,
    )?;
    let judge_sample_token_lease = lowered_budget(
        "COWD_AUTO_STRATEGY_JUDGE_SAMPLE_TOKEN_LEASE",
        JUDGE_SAMPLE_TOKEN_LEASE,
    )?;
    let condition_concurrency = condition_concurrency()?;
    let schedule = preregistered_schedule(&corpus, options.repetitions);
    let binary_sha256 = std::env::var("COWD_EVAL_BINARY_SHA256").ok();
    let workspace_revision = std::env::var("COWD_EVAL_WORKSPACE_REVISION").ok();
    let frontend_workspace_revision = std::env::var("COWD_EVAL_FRONTEND_WORKSPACE_REVISION").ok();
    let backend_source_archive_sha256 =
        std::env::var("COWD_EVAL_BACKEND_SOURCE_ARCHIVE_SHA256").ok();
    let frontend_source_archive_sha256 =
        std::env::var("COWD_EVAL_FRONTEND_SOURCE_ARCHIVE_SHA256").ok();
    let provider_account_ref = std::env::var("COWD_EVAL_PROVIDER_ACCOUNT_REF").ok();
    let invariant_contract = json!({
        "binary_sha256": binary_sha256.clone(),
        "workspace_revision": workspace_revision.clone(),
        "frontend_workspace_revision": frontend_workspace_revision.clone(),
        "backend_source_archive_sha256": backend_source_archive_sha256.clone(),
        "frontend_source_archive_sha256": frontend_source_archive_sha256.clone(),
        "provider_account_ref": provider_account_ref.clone(),
        "provider": options.provider.clone(),
        "judge_model": options.judge_model.clone(),
        "provider_fallbacks": "disabled",
        "tool_catalog": "same-binary-runtime-inspected",
        "workspace_fixture": "workspace-v546-frozen",
        "mutation_fixture_reset": "per-sample-pristine-full-workspace-sha256",
        "evidence_seed": corpus_hash.clone(),
        "permission_mode": "dontAsk",
        "context_budget": "same-gateway-config",
        "temperature_milli": 0,
        "timeout_ms": u64::try_from(options.timeout.as_millis()).unwrap_or(u64::MAX),
        "poll_interval_ms": u64::try_from(options.poll_interval.as_millis()).unwrap_or(u64::MAX),
        "token_budget": max_tokens,
        "cost_budget_usd_milli": max_cost_usd_milli,
        "business_sample_token_lease": business_sample_token_lease,
        "judge_sample_token_lease": judge_sample_token_lease,
        "condition_concurrency": condition_concurrency,
    });
    let condition_invariant_fingerprint = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&invariant_contract)
                .map_err(|error| format!("encode evaluation invariant contract: {error}"))?
        )
    );
    let provenance = json!({
        "binary_contract": "same binary; eval-only override differs",
        "provider": options.provider,
        "judge_model": options.judge_model,
        "provider_fallbacks": "disabled",
        "corpus_id": corpus.corpus_id,
        "corpus_sha256": corpus_hash,
        "rubric_id": rubric.rubric_id,
        "rubric_sha256": rubric_hash,
        "judge_prompt_revision": rubric.judge_prompt_revision,
        "seed": corpus.seed,
        "repetitions": options.repetitions,
        "warmup_per_task": 1,
        "order": "fixed Latin rotation by task index and repetition",
        "token_budget": max_tokens,
        "cost_budget_usd_milli": max_cost_usd_milli,
        "business_sample_token_lease": business_sample_token_lease,
        "judge_sample_token_lease": judge_sample_token_lease,
        "condition_concurrency": condition_concurrency,
        "all_failures_retained": true,
        "bootstrap_cluster": "task_id",
        "binary_sha256": binary_sha256,
        "workspace_revision": workspace_revision,
        "frontend_workspace_revision": frontend_workspace_revision,
        "backend_source_archive_sha256": backend_source_archive_sha256,
        "frontend_source_archive_sha256": frontend_source_archive_sha256,
        "provider_account_ref": provider_account_ref,
        "condition_invariants": invariant_contract,
        "condition_invariant_fingerprint": condition_invariant_fingerprint,
        "temperature_milli": 0,
        "strongest_non_team_baseline_rule": "pre-registered: consider Direct and ParallelTools only; quality>=8000bp is eligible; among eligible choose lower median wall time; if neither is eligible choose higher quality; exact ties choose Direct",
        "diagnostic_task_id": diagnostic_task_id,
        "formal_claim_scope": if diagnostic_task_id.is_some() { "diagnostic-only; never a formal corpus claim" } else { "full frozen corpus" },
    });
    if !options.allow_real_model {
        return Ok(json!({
            "kind": "harness_eval.auto_strategy_paired.v1",
            "status": "not_proven",
            "reason": "real provider execution was not enabled",
            "provenance": provenance,
            "schedule": schedule,
            "samples": [],
            "gate": {"passed": false, "claim_allowed": false},
        }));
    }

    let client = build_client(&options)?;
    let endpoints = BTreeMap::from([
        (
            Condition::Direct,
            options.direct_url.trim_end_matches('/').to_string(),
        ),
        (
            Condition::ParallelTools,
            options.parallel_url.trim_end_matches('/').to_string(),
        ),
        (
            Condition::Auto,
            options.auto_url.trim_end_matches('/').to_string(),
        ),
    ]);
    // 每个条件拥有独立的阻塞连接池。SSE TTFT 观察器会长期占用连接，
    // 若三路共享一个 Client，某一路可能在发出 session 请求前等待连接池。
    let condition_clients = Condition::ALL
        .into_iter()
        .map(|condition| build_client(&options).map(|client| (condition, client)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let mut samples = Vec::new();
    let mut total_tokens = 0_u64;
    let mut total_cost = 0_u64;
    let mut budget_observation_complete = true;
    let mut budget_stopped = false;
    let mut execution_isolation_stopped = false;
    for task in &corpus.tasks {
        for repetition in 0..=options.repetitions {
            let warmup = repetition == 0;
            let scored_repetition = repetition.saturating_sub(1);
            let order = condition_order(
                corpus
                    .tasks
                    .iter()
                    .position(|candidate| candidate.task_id == task.task_id)
                    .unwrap_or(0),
                scored_repetition,
            );
            let mut group = if execution_isolation_stopped {
                order
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(order_index, condition)| {
                        not_run_sample(
                            task,
                            scored_repetition,
                            warmup,
                            order_index,
                            condition,
                            "a prior timed-out execution did not reach a terminal state after cancellation; workspace reset is unsafe",
                        )
                    })
                    .collect::<Vec<_>>()
            } else if budget_stopped
                || total_tokens >= max_tokens
                || total_cost >= max_cost_usd_milli
            {
                budget_stopped = true;
                order
                    .iter()
                    .copied()
                    .enumerate()
                    .map(|(order_index, condition)| {
                        budget_not_run_sample(
                            task,
                            scored_repetition,
                            warmup,
                            order_index,
                            condition,
                            total_tokens,
                            max_tokens,
                            total_cost,
                            max_cost_usd_milli,
                        )
                    })
                    .collect::<Vec<_>>()
            } else {
                // 三个条件拥有独立 Gateway、数据库和工作区，可按 Provider
                // 账户并发上限分波运行；同一条件内仍按组串行，保证
                // workspace reset 不发生竞争。默认双路并发，避免第三路在
                // Provider 账户排队时无限占用评测样本的 900 秒业务租约。
                // 预算按整组均分后预留，避免并发请求超卖全局硬上限。
                let condition_count = u64::try_from(order.len()).unwrap_or(1).max(1);
                let sample_token_limit = provider_admission_token_limit(
                    &options.provider,
                    max_tokens.saturating_sub(total_tokens) / condition_count,
                    max_cost_usd_milli.saturating_sub(total_cost) / condition_count,
                    business_sample_token_lease,
                );
                let Some(sample_token_limit) = sample_token_limit else {
                    budget_stopped = true;
                    samples.extend(order.iter().copied().enumerate().map(
                        |(order_index, condition)| {
                            budget_not_run_sample(
                                task,
                                scored_repetition,
                                warmup,
                                order_index,
                                condition,
                                total_tokens,
                                max_tokens,
                                total_cost,
                                max_cost_usd_milli,
                            )
                        },
                    ));
                    continue;
                };
                let mut group = Vec::with_capacity(order.len());
                for wave_start in (0..order.len()).step_by(condition_concurrency) {
                    let wave_end = wave_start
                        .saturating_add(condition_concurrency)
                        .min(order.len());
                    let wave = thread::scope(|scope| {
                        let mut workers = Vec::with_capacity(wave_end - wave_start);
                        for (relative_index, condition) in
                            order[wave_start..wave_end].iter().copied().enumerate()
                        {
                            let order_index = wave_start + relative_index;
                            let endpoint = &endpoints[&condition];
                            let client = &condition_clients[&condition];
                            let options = &options;
                            workers.push((
                                order_index,
                                condition,
                                scope.spawn(move || {
                                    run_sample(
                                        client,
                                        endpoint,
                                        options,
                                        task,
                                        scored_repetition,
                                        warmup,
                                        order_index,
                                        condition,
                                        sample_token_limit,
                                    )
                                }),
                            ));
                        }
                        workers
                            .into_iter()
                            .map(|(order_index, condition, worker)| {
                                worker.join().unwrap_or_else(|_| {
                                    let mut sample = sample_shell(
                                        task,
                                        scored_repetition,
                                        warmup,
                                        order_index,
                                        condition,
                                    );
                                    sample.status = "worker_panicked".to_string();
                                    sample.error = Some(
                                        "condition worker panicked; retained as failed sample"
                                            .to_string(),
                                    );
                                    sample
                                })
                            })
                            .collect::<Vec<_>>()
                    });
                    group.extend(wave);
                }
                group
            };
            for sample in &mut group {
                execution_isolation_stopped |= sample.status == "isolation_failed";
                total_tokens = total_tokens
                    .saturating_add(sample.input_tokens)
                    .saturating_add(sample.output_tokens)
                    .saturating_add(sample.cached_tokens);
                total_cost = total_cost.saturating_add(sample.cost_usd_milli);
                budget_observation_complete &= sample.usage_observed
                    && sample.cost_observed
                    && sample.evaluation_budget_observed
                    && !sample.evaluation_budget_breached
                    && sample.evaluation_token_limit == business_sample_token_lease
                    && sample.evaluation_tokens_consumed
                        == sample
                            .input_tokens
                            .saturating_add(sample.output_tokens)
                            .saturating_add(sample.cached_tokens);
                if total_tokens > max_tokens || total_cost > max_cost_usd_milli {
                    budget_stopped = true;
                    sample.status = "budget_exceeded".to_string();
                    sample.error = Some(format!(
                        "fail-closed budget exceeded tokens={total_tokens}/{max_tokens} cost_milli={total_cost}/{max_cost_usd_milli}"
                    ));
                }
            }
            if !warmup && !budget_stopped && !execution_isolation_stopped {
                if let Some(judge_token_limit) = provider_admission_token_limit(
                    &options.judge_model,
                    max_tokens.saturating_sub(total_tokens),
                    max_cost_usd_milli.saturating_sub(total_cost),
                    judge_sample_token_lease,
                ) {
                    let judge_usage = apply_blind_judge(
                        &client,
                        &endpoints[&Condition::Direct],
                        &options,
                        &rubric,
                        task,
                        &mut group,
                        judge_token_limit,
                    );
                    total_tokens = total_tokens.saturating_add(judge_usage.tokens);
                    total_cost = total_cost.saturating_add(judge_usage.cost_usd_milli);
                    budget_observation_complete &=
                        judge_usage.usage_observed && judge_usage.cost_observed;
                } else {
                    budget_stopped = true;
                    for sample in &mut group {
                        sample.judge = json!({
                            "judge_run_status": "budget_not_run",
                            "judge_error": format!(
                                "judge admission reservation unavailable tokens={total_tokens}/{max_tokens} cost_milli={total_cost}/{max_cost_usd_milli}"
                            ),
                            "raw": null,
                            "judge_isolation_verified": false,
                        });
                    }
                }
            } else if !warmup {
                for sample in &mut group {
                    if sample.judge.is_null() {
                        sample.judge = json!({
                            "judge_run_status": "budget_not_run",
                            "raw": null,
                        });
                    }
                }
            }
            samples.extend(group);
        }
    }
    let mut report = evaluate_samples(
        &corpus,
        samples,
        provenance,
        total_tokens,
        total_cost,
        max_tokens,
        max_cost_usd_milli,
        options.repetitions,
        budget_observation_complete,
        business_sample_token_lease,
    );
    if diagnostic_task_id.is_some() {
        let gate = &report["gate"];
        let all_samples_completed = report["samples"]
            .as_array()
            .is_some_and(|samples| {
                !samples.is_empty()
                    && samples
                        .iter()
                        .all(|sample| sample["status"].as_str() == Some("completed"))
            });
        let diagnostic_passed = all_samples_completed
            && report["completeness_bp"].as_u64() == Some(10_000)
            && gate["per_task_pair_gate"] == true
            && gate["routing_gate"] == true
            && gate["judge_isolation_gate"] == true
            && gate["provenance_complete"] == true
            && gate["budget_observation_complete"] == true
            && gate["automatic_team_materialization_gate"] == true
            && gate["workspace_mutation_gate"] == true
            && gate["workspace_reset_gate"] == true
            && gate["hard_budget_lease_gate"] == true
            && gate["baseline_topology_isolation_gate"] == true
            && gate["tool_topology_observation_gate"] == true;
        report["status"] = json!(if diagnostic_passed {
            "diagnostic_passed"
        } else {
            "diagnostic_failed"
        });
        report["gate"]["passed"] = json!(false);
        report["gate"]["claim_allowed"] = json!(false);
        report["gate"]["diagnostic_passed"] = json!(diagnostic_passed);
        report["gate"]["all_samples_completed"] = json!(all_samples_completed);
    }
    Ok(report)
}

pub fn write_auto_strategy_report(path: &Path, report: &Value) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|error| format!("create auto strategy report directory: {error}"))?;
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(report)
            .map_err(|error| format!("serialize auto strategy report: {error}"))?,
    )
    .map_err(|error| format!("write auto strategy report: {error}"))
}

fn validate_options(options: &AutoStrategyPairedOptions) -> Result<(), String> {
    if options.repetitions < 3 {
        return Err("auto-strategy-paired requires at least three valid repetitions".to_string());
    }
    for (label, url) in [
        ("direct", &options.direct_url),
        ("parallel", &options.parallel_url),
        ("auto", &options.auto_url),
    ] {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(format!("{label} URL must be http(s)"));
        }
    }
    if options.provider.trim().is_empty() || options.judge_model.trim().is_empty() {
        return Err("provider and judge model revisions are required".to_string());
    }
    Ok(())
}

fn load_corpus(path: &Path) -> Result<(AutoStrategyCorpus, String), String> {
    let bytes =
        fs::read(path).map_err(|error| format!("read corpus {}: {error}", path.display()))?;
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let corpus = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse corpus {}: {error}", path.display()))?;
    Ok((corpus, hash))
}

fn load_rubric(path: &Path) -> Result<(AutoStrategyRubric, String), String> {
    let bytes =
        fs::read(path).map_err(|error| format!("read rubric {}: {error}", path.display()))?;
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let rubric = serde_json::from_slice(&bytes)
        .map_err(|error| format!("parse rubric {}: {error}", path.display()))?;
    Ok((rubric, hash))
}

fn validate_assets(corpus: &AutoStrategyCorpus, rubric: &AutoStrategyRubric) -> Result<(), String> {
    if corpus.schema_version != 1
        || corpus.corpus_id != "auto-strategy-v1"
        || corpus.seed != AUTO_STRATEGY_SEED
        || corpus.tasks.len() != 12
    {
        return Err("auto strategy corpus identity/seed/task-count mismatch".to_string());
    }
    let mut counts = BTreeMap::<&str, usize>::new();
    let mut ids = std::collections::BTreeSet::new();
    for task in &corpus.tasks {
        if !ids.insert(task.task_id.as_str())
            || task.prompt.trim().is_empty()
            || task.acceptance.is_empty()
            || task.judge_only
            || task.mutation_fixture.as_ref().is_some_and(|fixture| {
                !task.prompt.contains("{{EXPECTED_CONTENT}}")
                    || fixture.target_path.trim().is_empty()
                    || fixture.protected_path.trim().is_empty()
                    || fixture.target_path == fixture.protected_path
                    || !fixture.expected_content_template.contains("{repetition}")
            })
        {
            return Err("auto strategy corpus contains duplicate or incomplete tasks".to_string());
        }
        *counts.entry(task.expected_candidate.as_str()).or_default() += 1;
    }
    if counts != BTreeMap::from([("direct", 4), ("parallel_tools", 4), ("team", 4)]) {
        return Err("auto strategy corpus must freeze 4 Direct/4 ParallelTools/4 Team".to_string());
    }
    let weight = rubric.criteria.iter().fold(0_u16, |total, criterion| {
        total.saturating_add(criterion.weight_bp)
    });
    if rubric.schema_version != 1
        || rubric.rubric_id != "auto-strategy-rubric-v1"
        || rubric.criteria.is_empty()
        || weight != 10_000
    {
        return Err("auto strategy rubric identity or 10,000bp weights mismatch".to_string());
    }
    Ok(())
}

fn lowered_budget(name: &str, hard_default: u64) -> Result<u64, String> {
    match std::env::var(name) {
        Ok(value) => {
            let value = value
                .parse::<u64>()
                .map_err(|_| format!("{name} must be an integer"))?;
            if value == 0 || value > hard_default {
                return Err(format!(
                    "{name} may only lower the hard limit {hard_default}"
                ));
            }
            Ok(value)
        }
        Err(_) => Ok(hard_default),
    }
}

fn condition_concurrency() -> Result<usize, String> {
    let configured = std::env::var("COWD_AUTO_STRATEGY_CONCURRENCY").ok();
    parse_condition_concurrency(configured.as_deref())
}

fn parse_condition_concurrency(configured: Option<&str>) -> Result<usize, String> {
    let concurrency = configured.map_or(Ok(DEFAULT_CONDITION_CONCURRENCY), |value| {
        value
            .parse::<usize>()
            .map_err(|_| "COWD_AUTO_STRATEGY_CONCURRENCY must be an integer".to_string())
    })?;
    if !(1..=Condition::ALL.len()).contains(&concurrency) {
        return Err(format!(
            "COWD_AUTO_STRATEGY_CONCURRENCY must be between 1 and {}",
            Condition::ALL.len()
        ));
    }
    Ok(concurrency)
}

fn provider_admission_token_limit(
    model: &str,
    remaining_tokens: u64,
    remaining_cost_usd_milli: u64,
    requested_limit: u64,
) -> Option<u64> {
    if remaining_tokens < requested_limit {
        return None;
    }
    let pricing = model_protocol::model_registry::pricing_for_model(model)?;
    let maximum_rate = pricing
        .input_cost_per_million
        .max(pricing.output_cost_per_million)
        .max(pricing.cache_creation_cost_per_million)
        .max(pricing.cache_read_cost_per_million);
    if !maximum_rate.is_finite() || maximum_rate < 0.0 {
        return None;
    }
    // USD per million tokens -> milli-USD for `requested_limit`.
    let reserved_cost = ((requested_limit as f64 * maximum_rate) / 1_000.0).ceil() as u64;
    (remaining_cost_usd_milli >= reserved_cost).then_some(requested_limit)
}

fn preregistered_schedule(corpus: &AutoStrategyCorpus, repetitions: usize) -> Value {
    Value::Array(
        corpus
            .tasks
            .iter()
            .enumerate()
            .flat_map(|(task_index, task)| {
                (0..repetitions).map(move |repetition| {
                    json!({
                        "task_id": task.task_id,
                        "repetition": repetition,
                        "order": condition_order(task_index, repetition)
                            .map(Condition::as_str),
                    })
                })
            })
            .collect(),
    )
}

fn condition_order(task_index: usize, repetition: usize) -> [Condition; 3] {
    let offset = (task_index + repetition) % Condition::ALL.len();
    [
        Condition::ALL[offset],
        Condition::ALL[(offset + 1) % 3],
        Condition::ALL[(offset + 2) % 3],
    ]
}

fn build_client(options: &AutoStrategyPairedOptions) -> Result<Client, String> {
    let mut builder =
        Client::builder().timeout(options.timeout.saturating_add(Duration::from_secs(15)));
    if let Some(token) = options
        .token
        .as_deref()
        .filter(|token| !token.trim().is_empty())
    {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|error| format!("invalid bearer token: {error}"))?,
        );
        builder = builder.default_headers(headers);
    }
    builder.build().map_err(|error| error.to_string())
}

fn sample_shell(
    task: &AutoStrategyTask,
    repetition: usize,
    warmup: bool,
    order_index: usize,
    condition: Condition,
) -> Sample {
    Sample {
        task_id: task.task_id.clone(),
        repetition,
        warmup,
        order_index,
        condition,
        status: "failed".to_string(),
        session_id: None,
        execution_graph_id: None,
        wall_ms: 0,
        critical_path_ms: 0,
        ttft_ms: 0,
        ttft_observed: false,
        input_tokens: 0,
        output_tokens: 0,
        cached_tokens: 0,
        cost_usd_milli: 0,
        usage_observed: false,
        cost_observed: false,
        cost_source: None,
        selected_candidate: None,
        models_used: Vec::new(),
        duplicate_tool_calls: 0,
        tool_calls: 0,
        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        evidence_overlap_bp: 0,
        evidence_overlap_observed: false,
        merge_cost_ms: 0,
        team_materialized: false,
        working_state_verified: false,
        team_child_count: 0,
        team_agent_count: 0,
        parent_merge_count: 0,
        evaluation_token_limit: 0,
        evaluation_tokens_consumed: 0,
        evaluation_budget_observed: false,
        evaluation_budget_breached: false,
        evaluation_control_observed: false,
        provider_constraint: task.provider_constraint.clone(),
        workspace_fixture: task.workspace_fixture.clone(),
        workspace_reset_verified: false,
        workspace_mutation_verified: task.mutation_fixture.is_none(),
        workspace_changed_paths: Vec::new(),
        write_attempt_paths: Vec::new(),
        workspace_mutation_error: None,
        response: String::new(),
        projection: Value::Null,
        error: None,
        quality_bp: 0,
        judge: Value::Null,
    }
}

#[allow(clippy::too_many_arguments)]
fn budget_not_run_sample(
    task: &AutoStrategyTask,
    repetition: usize,
    warmup: bool,
    order_index: usize,
    condition: Condition,
    total_tokens: u64,
    max_tokens: u64,
    total_cost: u64,
    max_cost: u64,
) -> Sample {
    let mut sample = sample_shell(task, repetition, warmup, order_index, condition);
    sample.status = "budget_not_run".to_string();
    sample.error = Some(format!(
        "hard budget stopped provider admission tokens={total_tokens}/{max_tokens} cost_milli={total_cost}/{max_cost}"
    ));
    sample
}

fn not_run_sample(
    task: &AutoStrategyTask,
    repetition: usize,
    warmup: bool,
    order_index: usize,
    condition: Condition,
    reason: &str,
) -> Sample {
    let mut sample = sample_shell(task, repetition, warmup, order_index, condition);
    sample.status = "not_run_after_isolation_failure".to_string();
    sample.error = Some(reason.to_string());
    sample
}

#[derive(Debug)]
struct PreparedMutationFixture {
    workspace_root: PathBuf,
    target_path: String,
    expected_content: String,
    protected_path: String,
    protected_content: String,
    before: BTreeMap<String, String>,
}

fn prepare_mutation_fixture(
    task: &AutoStrategyTask,
    condition: Condition,
    repetition: usize,
) -> Result<Option<PreparedMutationFixture>, String> {
    let Some(fixture) = task.mutation_fixture.as_ref() else {
        return Ok(None);
    };
    let (workspace, _) = condition_workspace_roots(condition)?;
    let target = safe_fixture_path(&workspace, &fixture.target_path)?;
    let protected = safe_fixture_path(&workspace, &fixture.protected_path)?;
    fs::create_dir_all(
        target
            .parent()
            .ok_or_else(|| "mutation target has no parent".to_string())?,
    )
    .map_err(|error| format!("create mutation target parent: {error}"))?;
    fs::create_dir_all(
        protected
            .parent()
            .ok_or_else(|| "protected target has no parent".to_string())?,
    )
    .map_err(|error| format!("create protected target parent: {error}"))?;
    fs::write(&target, &fixture.initial_content)
        .map_err(|error| format!("seed mutation target: {error}"))?;
    fs::write(&protected, &fixture.protected_content)
        .map_err(|error| format!("seed protected target: {error}"))?;
    let before = snapshot_workspace_tree(&workspace)?;
    Ok(Some(PreparedMutationFixture {
        workspace_root: workspace,
        target_path: fixture.target_path.clone(),
        expected_content: fixture
            .expected_content_template
            .replace("{repetition}", &repetition.to_string()),
        protected_path: fixture.protected_path.clone(),
        protected_content: fixture.protected_content.clone(),
        before,
    }))
}

fn condition_workspace_roots(condition: Condition) -> Result<(PathBuf, PathBuf), String> {
    let environment = match condition {
        Condition::Direct => "COWD_EVAL_DIRECT_WORKSPACE",
        Condition::ParallelTools => "COWD_EVAL_PARALLEL_WORKSPACE",
        Condition::Auto => "COWD_EVAL_AUTO_WORKSPACE",
    };
    let pristine_environment = match condition {
        Condition::Direct => "COWD_EVAL_DIRECT_PRISTINE",
        Condition::ParallelTools => "COWD_EVAL_PARALLEL_PRISTINE",
        Condition::Auto => "COWD_EVAL_AUTO_PRISTINE",
    };
    let workspace = PathBuf::from(
        std::env::var(environment)
            .map_err(|_| format!("{environment} is required for the mutation fixture"))?,
    );
    let workspace = workspace
        .canonicalize()
        .map_err(|error| format!("canonicalize {environment}: {error}"))?;
    let pristine = PathBuf::from(
        std::env::var(pristine_environment)
            .map_err(|_| format!("{pristine_environment} is required for the mutation fixture"))?,
    )
    .canonicalize()
    .map_err(|error| format!("canonicalize {pristine_environment}: {error}"))?;
    Ok((workspace, pristine))
}

fn reset_workspace_from_pristine(workspace: &Path, pristine: &Path) -> Result<(), String> {
    for entry in
        fs::read_dir(workspace).map_err(|error| format!("read workspace for reset: {error}"))?
    {
        let entry = entry.map_err(|error| format!("read workspace reset entry: {error}"))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|error| format!("read workspace reset file type: {error}"))?;
        if file_type.is_dir() && !file_type.is_symlink() {
            fs::remove_dir_all(&path)
                .map_err(|error| format!("remove reset directory {}: {error}", path.display()))?;
        } else {
            fs::remove_file(&path)
                .map_err(|error| format!("remove reset file {}: {error}", path.display()))?;
        }
    }
    let source = pristine.join(".");
    let status = std::process::Command::new("cp")
        .arg("-a")
        .arg(source)
        .arg(workspace)
        .status()
        .map_err(|error| format!("start pristine workspace copy: {error}"))?;
    if !status.success() {
        return Err(format!(
            "pristine workspace copy exited with status {status}"
        ));
    }
    if snapshot_workspace_tree(workspace)? != snapshot_workspace_tree(pristine)? {
        return Err("pristine workspace copy did not reproduce the registered source tree".into());
    }
    Ok(())
}

fn safe_fixture_path(workspace: &Path, relative: &str) -> Result<PathBuf, String> {
    let relative = Path::new(relative);
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                std::path::Component::ParentDir
                    | std::path::Component::RootDir
                    | std::path::Component::Prefix(_)
            )
        })
    {
        return Err(format!(
            "unsafe mutation fixture path `{}`",
            relative.display()
        ));
    }
    let path = workspace.join(relative);
    if !path.starts_with(workspace) {
        return Err(format!(
            "mutation fixture path `{}` escaped its workspace",
            relative.display()
        ));
    }
    Ok(path)
}

fn snapshot_workspace_tree(root: &Path) -> Result<BTreeMap<String, String>, String> {
    fn visit(
        root: &Path,
        current: &Path,
        snapshot: &mut BTreeMap<String, String>,
    ) -> Result<(), String> {
        for entry in fs::read_dir(current)
            .map_err(|error| format!("read fixture tree {}: {error}", current.display()))?
        {
            let entry = entry.map_err(|error| format!("read fixture entry: {error}"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|error| format!("read fixture file type: {error}"))?;
            let relative = path
                .strip_prefix(root)
                .map_err(|error| format!("strip workspace root: {error}"))?
                .to_string_lossy()
                .replace('\\', "/");
            // `.cowd` is Runtime-owned recovery/audit state, not a business
            // workspace mutation. Every other path remains in the exact gate.
            if relative == ".cowd" {
                continue;
            }
            let metadata = fs::symlink_metadata(&path)
                .map_err(|error| format!("read fixture metadata: {error}"))?;
            let mode = workspace_entry_mode(&metadata);
            if file_type.is_symlink() {
                let target = fs::read_link(&path)
                    .map_err(|error| format!("read workspace symlink: {error}"))?;
                snapshot.insert(
                    relative,
                    format!(
                        "symlink:{mode}:{:x}",
                        Sha256::digest(format!("symlink:{}", target.display()).as_bytes())
                    ),
                );
            } else if file_type.is_dir() {
                snapshot.insert(relative, format!("directory:{mode}"));
                visit(root, &path, snapshot)?;
            } else if file_type.is_file() {
                let digest = format!(
                    "{:x}",
                    Sha256::digest(
                        fs::read(&path).map_err(|error| format!("read fixture file: {error}"))?
                    )
                );
                snapshot.insert(relative, format!("file:{mode}:{digest}"));
            } else {
                snapshot.insert(relative, format!("other:{mode}"));
            }
        }
        Ok(())
    }
    let mut snapshot = BTreeMap::new();
    let root_metadata =
        fs::symlink_metadata(root).map_err(|error| format!("read workspace root: {error}"))?;
    snapshot.insert(
        ".".to_string(),
        format!("directory:{}", workspace_entry_mode(&root_metadata)),
    );
    visit(root, root, &mut snapshot)?;
    Ok(snapshot)
}

#[cfg(unix)]
fn workspace_entry_mode(metadata: &fs::Metadata) -> String {
    use std::os::unix::fs::PermissionsExt;
    format!("{:o}", metadata.permissions().mode() & 0o7777)
}

#[cfg(not(unix))]
fn workspace_entry_mode(metadata: &fs::Metadata) -> String {
    if metadata.permissions().readonly() {
        "readonly".to_string()
    } else {
        "writable".to_string()
    }
}

fn verify_mutation_fixture(prepared: &PreparedMutationFixture) -> Result<Vec<String>, String> {
    let after = snapshot_workspace_tree(&prepared.workspace_root)?;
    let mut changed = prepared
        .before
        .keys()
        .chain(after.keys())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .filter(|path| prepared.before.get(*path) != after.get(*path))
        .cloned()
        .collect::<Vec<_>>();
    changed.sort();
    if changed != vec![prepared.target_path.clone()] {
        return Err(format!(
            "mutation changed paths {:?}, expected only {}",
            changed, prepared.target_path
        ));
    }
    let target = prepared.workspace_root.join(&prepared.target_path);
    let protected = prepared.workspace_root.join(&prepared.protected_path);
    if fs::read_to_string(target).map_err(|error| error.to_string())? != prepared.expected_content {
        return Err("mutation target content does not exactly match the frozen expectation".into());
    }
    if fs::read_to_string(protected).map_err(|error| error.to_string())?
        != prepared.protected_content
    {
        return Err("protected mutation fixture changed".into());
    }
    Ok(changed)
}

#[allow(clippy::too_many_arguments)]
fn run_sample(
    client: &Client,
    endpoint: &str,
    options: &AutoStrategyPairedOptions,
    task: &AutoStrategyTask,
    repetition: usize,
    warmup: bool,
    order_index: usize,
    condition: Condition,
    evaluation_token_limit: u64,
) -> Sample {
    let mut sample = sample_shell(task, repetition, warmup, order_index, condition);
    if !task.judge_only {
        let (workspace, pristine) = match condition_workspace_roots(condition) {
            Ok(roots) => roots,
            Err(error) => {
                sample.error = Some(format!("resolve_condition_workspace:{error}"));
                return sample;
            }
        };
        if let Err(error) = reset_workspace_from_pristine(&workspace, &pristine) {
            sample.error = Some(format!("reset_condition_workspace:{error}"));
            return sample;
        }
    }
    sample.workspace_reset_verified = true;
    let prepared_mutation = match prepare_mutation_fixture(task, condition, repetition) {
        Ok(prepared) => prepared,
        Err(error) => {
            sample.workspace_mutation_error = Some(error.clone());
            sample.error = Some(format!("prepare_mutation_fixture:{error}"));
            return sample;
        }
    };
    let effective_prompt = prepared_mutation.as_ref().map_or_else(
        || task.prompt.clone(),
        |prepared| {
            task.prompt
                .replace("{{EXPECTED_CONTENT}}", &prepared.expected_content)
        },
    );
    let session = match post_json(
        client,
        endpoint,
        "/api/sessions",
        json!({"model": options.provider}),
    ) {
        Ok(value) => value,
        Err(error) => {
            sample.error = Some(format!("create_session:{error}"));
            return sample;
        }
    };
    let Some(session_id) = extract_string(&session, &["session_id", "id"]) else {
        sample.error = Some("create_session:missing_session_id".to_string());
        return sample;
    };
    sample.session_id = Some(session_id.clone());
    let ttft_observer = match start_ttft_observer(client, endpoint, &session_id) {
        Ok(observer) => observer,
        Err(error) => {
            sample.error = Some(format!("ttft_observer:{error}"));
            return sample;
        }
    };
    let started = Instant::now();
    let controlled_prompt = format!(
        "COWD_EVAL_CONTROL {}\n{}",
        json!({
            "corpus_id": "auto-strategy-v1",
            "workspace_fixture": task.workspace_fixture,
            "provider_constraint": task.provider_constraint,
            "temperature_milli": 0,
            "resource_scopes": evaluation_resource_scopes(task),
            "budget_lease_id": format!(
                "auto-strategy:{}:{}:{}:{}",
                task.task_id,
                repetition,
                condition.as_str(),
                if warmup { "warmup" } else { "scored" }
            ),
            "max_total_tokens": evaluation_token_limit,
            "prompt": "",
        }),
        effective_prompt
    );
    let admission = match post_json(
        client,
        endpoint,
        &format!("/api/sessions/{session_id}/messages"),
        json!({
            "content": controlled_prompt,
            "idempotency_key": format!(
                "auto-strategy-{}-{}-{}-{}",
                task.task_id,
                repetition,
                condition.as_str(),
                if warmup { "warmup" } else { "scored" }
            ),
        }),
    ) {
        Ok(value) => value,
        Err(error) => {
            sample.error = Some(format!("admit_message:{error}"));
            return sample;
        }
    };
    sample.execution_graph_id = admission
        .pointer("/execution/graph_id")
        .and_then(Value::as_str)
        .or_else(|| admission.get("graph_id").and_then(Value::as_str))
        .map(str::to_string);
    let Some(graph_id) = sample.execution_graph_id.clone() else {
        sample.error = Some("admission_missing_execution_graph_id".to_string());
        return sample;
    };
    let deadline = started + options.timeout;
    let mut terminal_observed_at = None;
    loop {
        match get_json(
            client,
            endpoint,
            &format!("/api/sessions/{session_id}/messages?limit=200"),
        ) {
            Ok(messages) => {
                clear_recovered_poll_error(&mut sample, &["poll_messages:"]);
                if let Some(response) = latest_assistant_text(&messages) {
                    sample.response = response;
                }
            }
            Err(error) => {
                sample.error = Some(format!("poll_messages:{error}"));
            }
        }
        match get_json(
            client,
            endpoint,
            &format!("/api/runtime/executions/{graph_id}"),
        ) {
            Ok(projection) => {
                match serde_json::from_value::<harness_contract::projection::ExecutionProjection>(
                    projection.clone(),
                ) {
                    Ok(typed) => {
                        clear_recovered_poll_error(
                            &mut sample,
                            &["poll_full_projection:", "decode_full_projection:"],
                        );
                        let terminal = projection_is_terminal_sample(&typed);
                        if terminal {
                            // Polling a growing Full projection every 500ms made the
                            // evaluator itself dominate a gateway core and repeatedly
                            // transferred the same multi-megabyte lineage. Summary is
                            // sufficient for liveness; fetch Full exactly once at the
                            // terminal boundary for metrics and audit evidence.
                            let full_projection = match get_json(
                                client,
                                endpoint,
                                &format!("/api/runtime/executions/{graph_id}?detail_scope=full"),
                            ) {
                                Ok(value) => value,
                                Err(error) => {
                                    sample.error = Some(format!("poll_full_projection:{error}"));
                                    thread::sleep(options.poll_interval);
                                    continue;
                                }
                            };
                            let full_typed = match serde_json::from_value::<
                                harness_contract::projection::ExecutionProjection,
                            >(
                                full_projection.clone()
                            ) {
                                Ok(value) => value,
                                Err(error) => {
                                    sample.error = Some(format!("decode_full_projection:{error}"));
                                    thread::sleep(options.poll_interval);
                                    continue;
                                }
                            };
                            clear_recovered_poll_error(
                                &mut sample,
                                &["poll_full_projection:", "decode_full_projection:"],
                            );
                            apply_projection_metrics(&mut sample, &full_typed);
                            let successful = projection_is_successful_sample(&sample, &full_typed);
                            sample.projection = full_projection;
                            let first_terminal =
                                *terminal_observed_at.get_or_insert_with(Instant::now);
                            if should_wait_for_terminal_response(
                                successful,
                                &sample.response,
                                first_terminal,
                                Instant::now(),
                            ) {
                                thread::sleep(options.poll_interval);
                                continue;
                            }
                            sample.wall_ms =
                                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
                            if successful && !sample.response.trim().is_empty() {
                                sample.status = "completed".to_string();
                            } else {
                                sample.status = "failed".to_string();
                                sample.error = Some(
                                    "terminal execution failed its graph/result/Team verification contract"
                                        .to_string(),
                                );
                            }
                            break;
                        } else {
                            apply_projection_metrics(&mut sample, &typed);
                            sample.projection = projection;
                        }
                    }
                    Err(error) => {
                        sample.error = Some(format!("decode_full_projection:{error}"));
                    }
                }
            }
            Err(error) => {
                sample.error = Some(format!("poll_full_projection:{error}"));
            }
        }
        if Instant::now() >= deadline {
            sample.status = "timeout".to_string();
            sample.wall_ms = u64::try_from(options.timeout.as_millis()).unwrap_or(u64::MAX);
            sample.error = Some("terminal timeout retained as penalized sample".to_string());
            if let Err(error) = cancel_and_wait_for_terminal(
                client,
                endpoint,
                &session_id,
                &graph_id,
                options.poll_interval,
            ) {
                sample.status = "isolation_failed".to_string();
                sample.error = Some(format!(
                    "timed-out sample could not be isolated after cancellation: {error}"
                ));
            }
            break;
        }
        thread::sleep(options.poll_interval);
    }
    if let Ok(first_delta_at) = ttft_observer.recv_timeout(Duration::from_millis(500)) {
        sample.ttft_ms =
            u64::try_from(first_delta_at.duration_since(started).as_millis()).unwrap_or(u64::MAX);
        sample.ttft_observed = true;
    }
    if let Ok(stats) = get_json(
        client,
        endpoint,
        &format!("/api/sessions/{session_id}/stats"),
    ) {
        if !sample.usage_observed {
            sample.input_tokens = value_u64(&stats, &["tokens.input"]);
            sample.output_tokens = value_u64(&stats, &["tokens.output"]);
            sample.usage_observed = sample.input_tokens > 0 || sample.output_tokens > 0;
        }
    }
    apply_observed_cost(&mut sample, &options.provider);
    if let Some(prepared) = prepared_mutation.as_ref() {
        match verify_mutation_fixture(prepared) {
            Ok(changed) => {
                sample.workspace_mutation_verified = true;
                sample.workspace_changed_paths = changed;
            }
            Err(error) => {
                sample.workspace_mutation_error = Some(error.clone());
                sample.error = Some(format!("verify_mutation_fixture:{error}"));
                if sample.status == "completed" {
                    sample.status = "failed".to_string();
                }
            }
        }
    }
    sample
}

/// Admission returns the durable ingress graph identity before asynchronous
/// session dispatch necessarily makes its projection visible. A 404/403 (or a
/// partially decoded projection) in that short window is only a poll state, not
/// a permanent sample failure. Once the same channel returns a valid response,
/// remove only that channel's transient error and retain every unrelated error.
fn clear_recovered_poll_error(sample: &mut Sample, recovered_prefixes: &[&str]) {
    if sample.error.as_deref().is_some_and(|error| {
        recovered_prefixes
            .iter()
            .any(|prefix| error.starts_with(prefix))
    }) {
        sample.error = None;
    }
}

fn cancel_and_wait_for_terminal(
    client: &Client,
    endpoint: &str,
    session_id: &str,
    graph_id: &str,
    poll_interval: Duration,
) -> Result<(), String> {
    post_json(
        client,
        endpoint,
        &format!("/api/sessions/{session_id}/cancel"),
        json!({"reason": "auto-strategy evaluator timeout isolation"}),
    )
    .map_err(|error| format!("cancel timed-out session: {error}"))?;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let projection = get_json(
            client,
            endpoint,
            &format!("/api/runtime/executions/{graph_id}?detail_scope=full"),
        )
        .and_then(|value| {
            serde_json::from_value::<harness_contract::projection::ExecutionProjection>(value)
                .map_err(|error| format!("decode cancelled execution projection: {error}"))
        })?;
        let child_executions_terminal = projection.child_executions.iter().all(|child| {
            matches!(
                child.status.as_str(),
                "completed" | "failed" | "cancelled" | "blocked"
            )
        });
        let agents_terminal = projection.agents.iter().all(|agent| {
            agent.status.as_deref().is_some_and(|status| {
                matches!(status, "completed" | "failed" | "cancelled" | "blocked")
            })
        });
        if !projection.graph.nodes.is_empty()
            && projection
                .graph
                .nodes
                .iter()
                .all(|node| node.status.is_terminal())
            && child_executions_terminal
            && agents_terminal
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(
                "cancelled parent/child/Agent execution lineage remained non-terminal for 15 seconds"
                    .to_string(),
            );
        }
        thread::sleep(poll_interval.min(Duration::from_secs(1)));
    }
}

fn projection_is_terminal_sample(
    projection: &harness_contract::projection::ExecutionProjection,
) -> bool {
    let graph_terminal = !projection.graph.nodes.is_empty()
        && projection
            .graph
            .nodes
            .iter()
            .all(|node| node.status.is_terminal());
    let exact_outcome = projection
        .strategy
        .as_ref()
        .and_then(|strategy| strategy.actual.as_ref())
        .is_some();
    graph_terminal && exact_outcome
}

fn projection_is_successful_sample(
    sample: &Sample,
    projection: &harness_contract::projection::ExecutionProjection,
) -> bool {
    projection.graph.terminal_result_ref.is_some()
        && projection.graph.nodes.iter().all(|node| {
            node.status == harness_contract::execution_graph::ExecutionNodeStatus::Completed
        })
        && (sample.selected_candidate.as_deref() != Some("team") || sample.working_state_verified)
}

fn should_wait_for_terminal_response(
    successful: bool,
    response: &str,
    first_terminal: Instant,
    now: Instant,
) -> bool {
    successful
        && response.trim().is_empty()
        && now.saturating_duration_since(first_terminal) < TERMINAL_MESSAGE_VISIBILITY_GRACE
}

fn apply_projection_metrics(
    sample: &mut Sample,
    projection: &harness_contract::projection::ExecutionProjection,
) {
    let Some(strategy) = projection.strategy.as_ref() else {
        return;
    };
    sample.selected_candidate = strategy
        .selected_candidate
        .map(|candidate| candidate.as_str().to_string());
    let sample_source = strategy
        .resource_snapshot
        .as_ref()
        .map(|snapshot| snapshot.sample_source.as_str())
        .unwrap_or_default();
    sample.evaluation_control_observed = sample_source
        .contains(&format!("workspace_fixture={}", sample.workspace_fixture))
        && sample_source.contains(&format!(
            "provider_constraint={}",
            sample.provider_constraint
        ))
        && sample_source.contains("temperature_milli=0");
    let mut models = projection
        .graph
        .nodes
        .iter()
        .filter_map(|node| node.usage.model.clone())
        .collect::<std::collections::BTreeSet<_>>();
    models.extend(projection.agents.iter().filter_map(|agent| {
        agent
            .detail
            .as_ref()
            .and_then(|detail| detail.get("model"))
            .and_then(Value::as_str)
            .map(str::to_string)
    }));
    sample.models_used = models.into_iter().collect();
    let Some(outcome) = strategy.actual.as_ref() else {
        return;
    };
    sample.duplicate_tool_calls = outcome.duplicate_tool_calls;
    sample.write_attempt_paths = outcome.write_attempt_refs.clone();
    sample.write_attempt_paths.sort();
    sample.write_attempt_paths.dedup();
    sample.tool_calls = outcome.tool_calls;
    sample.max_tool_concurrency_observed = outcome.max_tool_concurrency_observed;
    sample.parallel_tool_batches = outcome.parallel_tool_batches;
    sample.evidence_overlap_bp = outcome.evidence_overlap_bp;
    sample.evidence_overlap_observed = outcome.evidence_overlap_observed;
    sample.working_state_verified = outcome.working_state_verified;
    sample.merge_cost_ms = outcome.merge_cost_ms;
    sample.critical_path_ms = outcome.duration_ms;
    let strategy_input_tokens = outcome.input_tokens;
    let strategy_output_tokens = outcome.output_tokens;
    sample.cached_tokens = outcome.cached_tokens;
    if strategy_input_tokens > 0 || strategy_output_tokens > 0 {
        sample.input_tokens = strategy_input_tokens;
        sample.output_tokens = strategy_output_tokens;
        sample.usage_observed = true;
    }
    sample.parent_merge_count = u64::from(outcome.parent_merge_count);
    sample.evaluation_token_limit = outcome.evaluation_token_limit;
    sample.evaluation_tokens_consumed = outcome.evaluation_tokens_consumed;
    sample.evaluation_budget_observed = outcome.evaluation_budget_observed;
    sample.evaluation_budget_breached = outcome.evaluation_budget_breached;
    sample.team_child_count = projection.child_executions.len();
    sample.team_agent_count = projection.agents.len();
    let child_terminal = !projection.child_executions.is_empty()
        && projection.child_executions.iter().all(|child| {
            matches!(
                child.status.as_str(),
                "completed" | "failed" | "cancelled" | "blocked"
            )
        });
    sample.team_materialized = sample.selected_candidate.as_deref() == Some("team")
        && sample.team_child_count >= 1
        && sample.team_agent_count >= 2
        && child_terminal
        && sample.parent_merge_count == 1;
}

fn apply_blind_judge(
    client: &Client,
    endpoint: &str,
    options: &AutoStrategyPairedOptions,
    rubric: &AutoStrategyRubric,
    task: &AutoStrategyTask,
    samples: &mut [Sample],
    evaluation_token_limit: u64,
) -> JudgeUsage {
    let labels = ["A", "B", "C"];
    let mut ordered = samples.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|sample| {
        stable_u64(format!("{}:{}", task.task_id, sample.condition.as_str()).as_bytes())
    });
    let outputs = ordered
        .iter()
        .zip(labels)
        .map(|(sample, label)| {
            format!(
                "OUTPUT {label}\nstatus={}\n{}",
                sample.status, sample.response
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    let criteria = rubric
        .criteria
        .iter()
        .map(|criterion| {
            format!(
                "{} weight={}bp: {}",
                criterion.criterion_id, criterion.weight_bp, criterion.instruction
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let prompt = format!(
        "You are a blind evaluator. Strategy labels and execution order are hidden. Do not call tools. Do not use tools. Do not inspect the workspace; judge only the frozen task contract and candidate outputs below. Score each output independently against the task and rubric. Return strict JSON only: {{\"scores\":{{\"A\":0,\"B\":0,\"C\":0}},\"reasons\":{{\"A\":\"...\",\"B\":\"...\",\"C\":\"...\"}},\"judge_model_revision\":\"{}\"}}. Scores are integer basis points 0..10000.\n\nTASK\n{}\n\nACCEPTANCE\n{}\n\nRUBRIC\n{}\n\n{}",
        options.judge_model,
        rendered_task_prompt(task, samples.first().map_or(0, |sample| sample.repetition)),
        task.acceptance.join("; "),
        criteria,
        outputs
    );
    let judge_task = AutoStrategyTask {
        task_id: format!("judge:{}", task.task_id),
        expected_candidate: "direct".to_string(),
        prompt,
        acceptance: vec!["strict JSON scores".to_string()],
        workspace_fixture: "none".to_string(),
        provider_constraint: "judge".to_string(),
        mutation_fixture: None,
        judge_only: true,
    };
    let mut judge_options = options.clone();
    judge_options.provider.clone_from(&options.judge_model);
    let judge = run_sample(
        client,
        endpoint,
        &judge_options,
        &judge_task,
        samples.first().map_or(0, |sample| sample.repetition),
        false,
        0,
        Condition::Direct,
        evaluation_token_limit,
    );
    let parsed = extract_json_object(&judge.response)
        .filter(|value| valid_judge_output(value, &labels, &options.judge_model));
    let judge_isolation_verified = judge.status == "completed"
        && judge.selected_candidate.as_deref() == Some("direct")
        && judge.tool_calls == 0
        && judge.team_child_count == 0
        && judge.team_agent_count == 0
        && exact_model_revisions(&judge.models_used, &options.judge_model)
        && judge.evaluation_budget_observed
        && !judge.evaluation_budget_breached
        && judge.evaluation_token_limit == evaluation_token_limit
        && judge.evaluation_tokens_consumed
            == judge
                .input_tokens
                .saturating_add(judge.output_tokens)
                .saturating_add(judge.cached_tokens)
        && parsed.is_some();
    let label_by_condition = ordered
        .iter()
        .zip(labels)
        .map(|(sample, label)| (sample.condition, label))
        .collect::<BTreeMap<_, _>>();
    drop(ordered);
    for sample in samples {
        let label = label_by_condition
            .get(&sample.condition)
            .copied()
            .unwrap_or("A");
        let base_score = parsed
            .as_ref()
            .and_then(|value| value.pointer(&format!("/scores/{label}")))
            .and_then(Value::as_u64)
            .and_then(|score| u16::try_from(score.min(10_000)).ok())
            .unwrap_or(0);
        sample.quality_bp = match sample.status.as_str() {
            "completed" if sample.response.trim().is_empty() => {
                base_score.saturating_sub(rubric.empty_result_penalty_bp)
            }
            "completed" => base_score,
            "timeout" => base_score.saturating_sub(rubric.timeout_penalty_bp),
            _ => base_score.saturating_sub(rubric.failure_penalty_bp),
        };
        sample.judge = json!({
            "label": label,
            "rubric_id": rubric.rubric_id,
            "judge_model_revision": options.judge_model,
            "raw": parsed.clone(),
            "judge_run_status": judge.status.clone(),
            "judge_error": judge.error.clone(),
            "observed_models": judge.models_used.clone(),
            "judge_isolation_verified": judge_isolation_verified,
        });
    }
    JudgeUsage {
        tokens: judge
            .input_tokens
            .saturating_add(judge.output_tokens)
            .saturating_add(judge.cached_tokens),
        cost_usd_milli: judge.cost_usd_milli,
        usage_observed: judge.usage_observed,
        cost_observed: judge.cost_observed,
    }
}

fn rendered_task_prompt(task: &AutoStrategyTask, repetition: usize) -> String {
    task.mutation_fixture.as_ref().map_or_else(
        || task.prompt.clone(),
        |fixture| {
            task.prompt.replace(
                "{{EXPECTED_CONTENT}}",
                &fixture
                    .expected_content_template
                    .replace("{repetition}", &repetition.to_string()),
            )
        },
    )
}

fn evaluation_resource_scopes(task: &AutoStrategyTask) -> Vec<String> {
    task.mutation_fixture
        .as_ref()
        .map_or_else(Vec::new, |fixture| {
            // The mutation ceiling constrains effects while preserving the
            // two exact evidence paths required by this frozen task. Keep the
            // reads bounded so automatic Team focus leases remain valid; the
            // verifier still rejects every changed or attempted write path
            // outside the target.
            vec![
                format!("read:{}", fixture.target_path),
                format!("read:{}", fixture.protected_path),
                format!("write:{}", fixture.target_path),
            ]
        })
}

fn valid_judge_output(value: &Value, labels: &[&str], judge_model: &str) -> bool {
    value.get("judge_model_revision").and_then(Value::as_str) == Some(judge_model)
        && labels.iter().all(|label| {
            value
                .pointer(&format!("/scores/{label}"))
                .and_then(Value::as_u64)
                .is_some_and(|score| score <= 10_000)
                && value
                    .pointer(&format!("/reasons/{label}"))
                    .and_then(Value::as_str)
                    .is_some_and(|reason| !reason.trim().is_empty())
        })
}

fn evaluate_samples(
    corpus: &AutoStrategyCorpus,
    samples: Vec<Sample>,
    provenance: Value,
    total_tokens: u64,
    total_cost: u64,
    max_tokens: u64,
    max_cost: u64,
    repetitions: usize,
    budget_observation_complete: bool,
    business_sample_token_lease: u64,
) -> Value {
    let scored = samples
        .iter()
        .filter(|sample| !sample.warmup)
        .collect::<Vec<_>>();
    let expected = corpus
        .tasks
        .len()
        .saturating_mul(3)
        .saturating_mul(repetitions);
    let complete = scored
        .iter()
        .filter(|sample| sample.status == "completed")
        .count();
    let completeness_bp = if expected == 0 {
        0
    } else {
        u16::try_from(complete.saturating_mul(10_000) / expected).unwrap_or(10_000)
    };
    let mut task_comparisons = Vec::new();
    for task in &corpus.tasks {
        let task_samples = scored
            .iter()
            .copied()
            .filter(|sample| sample.task_id == task.task_id)
            .collect::<Vec<_>>();
        let mut repetitions_by_condition =
            BTreeMap::<usize, std::collections::BTreeSet<Condition>>::new();
        for sample in &task_samples {
            if sample.status == "completed"
                && sample.judge.get("raw").is_some_and(|raw| !raw.is_null())
            {
                repetitions_by_condition
                    .entry(sample.repetition)
                    .or_default()
                    .insert(sample.condition);
            }
        }
        let valid_pair_count = repetitions_by_condition
            .values()
            .filter(|conditions| conditions.len() == 3)
            .count();
        let auto = summarize(&task_samples, Condition::Auto);
        let direct = summarize(&task_samples, Condition::Direct);
        let parallel = summarize(&task_samples, Condition::ParallelTools);
        let strongest = select_preregistered_baseline(&direct, &parallel);
        let quality_delta_bp = i32::from(auto.quality_bp) - i32::from(strongest.quality_bp);
        let speedup_bp = if strongest.critical_path_median_ms == 0 {
            i64::MIN / 4
        } else {
            i64::try_from(
                strongest
                    .critical_path_median_ms
                    .saturating_sub(auto.critical_path_median_ms)
                    .saturating_mul(10_000)
                    .saturating_div(strongest.critical_path_median_ms),
            )
            .unwrap_or(i64::MAX)
        };
        let regression_bp = if strongest.critical_path_median_ms == 0 {
            i64::MAX / 4
        } else {
            i64::try_from(
                auto.critical_path_median_ms
                    .saturating_sub(strongest.critical_path_median_ms)
                    .saturating_mul(10_000)
                    .saturating_div(strongest.critical_path_median_ms),
            )
            .unwrap_or(i64::MAX)
        };
        let speed_channel_margin = (speedup_bp - 2_000).min(i64::from(quality_delta_bp) + 200);
        let quality_channel_margin =
            (i64::from(quality_delta_bp) - 1_000).min(1_000 - regression_bp);
        task_comparisons.push(json!({
            "task_id": task.task_id,
            "expected_candidate": task.expected_candidate,
            "direct": direct,
            "parallel_tools": parallel,
            "auto": auto,
            "strongest_non_team_baseline": strongest.condition.as_str(),
            "paired_wall_delta_ms": i128::from(auto.wall_median_ms) - i128::from(strongest.wall_median_ms),
            "paired_critical_path_delta_ms": i128::from(auto.critical_path_median_ms) - i128::from(strongest.critical_path_median_ms),
            "paired_quality_delta_bp": quality_delta_bp,
            "preregistered_channel_margin_bp": speed_channel_margin.max(quality_channel_margin),
            "valid_pair_count": valid_pair_count,
        }));
    }
    let team_deltas = task_comparisons
        .iter()
        .filter(|comparison| comparison["expected_candidate"] == "team")
        .filter_map(|comparison| comparison["paired_critical_path_delta_ms"].as_i64())
        .collect::<Vec<_>>();
    let ci = cluster_bootstrap_ci(&team_deltas, AUTO_STRATEGY_SEED);
    let team_channel_margins = task_comparisons
        .iter()
        .filter(|comparison| comparison["expected_candidate"] == "team")
        .filter_map(|comparison| comparison["preregistered_channel_margin_bp"].as_i64())
        .collect::<Vec<_>>();
    let channel_margin_ci = cluster_bootstrap_ci(&team_channel_margins, AUTO_STRATEGY_SEED);
    let sign_inputs = team_channel_margins
        .iter()
        .map(|margin| margin.saturating_neg())
        .collect::<Vec<_>>();
    let paired_sign_confidence_bp = paired_sign_confidence_bp(&sign_inputs);
    let team_gate = task_comparisons
        .iter()
        .filter(|comparison| comparison["expected_candidate"] == "team")
        .all(|comparison| {
            let auto_critical_path = comparison
                .pointer("/auto/critical_path_median_ms")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX);
            let baseline_condition = comparison["strongest_non_team_baseline"]
                .as_str()
                .unwrap_or("direct");
            let baseline = comparison.get(baseline_condition).unwrap_or(&Value::Null);
            let baseline_critical_path = baseline["critical_path_median_ms"].as_u64().unwrap_or(0);
            let quality_delta = comparison["paired_quality_delta_bp"]
                .as_i64()
                .unwrap_or(i64::MIN);
            let auto_tokens = comparison
                .pointer("/auto/input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                .saturating_add(
                    comparison
                        .pointer("/auto/output_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                )
                .saturating_add(
                    comparison
                        .pointer("/auto/cached_tokens")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                );
            let baseline_tokens = baseline["input_tokens"]
                .as_u64()
                .unwrap_or(0)
                .saturating_add(baseline["output_tokens"].as_u64().unwrap_or(0))
                .saturating_add(baseline["cached_tokens"].as_u64().unwrap_or(0));
            let token_gate = quality_delta >= 1_000
                || (baseline_tokens > 0
                    && auto_tokens.saturating_mul(10) <= baseline_tokens.saturating_mul(18));
            let duplicate_gate = comparison
                .pointer("/auto/duplicate_tool_ratio_bp")
                .and_then(Value::as_u64)
                .is_some_and(|ratio| ratio < 1_500);
            let speed_channel = baseline_critical_path > 0
                && auto_critical_path.saturating_mul(100)
                    <= baseline_critical_path.saturating_mul(80)
                && quality_delta >= -200;
            let quality_channel = quality_delta >= 1_000
                && baseline_critical_path > 0
                && auto_critical_path.saturating_mul(100)
                    <= baseline_critical_path.saturating_mul(110);
            (speed_channel || quality_channel) && token_gate && duplicate_gate
        });
    let per_task_pair_gate = task_comparisons
        .iter()
        .all(|comparison| comparison["valid_pair_count"].as_u64().unwrap_or(0) >= 3);
    let routing_gate = task_comparisons.iter().all(|comparison| {
        let expected = comparison["expected_candidate"]
            .as_str()
            .unwrap_or("unknown");
        let task_id = comparison["task_id"].as_str().unwrap_or("");
        let observed = scored
            .iter()
            .filter(|sample| sample.task_id == task_id)
            .filter(|sample| sample.condition == Condition::Auto)
            .filter_map(|sample| sample.selected_candidate.as_deref())
            .collect::<std::collections::BTreeSet<_>>();
        observed.len() == 1 && observed.contains(expected)
    });
    let non_empty = |key: &str| {
        provenance[key]
            .as_str()
            .is_some_and(|value| !value.trim().is_empty())
    };
    let binary_digest_valid = provenance["binary_sha256"]
        .as_str()
        .is_some_and(valid_sha256);
    let run_provenance_complete = binary_digest_valid
        && non_empty("workspace_revision")
        && non_empty("frontend_workspace_revision")
        && provenance["backend_source_archive_sha256"]
            .as_str()
            .is_some_and(valid_sha256)
        && provenance["frontend_source_archive_sha256"]
            .as_str()
            .is_some_and(valid_sha256)
        && non_empty("provider_account_ref");
    let requested_provider = provenance["provider"].as_str().unwrap_or("");
    let requested_judge = provenance["judge_model"].as_str().unwrap_or("");
    let judge_isolation_gate = scored
        .iter()
        .all(|sample| sample.judge["judge_isolation_verified"] == true);
    let provenance_complete = run_provenance_complete
        && scored.iter().all(|sample| {
            sample.execution_graph_id.is_some()
                && sample.selected_candidate.is_some()
                && sample.judge.get("raw").is_some_and(|raw| !raw.is_null())
                && sample.judge["judge_run_status"] == "completed"
                && sample.judge["judge_isolation_verified"] == true
                && sample.ttft_observed
                && sample.critical_path_ms > 0
                && sample.usage_observed
                && sample.cost_observed
                && sample.evaluation_control_observed
                && sample.evaluation_budget_observed
                && !sample.evaluation_budget_breached
                && sample.evaluation_token_limit == business_sample_token_lease
                && sample.evaluation_tokens_consumed
                    == sample
                        .input_tokens
                        .saturating_add(sample.output_tokens)
                        .saturating_add(sample.cached_tokens)
                && exact_model_revisions(&sample.models_used, requested_provider)
                && sample
                    .judge
                    .get("observed_models")
                    .and_then(Value::as_array)
                    .is_some_and(|models| {
                        !models.is_empty()
                            && models
                                .iter()
                                .all(|model| model.as_str() == Some(requested_judge))
                    })
        });
    let automatic_team_materialization_gate = corpus
        .tasks
        .iter()
        .filter(|task| task.expected_candidate == "team")
        .all(|task| {
            let automatic = scored
                .iter()
                .filter(|sample| {
                    sample.task_id == task.task_id && sample.condition == Condition::Auto
                })
                .collect::<Vec<_>>();
            automatic.len() == repetitions
                && automatic.into_iter().all(|sample| {
                    sample.team_materialized
                        && sample.working_state_verified
                        && (task.mutation_fixture.is_some() || sample.evidence_overlap_observed)
                })
        });
    let workspace_mutation_gate = corpus
        .tasks
        .iter()
        .filter(|task| task.mutation_fixture.is_some())
        .all(|task| {
            let mutation_samples = scored
                .iter()
                .filter(|sample| sample.task_id == task.task_id)
                .collect::<Vec<_>>();
            mutation_samples.len() == repetitions.saturating_mul(Condition::ALL.len())
                && mutation_samples.into_iter().all(|sample| {
                    sample.workspace_mutation_verified
                        && sample.workspace_changed_paths
                            == vec![
                                task.mutation_fixture
                                    .as_ref()
                                    .expect("filtered mutation fixture")
                                    .target_path
                                    .clone(),
                            ]
                        && sample.write_attempt_paths
                            == vec![
                                task.mutation_fixture
                                    .as_ref()
                                    .expect("filtered mutation fixture")
                                    .target_path
                                    .clone(),
                            ]
                        && sample.workspace_mutation_error.is_none()
                })
        });
    let workspace_reset_gate = scored.iter().all(|sample| sample.workspace_reset_verified);
    let hard_budget_lease_gate = samples
        .iter()
        .filter(|sample| sample.execution_graph_id.is_some())
        .all(|sample| {
            sample.evaluation_budget_observed
                && !sample.evaluation_budget_breached
                && sample.evaluation_token_limit == business_sample_token_lease
                && sample.evaluation_tokens_consumed
                    == sample
                        .input_tokens
                        .saturating_add(sample.output_tokens)
                        .saturating_add(sample.cached_tokens)
                && sample.evaluation_tokens_consumed <= sample.evaluation_token_limit
        });
    let baseline_topology_isolation_gate = scored.iter().all(|sample| match sample.condition {
        Condition::Direct | Condition::ParallelTools => {
            !sample.team_materialized
                && !sample.working_state_verified
                && sample.parent_merge_count == 0
                && sample.team_child_count == 0
                && sample.team_agent_count == 0
        }
        Condition::Auto => true,
    });
    let tool_topology_observation_gate = scored.iter().all(|sample| {
        if sample.condition == Condition::Direct {
            sample.max_tool_concurrency_observed <= 1 && sample.parallel_tool_batches == 0
        } else if sample.condition == Condition::ParallelTools
            && corpus.tasks.iter().any(|task| {
                task.task_id == sample.task_id && task.expected_candidate == "parallel_tools"
            })
        {
            sample.max_tool_concurrency_observed >= 2 && sample.parallel_tool_batches >= 1
        } else if sample.condition == Condition::Auto
            && sample.selected_candidate.as_deref() == Some("parallel_tools")
        {
            sample.max_tool_concurrency_observed >= 2 && sample.parallel_tool_batches >= 1
        } else {
            true
        }
    });
    let gate_passed = completeness_bp >= 9_000
        && team_gate
        && per_task_pair_gate
        && routing_gate
        && judge_isolation_gate
        && provenance_complete
        && budget_observation_complete
        && automatic_team_materialization_gate
        && workspace_mutation_gate
        && workspace_reset_gate
        && hard_budget_lease_gate
        && baseline_topology_isolation_gate
        && tool_topology_observation_gate
        && total_tokens <= max_tokens
        && total_cost <= max_cost
        && channel_margin_ci
            .get("lower")
            .and_then(Value::as_i64)
            .is_some_and(|lower| lower >= 0)
        && paired_sign_confidence_bp >= 9_000;
    let strategy_calibration_records = if gate_passed {
        build_strategy_calibration_records(
            corpus,
            &task_comparisons,
            &samples,
            repetitions,
            &provenance,
        )
    } else {
        Vec::new()
    };
    json!({
        "kind": "harness_eval.auto_strategy_paired.v1",
        "status": if gate_passed {
            "passed"
        } else if provenance_complete && budget_observation_complete {
            "failed"
        } else {
            "not_proven"
        },
        "provenance": provenance,
        "budget": {
            "tokens_used": total_tokens,
            "tokens_limit": max_tokens,
            "cost_usd_milli_used": total_cost,
            "cost_usd_milli_limit": max_cost,
            "judge_included": budget_observation_complete,
            "observation_complete": budget_observation_complete,
        },
        "completeness_bp": completeness_bp,
        "task_comparisons": task_comparisons,
        "team_cluster_bootstrap_critical_path_delta_95ci": ci,
        "team_cluster_bootstrap_channel_margin_95ci": channel_margin_ci,
        "evolution_paired_sign_confidence_bp": paired_sign_confidence_bp,
        "strategy_calibration_records": strategy_calibration_records,
        "samples": samples,
        "gate": {
            "passed": gate_passed,
            "claim_allowed": gate_passed,
            "team_gate": team_gate,
            "per_task_pair_gate": per_task_pair_gate,
            "routing_gate": routing_gate,
            "judge_isolation_gate": judge_isolation_gate,
            "provenance_complete": provenance_complete,
            "budget_observation_complete": budget_observation_complete,
            "automatic_team_materialization_gate": automatic_team_materialization_gate,
            "workspace_mutation_gate": workspace_mutation_gate,
            "workspace_reset_gate": workspace_reset_gate,
            "hard_budget_lease_gate": hard_budget_lease_gate,
            "baseline_topology_isolation_gate": baseline_topology_isolation_gate,
            "tool_topology_observation_gate": tool_topology_observation_gate,
            "minimum_complete_pairs_bp": 9_000,
            "minimum_paired_sign_confidence_bp": 9_000,
            "failure_timeout_samples_retained": true,
        }
    })
}

fn build_strategy_calibration_records(
    corpus: &AutoStrategyCorpus,
    task_comparisons: &[Value],
    samples: &[Sample],
    repetitions: usize,
    provenance: &Value,
) -> Vec<harness_contract::strategy::StrategyExperienceRecord> {
    use harness_contract::{
        core::ExecutionPattern,
        strategy::{
            PairedStrategyCalibrationEvidence, StrategyExperienceRecord, StrategyInput, understand,
        },
    };

    corpus
        .tasks
        .iter()
        .filter(|task| task.expected_candidate == "team")
        .flat_map(|task| {
            let comparison = task_comparisons
                .iter()
                .find(|comparison| comparison["task_id"].as_str() == Some(task.task_id.as_str()));
            let Some(baseline_condition) = comparison
                .and_then(|value| value["strongest_non_team_baseline"].as_str())
                .and_then(|value| match value {
                    "direct" => Some(Condition::Direct),
                    "parallel_tools" => Some(Condition::ParallelTools),
                    _ => None,
                })
            else {
                return Vec::new();
            };
            let understanding = understand(&StrategyInput::from_prompt(task.prompt.clone()));
            (0..repetitions)
                .filter_map(|repetition| {
                    let auto = samples.iter().find(|sample| {
                        !sample.warmup
                            && sample.task_id == task.task_id
                            && sample.repetition == repetition
                            && sample.condition == Condition::Auto
                            && sample.status == "completed"
                    })?;
                    let baseline = samples.iter().find(|sample| {
                        !sample.warmup
                            && sample.task_id == task.task_id
                            && sample.repetition == repetition
                            && sample.condition == baseline_condition
                            && sample.status == "completed"
                    })?;
                    let mut paired_calibration = PairedStrategyCalibrationEvidence {
                        evaluation_ref: format!(
                            "harness_eval.auto_strategy_paired.v1:{}:{}:{}",
                            provenance["corpus_id"].as_str()?,
                            task.task_id,
                            repetition,
                        ),
                        corpus_sha256: provenance["corpus_sha256"].as_str()?.to_string(),
                        workspace_revision: provenance["workspace_revision"].as_str()?.to_string(),
                        provider_account_ref: provenance["provider_account_ref"]
                            .as_str()?
                            .to_string(),
                        baseline_pattern: if baseline_condition == Condition::Direct {
                            ExecutionPattern::Direct
                        } else {
                            ExecutionPattern::Explore
                        },
                        baseline_duration_ms: baseline.critical_path_ms,
                        baseline_quality_score_bp: baseline.quality_bp,
                        candidate_duration_ms: auto.critical_path_ms,
                        candidate_quality_score_bp: auto.quality_bp,
                        blind_judge_completed: true,
                        baseline_total_tokens: baseline
                            .input_tokens
                            .saturating_add(baseline.output_tokens)
                            .saturating_add(baseline.cached_tokens),
                        candidate_total_tokens: auto
                            .input_tokens
                            .saturating_add(auto.output_tokens)
                            .saturating_add(auto.cached_tokens),
                        candidate_duplicate_tool_ratio_bp: if auto.tool_calls == 0 {
                            0
                        } else {
                            u16::try_from(
                                auto.duplicate_tool_calls.saturating_mul(10_000) / auto.tool_calls,
                            )
                            .unwrap_or(10_000)
                        },
                        admission_channel: None,
                        report_sha256: String::new(),
                        rubric_sha256: String::new(),
                        binary_sha256: String::new(),
                        frontend_workspace_revision: String::new(),
                        model_revision: String::new(),
                        judge_model_revision: String::new(),
                        invariant_fingerprint: String::new(),
                    };
                    paired_calibration.admission_channel =
                        paired_calibration.registered_admission_channel();
                    let multi_agent_positive_lift = paired_calibration.demonstrates_positive_lift();
                    Some(StrategyExperienceRecord {
                        domain: understanding.domain,
                        complexity: understanding.complexity,
                        risk: understanding.risk,
                        selected_pattern: ExecutionPattern::Collaborate,
                        selected_candidate: Some(
                            harness_contract::strategy::ExecutionCandidateKind::Team,
                        ),
                        succeeded: true,
                        verification_blocked: false,
                        context_pressure: false,
                        composite_execution: false,
                        multi_agent_positive_lift,
                        created_at_ms: 0,
                        actual_duration_ms: paired_calibration.candidate_duration_ms,
                        actual_input_tokens: auto.input_tokens,
                        actual_output_tokens: auto.output_tokens,
                        actual_cached_tokens: auto.cached_tokens,
                        actual_coordination_cost_ms: auto.merge_cost_ms,
                        paired_calibration: Some(paired_calibration),
                    })
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

#[derive(Debug, Clone, Serialize)]
struct ConditionSummary {
    condition: Condition,
    sample_count: usize,
    wall_median_ms: u64,
    wall_p95_ms: u64,
    critical_path_median_ms: u64,
    critical_path_p95_ms: u64,
    ttft_median_ms: u64,
    ttft_p95_ms: u64,
    quality_bp: u16,
    input_tokens: u64,
    output_tokens: u64,
    cached_tokens: u64,
    tool_calls: u64,
    duplicate_tool_ratio_bp: u16,
    evidence_overlap_bp: u16,
    merge_cost_ms: u64,
}

fn select_preregistered_baseline<'a>(
    direct: &'a ConditionSummary,
    parallel: &'a ConditionSummary,
) -> &'a ConditionSummary {
    const QUALITY_FLOOR_BP: u16 = 8_000;
    match (
        direct.quality_bp >= QUALITY_FLOOR_BP,
        parallel.quality_bp >= QUALITY_FLOOR_BP,
    ) {
        (true, true) => {
            if direct.wall_median_ms <= parallel.wall_median_ms {
                direct
            } else {
                parallel
            }
        }
        (true, false) => direct,
        (false, true) => parallel,
        (false, false) => {
            if direct.quality_bp >= parallel.quality_bp {
                direct
            } else {
                parallel
            }
        }
    }
}

fn summarize(samples: &[&Sample], condition: Condition) -> ConditionSummary {
    let selected = samples
        .iter()
        .copied()
        .filter(|sample| sample.condition == condition)
        .collect::<Vec<_>>();
    let walls = selected
        .iter()
        .map(|sample| sample.wall_ms)
        .collect::<Vec<_>>();
    let ttfts = selected
        .iter()
        .map(|sample| sample.ttft_ms)
        .collect::<Vec<_>>();
    let critical_paths = selected
        .iter()
        .map(|sample| sample.critical_path_ms)
        .collect::<Vec<_>>();
    let quality = selected
        .iter()
        .map(|sample| u64::from(sample.quality_bp))
        .sum::<u64>();
    let tool_calls = selected.iter().map(|sample| sample.tool_calls).sum::<u64>();
    let duplicates = selected
        .iter()
        .map(|sample| sample.duplicate_tool_calls)
        .sum::<u64>();
    ConditionSummary {
        condition,
        sample_count: selected.len(),
        wall_median_ms: percentile(walls.clone(), 50),
        wall_p95_ms: percentile(walls, 95),
        critical_path_median_ms: percentile(critical_paths.clone(), 50),
        critical_path_p95_ms: percentile(critical_paths, 95),
        ttft_median_ms: percentile(ttfts.clone(), 50),
        ttft_p95_ms: percentile(ttfts, 95),
        quality_bp: if selected.is_empty() {
            0
        } else {
            u16::try_from(quality / selected.len() as u64).unwrap_or(10_000)
        },
        input_tokens: selected.iter().map(|sample| sample.input_tokens).sum(),
        output_tokens: selected.iter().map(|sample| sample.output_tokens).sum(),
        cached_tokens: selected.iter().map(|sample| sample.cached_tokens).sum(),
        tool_calls,
        duplicate_tool_ratio_bp: if tool_calls == 0 {
            0
        } else {
            u16::try_from(duplicates.saturating_mul(10_000) / tool_calls).unwrap_or(10_000)
        },
        evidence_overlap_bp: selected
            .iter()
            .map(|sample| sample.evidence_overlap_bp)
            .max()
            .unwrap_or(0),
        merge_cost_ms: selected.iter().map(|sample| sample.merge_cost_ms).sum(),
    }
}

fn percentile(mut values: Vec<u64>, percentile: usize) -> u64 {
    if values.is_empty() {
        return 0;
    }
    values.sort_unstable();
    let index = values
        .len()
        .saturating_mul(percentile)
        .saturating_add(99)
        .saturating_div(100)
        .saturating_sub(1)
        .min(values.len() - 1);
    values[index]
}

fn cluster_bootstrap_ci(deltas: &[i64], mut state: u64) -> Value {
    if deltas.is_empty() {
        return json!({"lower": null, "upper": null, "samples": 0});
    }
    let mut means = Vec::with_capacity(2_000);
    for _ in 0..2_000 {
        let mut total = 0_i128;
        for _ in 0..deltas.len() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            total += i128::from(deltas[(state as usize) % deltas.len()]);
        }
        means.push((total / deltas.len() as i128) as i64);
    }
    means.sort_unstable();
    json!({
        "lower": means[means.len() * 25 / 1000],
        "upper": means[means.len() * 975 / 1000],
        "samples": 2_000,
    })
}

fn paired_sign_confidence_bp(deltas: &[i64]) -> u16 {
    let wins = deltas.iter().filter(|delta| **delta < 0).count();
    let losses = deltas.iter().filter(|delta| **delta > 0).count();
    let n = wins.saturating_add(losses);
    if n == 0 {
        return 0;
    }
    let tail = (wins..=n).map(|k| binomial_coefficient(n, k)).sum::<u128>();
    let denominator = 1_u128.checked_shl(n as u32).unwrap_or(u128::MAX);
    let confidence = denominator
        .saturating_sub(tail)
        .saturating_mul(10_000)
        .saturating_div(denominator);
    u16::try_from(confidence).unwrap_or(10_000)
}

fn binomial_coefficient(n: usize, k: usize) -> u128 {
    let k = k.min(n.saturating_sub(k));
    (0..k).fold(1_u128, |value, index| {
        value
            .saturating_mul((n - index) as u128)
            .saturating_div((index + 1) as u128)
    })
}

fn start_ttft_observer(
    client: &Client,
    base: &str,
    session_id: &str,
) -> Result<mpsc::Receiver<Instant>, String> {
    let client = client.clone();
    let url = format!("{base}/api/sessions/{session_id}/stream");
    let (ready_tx, ready_rx) = mpsc::sync_channel::<Result<(), String>>(1);
    let (first_tx, first_rx) = mpsc::sync_channel::<Instant>(1);
    thread::spawn(move || {
        let response = match client.get(url).send() {
            Ok(response) if response.status().is_success() => response,
            Ok(response) => {
                let _ = ready_tx.send(Err(format!("SSE returned HTTP {}", response.status())));
                return;
            }
            Err(error) => {
                let _ = ready_tx.send(Err(error.to_string()));
                return;
            }
        };
        let mut ready = false;
        let mut first_sent = false;
        for line in BufReader::new(response).lines() {
            let Ok(line) = line else {
                break;
            };
            let Some(data) = line.strip_prefix("data:") else {
                continue;
            };
            let Ok(event) = serde_json::from_str::<Value>(data.trim()) else {
                continue;
            };
            match event.get("type").and_then(Value::as_str) {
                Some("Connected") => {
                    if !ready {
                        ready = true;
                        let _ = ready_tx.send(Ok(()));
                    }
                }
                Some("TextDelta")
                    if !first_sent
                        && event
                            .get("content")
                            .or_else(|| event.get("text"))
                            .and_then(Value::as_str)
                            .is_some_and(|text| !text.is_empty()) =>
                {
                    first_sent = true;
                    let _ = first_tx.send(Instant::now());
                }
                Some("TerminalCommitted") => break,
                _ => {}
            }
        }
        if !ready {
            let _ = ready_tx.send(Err("SSE closed before the Connected event".to_string()));
        }
    });
    ready_rx
        .recv_timeout(Duration::from_secs(5))
        .map_err(|_| "SSE Connected event timed out".to_string())??;
    Ok(first_rx)
}

fn apply_observed_cost(sample: &mut Sample, requested_model: &str) {
    if !sample.usage_observed || !exact_model_revisions(&sample.models_used, requested_model) {
        return;
    }
    let Some(pricing) = model_protocol::model_registry::pricing_for_model(requested_model) else {
        return;
    };
    // Cache counters are combined in the execution projection. Charge every
    // cached token at the more expensive cache rate, making the hard budget a
    // conservative upper bound rather than an optimistic estimate.
    let cached_rate = pricing
        .cache_creation_cost_per_million
        .max(pricing.cache_read_cost_per_million);
    let cost_usd_milli = ((sample.input_tokens as f64 * pricing.input_cost_per_million
        + sample.output_tokens as f64 * pricing.output_cost_per_million
        + sample.cached_tokens as f64 * cached_rate)
        / 1_000.0)
        .ceil();
    if !cost_usd_milli.is_finite() || cost_usd_milli < 0.0 {
        return;
    }
    sample.cost_usd_milli = cost_usd_milli as u64;
    sample.cost_observed = true;
    sample.cost_source = Some(
        "model-registry pricing over observed tokens; cached tokens upper-bounded".to_string(),
    );
}

fn exact_model_revisions(observed: &[String], requested: &str) -> bool {
    !observed.is_empty() && observed.iter().all(|model| model == requested)
}

fn post_json(client: &Client, base: &str, path: &str, body: Value) -> Result<Value, String> {
    response_json(
        client
            .post(format!("{base}{path}"))
            .json(&body)
            .send()
            .map_err(|error| error.to_string())?,
    )
}

fn get_json(client: &Client, base: &str, path: &str) -> Result<Value, String> {
    response_json(
        client
            .get(format!("{base}{path}"))
            .send()
            .map_err(|error| error.to_string())?,
    )
}

fn response_json(response: reqwest::blocking::Response) -> Result<Value, String> {
    let status = response.status();
    let text = response.text().map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("HTTP {status}: {text}"));
    }
    serde_json::from_str(&text).map_err(|error| format!("{error}: {text}"))
}

fn extract_string(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
        .map(str::to_string)
}

fn latest_assistant_text(value: &Value) -> Option<String> {
    let messages = value
        .as_array()
        .or_else(|| value.get("messages").and_then(Value::as_array))?;
    messages.iter().rev().find_map(|message| {
        let role = message.get("role").and_then(Value::as_str)?;
        if role != "assistant" {
            return None;
        }
        let legacy = message
            .get("content")
            .or_else(|| message.get("text"))
            .and_then(Value::as_str)
            .filter(|text| !text.trim().is_empty())
            .map(str::to_string);
        legacy.or_else(|| {
            let text = message
                .get("blocks")
                .and_then(Value::as_array)?
                .iter()
                .filter(|block| {
                    block
                        .get("type")
                        .and_then(Value::as_str)
                        .is_none_or(|kind| kind == "text")
                })
                .filter_map(|block| block.get("text").and_then(Value::as_str))
                .filter(|text| !text.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n");
            (!text.is_empty()).then_some(text)
        })
    })
}

fn value_u64(value: &Value, paths: &[&str]) -> u64 {
    paths
        .iter()
        .find_map(|path| {
            value
                .pointer(&format!("/{}", path.replace('.', "/")))
                .and_then(Value::as_u64)
        })
        .unwrap_or(0)
}

fn extract_json_object(text: &str) -> Option<Value> {
    serde_json::from_str(text).ok().or_else(|| {
        let start = text.find('{')?;
        let end = text.rfind('}')?;
        serde_json::from_str(&text[start..=end]).ok()
    })
}

fn stable_u64(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_be_bytes(digest[..8].try_into().ok().unwrap_or([0; 8]))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latin_schedule_is_stable_and_balanced() {
        assert_eq!(
            condition_order(0, 0),
            [Condition::Direct, Condition::ParallelTools, Condition::Auto]
        );
        assert_eq!(
            condition_order(0, 1),
            [Condition::ParallelTools, Condition::Auto, Condition::Direct]
        );
        assert_eq!(
            condition_order(0, 2),
            [Condition::Auto, Condition::Direct, Condition::ParallelTools]
        );
    }

    #[test]
    fn hard_budgets_can_only_be_lowered() {
        assert!(DEFAULT_MAX_TOKENS == 20_000_000);
        assert!(DEFAULT_MAX_COST_USD_MILLI == 50_000);
        assert_eq!(
            provider_admission_token_limit(
                "claude-opus-4-6",
                BUSINESS_SAMPLE_TOKEN_LEASE.saturating_sub(1),
                DEFAULT_MAX_COST_USD_MILLI,
                BUSINESS_SAMPLE_TOKEN_LEASE,
            ),
            None,
            "a tiny remaining token budget must produce zero provider admission"
        );
        assert_eq!(
            provider_admission_token_limit(
                "claude-opus-4-6",
                DEFAULT_MAX_TOKENS,
                0,
                BUSINESS_SAMPLE_TOKEN_LEASE,
            ),
            None,
            "a zero remaining cost budget must produce zero provider admission"
        );
    }

    #[test]
    fn condition_concurrency_defaults_to_two_and_stays_bounded() {
        assert_eq!(parse_condition_concurrency(None), Ok(2));
        assert_eq!(parse_condition_concurrency(Some("1")), Ok(1));
        assert_eq!(parse_condition_concurrency(Some("3")), Ok(3));
        assert!(parse_condition_concurrency(Some("0")).is_err());
        assert!(parse_condition_concurrency(Some("4")).is_err());
        assert!(parse_condition_concurrency(Some("many")).is_err());
    }

    #[test]
    fn latest_assistant_text_reads_gateway_structured_blocks_and_legacy_text() {
        let structured = json!({
            "messages": [
                {"role": "user", "blocks": [{"type": "text", "text": "ignore"}]},
                {"role": "assistant", "blocks": [
                    {"type": "text", "text": "first"},
                    {"type": "tool_use", "name": "read_file"},
                    {"type": "text", "text": "second"}
                ]}
            ]
        });
        assert_eq!(
            latest_assistant_text(&structured).as_deref(),
            Some("first\nsecond")
        );

        let legacy = json!([
            {"role": "assistant", "content": "legacy"}
        ]);
        assert_eq!(latest_assistant_text(&legacy).as_deref(), Some("legacy"));
    }

    #[test]
    fn recovered_projection_poll_clears_only_its_transient_error() {
        let task = AutoStrategyTask {
            task_id: "poll-recovery".to_string(),
            expected_candidate: "direct".to_string(),
            prompt: "inspect".to_string(),
            acceptance: vec!["complete".to_string()],
            workspace_fixture: "fixture".to_string(),
            provider_constraint: "normal".to_string(),
            mutation_fixture: None,
            judge_only: false,
        };
        let mut sample = sample_shell(&task, 0, true, 0, Condition::Direct);
        sample.error = Some(
            "poll_full_projection:HTTP 404 Not Found: execution graph is not visible yet"
                .to_string(),
        );
        clear_recovered_poll_error(
            &mut sample,
            &["poll_full_projection:", "decode_full_projection:"],
        );
        assert!(sample.error.is_none());

        sample.error = Some("verify_mutation_fixture:target unchanged".to_string());
        clear_recovered_poll_error(
            &mut sample,
            &["poll_full_projection:", "decode_full_projection:"],
        );
        assert_eq!(
            sample.error.as_deref(),
            Some("verify_mutation_fixture:target unchanged")
        );
    }

    #[test]
    fn successful_terminal_waits_only_for_bounded_message_visibility_grace() {
        let first_terminal = Instant::now();
        assert!(should_wait_for_terminal_response(
            true,
            "",
            first_terminal,
            first_terminal + Duration::from_secs(1)
        ));
        assert!(!should_wait_for_terminal_response(
            true,
            "answer",
            first_terminal,
            first_terminal + Duration::from_secs(1)
        ));
        assert!(!should_wait_for_terminal_response(
            true,
            "",
            first_terminal,
            first_terminal + TERMINAL_MESSAGE_VISIBILITY_GRACE
        ));
        assert!(!should_wait_for_terminal_response(
            false,
            "",
            first_terminal,
            first_terminal
        ));
    }

    #[test]
    fn judge_output_requires_exact_labels_reasons_score_bounds_and_model_revision() {
        let labels = ["A", "B", "C"];
        let valid = json!({
            "scores": {"A": 8_000, "B": 7_000, "C": 9_000},
            "reasons": {"A": "grounded", "B": "partial", "C": "complete"},
            "judge_model_revision": "judge-v1"
        });
        assert!(valid_judge_output(&valid, &labels, "judge-v1"));

        let mut invalid = valid;
        invalid["scores"]["C"] = json!(10_001);
        assert!(!valid_judge_output(&invalid, &labels, "judge-v1"));
        invalid["scores"]["C"] = json!(9_000);
        assert!(!valid_judge_output(&invalid, &labels, "judge-v2"));
    }

    #[test]
    fn write_task_judge_contract_uses_the_exact_repetition_content() {
        let task = AutoStrategyTask {
            task_id: "write".to_string(),
            expected_candidate: "team".to_string(),
            prompt: "write {{EXPECTED_CONTENT}}".to_string(),
            acceptance: vec!["exact".to_string()],
            workspace_fixture: "fixture".to_string(),
            provider_constraint: "normal".to_string(),
            mutation_fixture: Some(WorkspaceMutationFixture {
                target_path: "target".to_string(),
                initial_content: "before".to_string(),
                expected_content_template: "after-{repetition}".to_string(),
                protected_path: "protected".to_string(),
                protected_content: "sentinel".to_string(),
            }),
            judge_only: false,
        };
        assert_eq!(rendered_task_prompt(&task, 2), "write after-2");
        assert_eq!(
            evaluation_resource_scopes(&task),
            ["read:target", "read:protected", "write:target"]
        );
    }

    #[test]
    fn mixed_root_child_or_fallback_models_fail_exact_revision_provenance() {
        assert!(exact_model_revisions(&["model-v1".to_string()], "model-v1"));
        assert!(!exact_model_revisions(&[], "model-v1"));
        assert!(!exact_model_revisions(
            &["model-v1".to_string(), "fallback-v2".to_string()],
            "model-v1"
        ));
    }

    #[cfg(unix)]
    #[test]
    fn workspace_snapshot_detects_empty_directories_and_permission_only_changes() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().expect("snapshot workspace");
        let file = root.path().join("target.txt");
        fs::write(&file, "same bytes").expect("fixture");
        let before = snapshot_workspace_tree(root.path()).expect("before");

        fs::create_dir(root.path().join("empty")).expect("empty directory");
        let with_directory = snapshot_workspace_tree(root.path()).expect("directory snapshot");
        assert_ne!(before, with_directory);

        fs::remove_dir(root.path().join("empty")).expect("remove empty directory");
        fs::set_permissions(&file, fs::Permissions::from_mode(0o755)).expect("chmod");
        let with_mode = snapshot_workspace_tree(root.path()).expect("mode snapshot");
        assert_ne!(before, with_mode);
    }

    #[test]
    fn workspace_snapshot_excludes_only_root_runtime_private_state() {
        let root = tempfile::tempdir().expect("snapshot workspace");
        fs::write(root.path().join("target.txt"), "same bytes").expect("fixture");
        let before = snapshot_workspace_tree(root.path()).expect("before");

        fs::create_dir_all(root.path().join(".cowd/checkpoints/internal")).expect("Runtime state");
        fs::write(
            root.path().join(".cowd/checkpoints/internal/target.txt"),
            "private copy",
        )
        .expect("Runtime private file");
        let after_runtime_state = snapshot_workspace_tree(root.path()).expect("after Runtime state");
        assert_eq!(before, after_runtime_state);

        fs::create_dir_all(root.path().join("nested/.cowd")).expect("nested business directory");
        let after_nested_state = snapshot_workspace_tree(root.path()).expect("after nested state");
        assert_ne!(before, after_nested_state);
    }
}
