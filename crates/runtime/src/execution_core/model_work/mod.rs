mod coalescer;
mod compiler;
mod estimator;
mod graph;
mod metrics;
mod reducer;

pub use coalescer::{Coalesced, ImmutableWorkKey, InFlightCoalescer};
pub use compiler::{ModelWorkCompileError, ModelWorkGraphCompiler};
pub use estimator::{
    ModelWorkEstimate, ModelWorkEstimateInput, ModelWorkGraphEstimator, ModelWorkTopology,
};
pub use graph::{ModelWorkNode, ModelWorkPlan};
pub use metrics::model_work_metrics;
pub use reducer::{ModelWorkReducer, ModelWorkReductionInput, ReducedModelWork};

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use harness_contract::execution_graph::{
        ExecutionDependencyPolicy, ExecutionNodeKind, ExecutionWorkRole,
    };

    use super::*;

    fn analysis(id: &str) -> ModelWorkNode {
        let mut node = ModelWorkNode::new(
            id,
            ExecutionWorkRole::EvidenceAnalyze,
            ExecutionNodeKind::AgentTask,
            "agent_task",
            format!("payload:{id}"),
        );
        node.expected_duration_ms = 1_000;
        node.expected_input_tokens = 100;
        node.expected_output_tokens = 100;
        node
    }

    #[test]
    fn compiler_preserves_parallel_frontier_and_critical_path() {
        let mut merge = ModelWorkNode::new(
            "merge",
            ExecutionWorkRole::Synthesize,
            ExecutionNodeKind::Synthesize,
            "synthesize",
            "payload:merge",
        );
        merge.depends_on = vec!["left".to_string(), "right".to_string()];
        merge.expected_duration_ms = 500;
        let graph = ModelWorkGraphCompiler
            .compile(ModelWorkPlan {
                objective: "parallel research".to_string(),
                graph_id: Some("work:test".to_string()),
                nodes: vec![analysis("left"), analysis("right"), merge],
            })
            .expect("compile");
        let metrics = model_work_metrics(&graph).expect("work metrics");
        assert_eq!(metrics.width, 2);
        assert_eq!(metrics.depth, 2);
        assert_eq!(metrics.expected_serial_ms, 2_500);
        assert_eq!(metrics.expected_critical_path_ms, 1_500);
    }

    #[test]
    fn estimator_downgrades_only_with_sufficient_unhealthy_samples() {
        let graph = ModelWorkGraphCompiler
            .compile(ModelWorkPlan {
                objective: "parallel research".to_string(),
                graph_id: None,
                nodes: vec![analysis("left"), analysis("right")],
            })
            .expect("compile");
        let estimate = ModelWorkGraphEstimator.estimate(
            &graph,
            &ModelWorkEstimateInput {
                provider_effective_limit: 4,
                provider_available: 4,
                provider_failure_timeout_upper_bound_basis_points: 3_000,
                provider_samples: 5,
                maximum_token_amplification_basis_points: 100_000,
                ..ModelWorkEstimateInput::default()
            },
        );
        assert_eq!(estimate.topology, ModelWorkTopology::Downgraded);
        assert!(estimate.automatic);
    }

    #[test]
    fn estimator_respects_hard_capacity_without_waiting_for_samples() {
        let graph = ModelWorkGraphCompiler
            .compile(ModelWorkPlan {
                objective: "parallel research".to_string(),
                graph_id: None,
                nodes: vec![analysis("left"), analysis("right")],
            })
            .expect("compile");
        let estimate = ModelWorkGraphEstimator.estimate(
            &graph,
            &ModelWorkEstimateInput {
                provider_effective_limit: 1,
                provider_available: 1,
                provider_samples: 0,
                maximum_token_amplification_basis_points: 100_000,
                ..ModelWorkEstimateInput::default()
            },
        );
        assert_eq!(estimate.topology, ModelWorkTopology::Downgraded);
        assert!(!estimate.automatic);
    }

    #[test]
    fn estimator_accounts_for_multiple_waves_at_limited_provider_capacity() {
        let graph = ModelWorkGraphCompiler
            .compile(ModelWorkPlan {
                objective: "four independent lanes".to_string(),
                graph_id: None,
                nodes: vec![
                    analysis("one"),
                    analysis("two"),
                    analysis("three"),
                    analysis("four"),
                ],
            })
            .expect("compile");
        let estimate = ModelWorkGraphEstimator.estimate(
            &graph,
            &ModelWorkEstimateInput {
                provider_effective_limit: 2,
                provider_available: 2,
                agent_available: 4,
                maximum_token_amplification_basis_points: 100_000,
                ..ModelWorkEstimateInput::default()
            },
        );

        assert_eq!(estimate.width, 2);
        assert_eq!(estimate.expected_serial_ms, 4_000);
        assert_eq!(estimate.expected_parallel_ms, 2_000);
        assert_eq!(estimate.expected_speedup_basis_points, Some(20_000));
    }

    #[test]
    fn quorum_contract_compiles_optional_read_only_lanes() {
        let mut left = analysis("left");
        left.required = false;
        left.cancellation_group = Some("research".to_string());
        let mut right = analysis("right");
        right.required = false;
        right.cancellation_group = Some("research".to_string());
        let mut merge = analysis("merge");
        merge.depends_on = vec!["left".to_string(), "right".to_string()];
        merge.dependency = ExecutionDependencyPolicy::Quorum {
            minimum: 1,
            cancel_remaining: true,
        };
        ModelWorkGraphCompiler
            .compile(ModelWorkPlan {
                objective: "first verified answer".to_string(),
                graph_id: None,
                nodes: vec![left, right, merge],
            })
            .expect("quorum graph");
    }

    #[test]
    fn reducer_prioritizes_required_results_and_deduplicates_evidence() {
        let reduced = ModelWorkReducer::new(10).reduce(vec![
            ModelWorkReductionInput {
                summary: "optional".to_string(),
                required: false,
                evidence_refs: vec!["e1".to_string()],
            },
            ModelWorkReductionInput {
                summary: "required".to_string(),
                required: true,
                evidence_refs: vec!["e1".to_string(), "e2".to_string()],
            },
        ]);
        assert_eq!(reduced.summary, "required");
        assert_eq!(reduced.evidence_refs, vec!["e1", "e2"]);
        assert_eq!(reduced.omitted_items, 1);
    }

    #[test]
    fn reducer_bounds_oversized_required_text_without_losing_evidence() {
        let reduced = ModelWorkReducer::new(4).reduce(vec![ModelWorkReductionInput {
            summary: "必要结论不能静默丢失".to_string(),
            required: true,
            evidence_refs: vec!["durable:evidence".to_string()],
        }]);

        assert_eq!(reduced.summary, "必要结论");
        assert_eq!(reduced.summary.chars().count(), 4);
        assert_eq!(reduced.evidence_refs, vec!["durable:evidence"]);
        assert_eq!(reduced.omitted_items, 1);
    }

    #[tokio::test]
    async fn coalescer_executes_identical_inflight_work_once() {
        let coalescer = Arc::new(InFlightCoalescer::<String, usize, String>::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..4 {
            let coalescer = Arc::clone(&coalescer);
            let calls = Arc::clone(&calls);
            tasks.push(tokio::spawn(async move {
                coalescer
                    .run("same".to_string(), || async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        Ok(7)
                    })
                    .await
                    .expect("coalesced")
            }));
        }
        for task in tasks {
            assert_eq!(task.await.expect("join").value, 7);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancelled_coalescer_owner_does_not_poison_the_key() {
        let coalescer = Arc::new(InFlightCoalescer::<String, usize, String>::default());
        let started = Arc::new(tokio::sync::Notify::new());
        let task = {
            let coalescer = Arc::clone(&coalescer);
            let started = Arc::clone(&started);
            tokio::spawn(async move {
                coalescer
                    .run("recoverable".to_string(), || async move {
                        started.notify_one();
                        std::future::pending::<Result<usize, String>>().await
                    })
                    .await
            })
        };
        started.notified().await;
        task.abort();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            coalescer.run("recoverable".to_string(), || async { Ok(9) }),
        )
        .await
        .expect("replacement initializer does not hang")
        .expect("replacement succeeds");
        assert_eq!(result.value, 9);
    }
}
