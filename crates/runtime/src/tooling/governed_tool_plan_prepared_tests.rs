use super::*;
use serde_json::json;

fn prepared_call(
    lease: &tools::ToolHostLease,
    id: &str,
    dependencies: &[&str],
) -> (ToolRequest, GovernedToolInvocation) {
    let dependencies = dependencies
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    let request = ToolRequest {
        tool_use_id: id.into(),
        tool_name: "read_file".into(),
        input: r#"{"path":"report.txt"}"#.into(),
        depends_on: dependencies.clone(),
    };
    let mut invocation = lease.prepare_governed_invocation(
        id,
        "read_file",
        &json!({"path":"report.txt"}),
        &dependencies,
    );
    invocation.origin = Some(
        serde_json::from_value(json!({
            "origin_step_id":"original-step", "call_id":id, "catalog_digest":"original-catalog",
            "source":{"kind":"provider","attempt_id":"original-provider-attempt"}
        }))
        .unwrap(),
    );
    (request, invocation)
}

#[test]
fn identical_inputs_keep_distinct_original_preparations_by_call_identity() {
    let root = tempfile::tempdir().unwrap();
    let host = tools::ToolHost::builtin("prepared-contract", root.path());
    let lease = host.pin_snapshot();
    let (a, mut first) = prepared_call(&lease, "first", &[]);
    let (b, mut second) = prepared_call(&lease, "second", &[]);
    first.idempotency_key = "original-first-key".into();
    second.idempotency_key = "original-second-key".into();
    let plan = GovernedToolCompiler
        .compile_prepared_partial(root.path(), &[a, b], &[second, first], &BTreeMap::new())
        .unwrap()
        .plan
        .unwrap();
    assert_eq!(plan.tasks.iter().map(|task| task.tool_call_id.as_str()).collect::<Vec<_>>(), ["first", "second"]);
    for (index, id, key) in [
        (0, "first", "original-first-key"),
        (1, "second", "original-second-key"),
    ] {
        let task = &plan.tasks[index];
        assert_eq!(task.original_call_index, index);
        assert_eq!(task.invocation.invocation_id, id);
        assert_eq!(task.invocation.intent.invocation_id, id);
        assert_eq!(
            task.invocation.intent.normalized_input,
            json!({"path":"report.txt"})
        );
        assert_eq!(task.invocation.origin.as_ref().unwrap().call_id, id);
        assert_eq!(
            task.invocation.origin.as_ref().unwrap().origin_step_id,
            "original-step"
        );
        assert_eq!(
            task.invocation.origin.as_ref().unwrap().catalog_digest,
            "original-catalog"
        );
        assert_eq!(task.idempotency_key, key);
        assert_eq!(task.invocation.idempotency_key, key);
        assert_eq!(
            task.invocation.contract_version, 1,
            "retain the producer contract version"
        );
    }
}

#[test]
fn preparation_rejection_keeps_independent_branch_and_original_result_index() {
    let root = tempfile::tempdir().unwrap();
    let host = tools::ToolHost::builtin("prepared-contract", root.path());
    let lease = host.pin_snapshot();
    let (bad, _) = prepared_call(&lease, "bad", &[]);
    let (child, child_invocation) = prepared_call(&lease, "child", &["bad"]);
    let (good, good_invocation) = prepared_call(&lease, "good", &[]);
    let result = GovernedToolCompiler
        .compile_prepared_partial(
            root.path(),
            &[bad, child, good],
            &[child_invocation, good_invocation],
            &BTreeMap::from([("bad".into(), "schema refused by original producer".into())]),
        )
        .unwrap();
    assert_eq!(
        result
            .rejected
            .iter()
            .map(|rejection| rejection.tool_call_id.as_str())
            .collect::<Vec<_>>(),
        vec!["bad", "child"]
    );
    let plan = result.plan.unwrap();
    assert!(matches!(plan.tasks.as_slice(), [task] if task.tool_call_id == "good"));
    assert_eq!(plan.tasks[0].original_call_index, 2);
    assert_eq!(
        plan.tasks[0].invocation.origin.as_ref().unwrap().call_id,
        "good"
    );
}

#[test]
fn foreign_duplicate_or_mutated_preparation_cannot_replace_another_call() {
    let root = tempfile::tempdir().unwrap();
    let host = tools::ToolHost::builtin("prepared-contract", root.path());
    let lease = host.pin_snapshot();
    let (request, invocation) = prepared_call(&lease, "known", &[]);
    let (_, foreign) = prepared_call(&lease, "foreign", &[]);
    assert!(GovernedToolCompiler
        .compile_prepared_partial(
            root.path(),
            std::slice::from_ref(&request),
            &[foreign],
            &BTreeMap::new()
        )
        .is_err());
    assert!(GovernedToolCompiler
        .compile_prepared_partial(
            root.path(),
            std::slice::from_ref(&request),
            &[invocation.clone(), invocation.clone()],
            &BTreeMap::new()
        )
        .is_err());
    let mut changed = invocation;
    changed.intent.invocation_id = "another".into();
    let result = GovernedToolCompiler
        .compile_prepared_partial(root.path(), &[request], &[changed], &BTreeMap::new())
        .unwrap();
    assert!(result.plan.is_none());
    assert!(matches!(result.rejected.as_slice(), [rejection] if rejection.tool_call_id == "known"));
}

#[test]
fn prepared_cycles_do_not_disable_an_unrelated_valid_branch() {
    let root = tempfile::tempdir().unwrap();
    let host = tools::ToolHost::builtin("prepared-contract", root.path());
    let lease = host.pin_snapshot();
    let (a, first) = prepared_call(&lease, "a", &["b"]);
    let (b, second) = prepared_call(&lease, "b", &["a"]);
    let (free, third) = prepared_call(&lease, "free", &[]);
    let result = GovernedToolCompiler
        .compile_prepared_partial(
            root.path(),
            &[a, b, free],
            &[first, second, third],
            &BTreeMap::new(),
        )
        .unwrap();
    let mut rejected = result.rejected.iter().map(|item| item.tool_call_id.as_str()).collect::<Vec<_>>();
    rejected.sort_unstable();
    assert_eq!(rejected, ["a", "b"]);
    let plan = result.plan.unwrap();
    assert!(matches!(plan.tasks.as_slice(), [task] if task.tool_call_id == "free"));
    assert_eq!(plan.tasks[0].original_call_index, 2);
}

#[test]
fn partial_fixture_adapter_resolves_each_input_once_including_rejected_nodes() {
    let root = tempfile::tempdir().unwrap();
    let calls = std::cell::RefCell::new(BTreeMap::<String, usize>::new());
    let requests = ["good", "bad", "independent"].map(|id| ToolRequest {
        tool_use_id: id.into(),
        tool_name: id.into(),
        input: "{}".into(),
        depends_on: vec![],
    });
    let result = GovernedToolCompiler
        .compile_partial(root.path(), &requests, |name, input| {
            *calls.borrow_mut().entry(name.into()).or_default() += 1;
            (name != "bad").then(|| (fixture_effect("read_file", input), 1, "fixture".into()))
        })
        .unwrap();
    assert_eq!(
        *calls.borrow(),
        BTreeMap::from([
            ("bad".into(), 1),
            ("good".into(), 1),
            ("independent".into(), 1)
        ])
    );
    assert!(matches!(result.rejected.as_slice(), [item] if item.tool_call_id == "bad"));
    let plan = result.plan.unwrap();
    let mut accepted = plan.tasks.iter().map(|task| task.tool_call_id.as_str()).collect::<Vec<_>>();
    accepted.sort_unstable();
    assert_eq!(accepted, ["good", "independent"]);
}
