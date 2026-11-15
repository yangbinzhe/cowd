use harness_contract::policy::{AuthorizationPath, PermissionMode};
use harness_contract::tool::ToolEffectDescriptor;
use runtime::authorization_negotiator::{AuthorizationNegotiator, AuthorizationRequest};
use runtime::permissions::{PermissionContext, PermissionPolicy};
use serde_json::{json, Value};

fn assess(
    effect: ToolEffectDescriptor,
    input: &Value,
) -> harness_contract::policy::CapabilityAssessment {
    let request = AuthorizationRequest {
        principal_id: "session:fix2".into(),
        capability: effect.tool_id.clone(),
        input: input.to_string(),
        idempotency_key: "fix2-call".into(),
        effect,
        parent_ceiling: PermissionMode::ReadOnly,
        parent_lease_id: Some("parent:read-only".into()),
        policy_revision: 1,
        recovery_scope: "turn:fix2".into(),
        context: PermissionContext::default(),
        safe_alternatives: vec![],
    };
    AuthorizationNegotiator::new()
        .assess(&PermissionPolicy::new(PermissionMode::ReadOnly), &request)
}

#[test]
fn fix2_runtime_authorization_accepts_eleven_shapes_and_denies_boundaries() {
    let host = tools::ToolHost::builtin("fix2-auth", &std::env::current_dir().unwrap());
    let lease = host.pin_snapshot();
    let read = |tool: &str| match tool {
        "glob_search" => json!({"tool":tool,"input":{"path":"src","pattern":"*.rs"}}),
        "grep_search" => json!({"tool":tool,"input":{"path":"src","pattern":"fn"}}),
        "workspace_snapshot" => json!({"tool":tool,"input":{"roots":["src"]}}),
        _ => json!({"tool":tool,"input":{"path":"src/a"}}),
    };
    let call = |id: &str, tool: &str| {
        let mut step = read(tool);
        step["id"] = json!(id);
        step["op"] = json!("call");
        step
    };
    let batch =
        |id: &str| json!({"id":id,"op":"batch","calls":[read("read_file"),read("grep_search")]});
    // Reproduce the step shapes in the adjudicated 6 write / 5 danger rows;
    // these are representative inputs, not a replay of the original payloads.
    let shapes = vec![
        (
            vec![
                call("s", "workspace_snapshot"),
                call("g", "glob_search"),
                call("g1", "grep_search"),
                call("g2", "grep_search"),
                call("g3", "grep_search"),
            ],
            json!([1, 2]),
        ),
        (vec![batch("r")], json!("$r")),
        (vec![batch("r"), batch("r2")], json!("$r2")),
        (vec![batch("r")], json!([1, 2])),
        (vec![batch("r")], json!({"data":"$r"})),
        (
            vec![
                call("g1", "glob_search"),
                call("g2", "glob_search"),
                call("g3", "glob_search"),
            ],
            json!(1),
        ),
        (vec![batch("read1")], json!("$read1")),
        (vec![call("g", "glob_search")], json!({"x":1})),
        (
            vec![
                call("g1", "glob_search"),
                call("g2", "glob_search"),
                batch("reads"),
            ],
            json!("$reads"),
        ),
        (vec![batch("r")], json!([2, 3])),
        (
            vec![
                call("r1", "read_file"),
                call("g1", "grep_search"),
                call("r2", "read_file"),
                call("r3", "read_file"),
                call("g2", "grep_search"),
                call("g3", "grep_search"),
                call("r4", "read_file"),
                call("g4", "grep_search"),
            ],
            json!(0),
        ),
    ];
    assert_eq!(shapes.len(), 11);
    for (mut steps, data) in shapes {
        steps.push(json!({"id":"c","op":"compute","language":"python","code":"print(_input)","input":data}));
        let input = json!({"steps":steps});
        let result = assess(lease.describe_effect("tool_orchestrate", &input), &input);
        assert_eq!(
            result.path,
            AuthorizationPath::PolicyAutoGrant,
            "{input}: {result:?}"
        );
        assert_eq!(result.required_mode, PermissionMode::ReadOnly);
        assert!(result.lease.is_some());
    }
    for input in [
        json!({"steps":[{"id":"r","op":"call","tool":"read_file","input":{"path":".env"}},{"id":"c","op":"compute","language":"python","code":"print(_input)","input":"$r"}]}),
        json!({"steps":[{"id":"w","op":"call","tool":"edit","input":{"action":"write","path":"out","content":"x"}},{"id":"c","op":"compute","language":"python","code":"print(1)","input":1}]}),
        json!({"steps":[{"id":"r","op":"call","tool":"read_file","input":{"path":"$previous.path"}},{"id":"c","op":"compute","language":"python","code":"print(_input)","input":"$r"}]}),
    ] {
        let result = assess(lease.describe_effect("tool_orchestrate", &input), &input);
        assert_eq!(
            result.path,
            AuthorizationPath::HardDeny,
            "{input}: {result:?}"
        );
    }
    for input in [
        json!({"language":"python","code":"print(1)","input_files":["src/a"]}),
        json!({"language":"python","code":"print(1)","input_files":[],"workspaceAccess":"write"}),
        json!({"language":"python","code":"print(1)","input_files":[],"isolateNetwork":false}),
        json!({"language":"python","code":"print(1)","input_files":[],"dangerouslyDisableSandbox":true}),
    ] {
        let result = assess(lease.describe_effect("execute_code", &input), &input);
        assert_eq!(
            result.path,
            AuthorizationPath::HardDeny,
            "{input}: {result:?}"
        );
    }
}
