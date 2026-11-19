use super::*;
mod own832_diagnostics;
use harness_contract::agent_action::{AgentActorBinding, AgentActorKind, TaskClaimInput};

#[cfg(test)]
pub(crate) mod diagnostic_1120 {
    use serde_json::{json, Value};
    use std::{
        cell::RefCell,
        io::Write,
        path::PathBuf,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
    };
    static SEQUENCE: AtomicU64 = AtomicU64::new(1);
    static FAILED: AtomicBool = AtomicBool::new(false);
    thread_local! {
        static CONTEXT: RefCell<Vec<Value>> = const { RefCell::new(Vec::new()) };
        static DIRECTORY: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }
    pub(crate) fn record(kind: &str, payload: Value) {
        let directory = DIRECTORY
            .with(|slot| slot.borrow().clone())
            .or_else(|| std::env::var_os("COWD_DIAG_1120_DIR").map(PathBuf::from));
        let Some(directory) = directory else {
            return;
        };
        let result = (|| -> std::io::Result<()> {
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if directory.symlink_metadata()?.file_type().is_symlink()
                    || directory.metadata()?.permissions().mode() & 0o777 != 0o700
                {
                    return Err(std::io::Error::other("observer directory is not private"));
                }
            }
            let sequence = SEQUENCE.fetch_add(1, Ordering::SeqCst);
            let context = CONTEXT
                .try_with(|slot| slot.borrow().last().cloned())
                .ok()
                .flatten();
            let mut event = json!({"schema":1,"pid":std::process::id(),"sequence":sequence,
                "kind":kind,"context":context,"payload":payload});
            let mut bytes = serde_json::to_vec(&event)?;
            if bytes.len() > 65536 {
                event["payload"] = json!({"oversize":true,"bytes":bytes.len(),
                    "digest":crate::agentic::review_evidence::proof_value_digest(&event["payload"])});
                bytes = serde_json::to_vec(&event)?;
            }
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(directory.join(format!(
                "observer-{}-{sequence:08}.json",
                std::process::id()
            )))?;
            file.write_all(&bytes)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            std::fs::File::open(directory)?.sync_all()?;
            Ok(())
        })();
        if result.is_err() {
            FAILED.store(true, Ordering::SeqCst);
            eprintln!("OBS1120_WRITE_FAILURE kind={kind}");
        }
    }
    pub(crate) struct Frame;
    impl Frame {
        pub(crate) fn enter(mut context: Value) -> Self {
            context["call_id"] = json!(SEQUENCE.fetch_add(1, Ordering::SeqCst));
            CONTEXT.with(|slot| slot.borrow_mut().push(context));
            record("validator.enter", json!({}));
            Self
        }
    }
    impl Drop for Frame {
        fn drop(&mut self) {
            record(
                "validator.exit",
                json!({"unwinding":std::thread::panicking()}),
            );
            let _ = CONTEXT.try_with(|slot| slot.borrow_mut().pop());
        }
    }
    pub(crate) struct Boundary {
        stage: &'static str,
        identity: Value,
        ended: bool,
    }
    impl Boundary {
        pub(crate) fn enter(stage: &'static str, identity: Value) -> Self {
            record("cleanup.begin", json!({"stage":stage,"identity":identity}));
            Self {
                stage,
                identity,
                ended: false,
            }
        }
        pub(crate) fn end(mut self) {
            self.ended = true;
            record(
                "cleanup.end",
                json!({"stage":self.stage,"identity":self.identity}),
            );
        }
    }
    impl Drop for Boundary {
        fn drop(&mut self) {
            if !self.ended {
                record(
                    "cleanup.dropped",
                    json!({"stage":self.stage,"identity":self.identity,
                    "unwinding":std::thread::panicking()}),
                );
            }
        }
    }
    // Explicit diagnostic admission gate: never a default-suite success with
    // missing external evidence. The owning runner executes this exact test
    // with --ignored and verifies the files after this process has exited.
    #[test]
    #[ignore = "requires the diagnostic owner's private COWD_DIAG_1120_DIR"]
    fn external_records_survive_original_scope_unwind() {
        let directory = std::env::var_os("COWD_DIAG_1120_DIR")
            .map(PathBuf::from)
            .expect("diagnostic admission supplies a private collector");
        DIRECTORY.with(|slot| *slot.borrow_mut() = Some(directory.clone()));
        let caught = std::panic::catch_unwind(|| {
            let _frame = Frame::enter(json!({"callsite":"observer_contract","identity":"Applied",
                "action_id":"independent-literal-action"}));
            let _boundary = Boundary::enter("contract_unwind", json!({"run":"original-run"}));
            record(
                "contract.before_panic",
                json!({"expected":"durable before unwind"}),
            );
            panic!("1120 controlled observer unwind only");
        });
        DIRECTORY.with(|slot| *slot.borrow_mut() = None);
        Boundary::enter("contract_return", json!({"run":"original-return"})).end();
        assert!(caught.is_err());
        assert!(!FAILED.load(Ordering::SeqCst));
        let events = std::fs::read_dir(directory)
            .unwrap()
            .map(|p| {
                serde_json::from_slice::<Value>(&std::fs::read(p.unwrap().path()).unwrap()).unwrap()
            })
            .collect::<Vec<_>>();
        assert!(events.iter().any(|e| e["kind"] == "contract.before_panic"));
        assert!(events.iter().any(|e| e["kind"] == "cleanup.dropped"
            && e["payload"]["unwinding"] == true
            && e["context"]["action_id"] == "independent-literal-action"));
        assert!(events
            .iter()
            .any(|e| e["kind"] == "validator.exit" && e["payload"]["unwinding"] == true));
    }

}

pub(super) fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            original_execution: None,
            objective_id: "objective-dispatch".to_string(),
            program_id: "program-dispatch".to_string(),
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            root_execution_id: None,
            // No user topology constraint in this fixture; the ceiling must
            // therefore not restrict the teams these tests build.
            required_team_count: 0,
            exact_team_count: None,
            objective_summary: "dispatch objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: "root:session-dispatch".to_string(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        },
        expected_revision: None,
        action,
    }
}

/// Root envelope carrying the ingress execution chosen by the D-K fixture.
pub(super) fn root_in_dispatch(
    execution_id: &str,
    action_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.root_execution_id = Some(execution_id.to_string());
    envelope
}

/// An Agent actor that holds a registered attempt lease.
///
/// The product accepts two proofs for an Agent actor: an original execution, or
/// an active attempt lease on an AgentTask node whose packet carries the
/// matching program/agent/team/membership. These fixtures have no original
/// execution, so they register the lease graph instead.
pub(super) fn agent_with_lease(
    services: &RuntimeServices,
    action_id: &str,
    team_id: &str,
    agent_id: &str,
    execution_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    let mut envelope = agent(action_id, team_id, agent_id, execution_id, action);
    let projection = services
        .agent_action_service()
        .project(&envelope.actor.program_id)
        .expect("program projection");
    envelope.actor.objective_id = projection.objective_id.clone();
    envelope.actor.program_id = projection.program_id.clone();
    envelope.actor.session_id = projection.session_id.clone();
    envelope.actor.turn_id = projection.turn_id.clone();
    envelope.actor.root_execution_id = projection.root_execution_id.clone();
    envelope.actor.required_team_count = projection.required_team_count;
    envelope.actor.exact_team_count = projection.exact_team_count;
    envelope.actor.objective_summary = projection.objective_summary.clone();
    envelope.actor.model_lease = projection.model_lease.clone();
    envelope.actor.permission_ceiling = Some(projection.permission_ceiling);
    envelope.actor.resource_scopes = projection.resource_scopes.clone();
    register_agent_lease_graph(services, &envelope);
    envelope
}

/// Register the attempt lease an Agent actor acts under.
pub(super) fn register_agent_lease_graph(
    services: &RuntimeServices,
    envelope: &AgentActionEnvelope,
) {
    let projection = services
        .agent_action_service()
        .project(&envelope.actor.program_id)
        .expect("program projection");
    let graph_id = envelope
        .actor
        .execution_id
        .clone()
        .expect("agent execution id");
    crate::test_support::register_agent_lease_graph(
        services.event_store(),
        &projection,
        &envelope.actor,
        &graph_id,
        &envelope.actor.objective_id,
    );
}

pub(super) fn agent(
    action_id: &str,
    team_id: &str,
    agent_id: &str,
    execution_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.actor_id = agent_id.to_string();
    envelope.actor.kind = AgentActorKind::Agent;
    envelope.actor.execution_id = Some(execution_id.to_string());
    envelope.actor.team_id = Some(team_id.to_string());
    envelope.actor.agent_id = Some(agent_id.to_string());
    envelope
}


fn original_packet_followup_context(
    packet: &AgentTaskPacket,
    parent: &ExecutionParentBinding,
    actor: &AgentActorBinding,
) -> Result<AgenticDispatchContext, String> {
    packet.assignment.validate().map_err(|error| error.to_string())?;
    let turn_id = packet.assignment.execution_identity.turn_id()
        .filter(|turn| !turn.is_empty())
        .ok_or_else(|| "original followup packet has no turn identity".to_string())?;
    let binding = packet.agentic_binding.as_ref()
        .ok_or_else(|| "original followup packet has no Program binding".to_string())?;
    if packet.graph_id() != parent.execution_id || packet.node_id() != parent.node_id
        || actor.kind != AgentActorKind::Agent
        || actor.execution_id.as_deref() != Some(packet.graph_id())
        || actor.session_id != packet.session_id() || actor.turn_id != turn_id
        || actor.program_id != binding.program_id
        || actor.actor_id != binding.agent_id
        || actor.agent_id.as_deref() != Some(binding.agent_id.as_str())
        || actor.team_id.as_deref() != Some(binding.team_id.as_str())
    {
        return Err("original followup packet/parent/actor identity mismatch".into());
    }
    Ok(AgenticDispatchContext {
        session_id: packet.session_id().into(),
        turn_id: turn_id.into(),
        model_lease: packet.model_lease.clone(),
        permission_ceiling: packet.permission_ceiling,
        resource_scopes: packet.resource_scopes.clone(),
    })
}
