use super::*;

/// `glob_search` effect descriptors carry only the request root, so the
/// pattern itself must also be checked against the delegated lexical lease.
/// Without this guard an out-of-scope pattern such as
/// `path:"."/pattern:"crates/gateway/**/*.rs"` looks like a workspace-root
/// read even when the Agent owns only `read:crates/runtime`.
pub(super) fn enforce_glob_scope(
    input: &serde_json::Value,
    allowed_scopes: &[String],
) -> Result<(), ToolError> {
    let object = input
        .as_object()
        .ok_or_else(|| ToolError::new("glob_search input must be a JSON object"))?;
    let path = object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(".")
        .trim()
        .replace('\\', "/");
    let pattern = object
        .get("pattern")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ToolError::new("glob_search input is missing pattern"))?
        .trim()
        .replace('\\', "/");
    let path_parts = normalized_relative_parts(&path)
        .ok_or_else(|| ToolError::new("glob_search path escapes its workspace"))?;
    let pattern_parts = normalized_relative_parts(&pattern)
        .ok_or_else(|| ToolError::new("glob_search pattern escapes its workspace"))?;
    let pattern_prefix = pattern_parts
        .iter()
        .take_while(|part| !part.contains(['*', '?', '[', '{']))
        .cloned()
        .collect::<Vec<_>>()
        .join("/");
    let requested_prefix = if path_parts.is_empty() {
        pattern_prefix
    } else if pattern_prefix.is_empty() {
        path_parts.join("/")
    } else {
        format!("{}/{}", path_parts.join("/"), pattern_prefix)
    };
    let permitted = allowed_scopes.iter().any(|scope| {
        let Some((mode, raw_scope)) = scope.split_once(':') else {
            return false;
        };
        if !matches!(mode, "read" | "write" | "workspace") {
            return false;
        }
        let scope_parts = match normalized_relative_parts(raw_scope) {
            Some(parts) => parts,
            None => return false,
        };
        let scope = scope_parts.join("/");
        scope_parts.is_empty()
            || requested_prefix == scope
            || requested_prefix.starts_with(&(scope + "/"))
    });
    permitted
        .then_some(())
        .ok_or_else(|| ToolError::new("glob_search pattern is outside the Team resource lease"))
}

pub(super) fn deterministic_scoped_tool_idempotency_key(
    execution_id: &str,
    node_id: &str,
    attempt: u32,
    sequence: u64,
    tool_name: &str,
    input: &str,
) -> String {
    let input_sha256 = format!("{:x}", Sha256::digest(input.as_bytes()));
    format!("agent-tool:{execution_id}:{node_id}:{attempt}:{sequence}:{tool_name}:{input_sha256}")
}

pub(super) fn normalize_delegated_resource_paths(
    tool_name: &str,
    input: &str,
    workspace_root: &std::path::Path,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    resource_scopes: Option<&[String]>,
) -> Result<String, ToolError> {
    let parsed = serde_json::from_str::<serde_json::Value>(input)
        .map_err(|error| ToolError::new(format!("invalid scoped tool input: {error}")))?;
    let parsed = normalize_delegated_resource_value(
        tool_name,
        parsed,
        workspace_root,
        path_identity_resolver,
        resource_scopes,
    );
    serde_json::to_string(&parsed)
        .map_err(|error| ToolError::new(format!("serialize normalized scoped tool input: {error}")))
}

pub(super) fn normalize_delegated_resource_value(
    tool_name: &str,
    parsed: serde_json::Value,
    workspace_root: &std::path::Path,
    path_identity_resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    resource_scopes: Option<&[String]>,
) -> serde_json::Value {
    let parsed = normalize_workspace_internal_resource_value(tool_name, parsed, workspace_root);
    let mut parsed = normalize_single_scope_relative_read_value(
        tool_name,
        parsed,
        path_identity_resolver,
        resource_scopes,
    );
    if tool_name != "glob_search" {
        return parsed;
    }
    let Some(scopes) = resource_scopes else {
        return parsed;
    };
    let Some(object) = parsed.as_object_mut() else {
        return parsed;
    };
    let Some(pattern) = object
        .get("pattern")
        .and_then(serde_json::Value::as_str)
        .map(|value| value.trim().replace('\\', "/"))
    else {
        return parsed;
    };
    let requested_path = object
        .get("path")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_string();
    // Empty glob roots are semantically the workspace root.  Normalize them
    // before scope rewriting so the ToolHost never receives an empty resource
    // key (which is rejected as malformed rather than treated as `.`).
    if requested_path.trim().is_empty() {
        object.insert(
            "path".to_string(),
            serde_json::Value::String(".".to_string()),
        );
    }
    let requested_root = workspace_root_request(&requested_path, workspace_root);
    if !requested_root {
        return parsed;
    }

    let mut allowed = scopes
        .iter()
        .filter_map(|scope| {
            let (mode, path) = scope.split_once(':')?;
            matches!(mode, "read" | "write" | "workspace").then_some(path)
        })
        .filter_map(|path| {
            let parts = normalized_relative_parts(path)?;
            Some(if parts.is_empty() {
                ".".to_string()
            } else {
                parts.join("/")
            })
        })
        .collect::<Vec<_>>();
    allowed.sort();
    allowed.dedup();
    let matched = allowed
        .iter()
        .filter(|scope| {
            scope.as_str() == "."
                || pattern == scope.as_str()
                || pattern
                    .strip_prefix(scope.as_str())
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
        .cloned()
        .collect::<Vec<_>>();
    let mut allowed = if matched.is_empty()
        && requested_path == "."
        && allowed.len() == 1
        && glob_pattern_has_no_explicit_root(&pattern)
    {
        // A delegated Agent commonly uses `path: "."` to mean "search my
        // assigned directory".  Rebind that unambiguous root to the sole
        // leased scope; never do this when multiple scopes exist.
        allowed
    } else if matched.is_empty()
        && allowed.len() == 1
        && glob_pattern_has_no_explicit_root(&pattern)
    {
        allowed
    } else {
        matched
    };
    allowed.sort_by_key(|scope| std::cmp::Reverse(scope.len()));
    let Some(scope) = allowed.first() else {
        return parsed;
    };
    if scope == "." {
        object.insert(
            "path".to_string(),
            serde_json::Value::String(".".to_string()),
        );
        return parsed;
    }
    let suffix = pattern
        .strip_prefix(scope)
        .unwrap_or_default()
        .trim_start_matches('/');
    let scoped_identity = path_identity_resolver.resolve_existing(scope).ok();
    // A Team may receive a write lease for a not-yet-created virtual delivery
    // directory (currently `team.dependencies`).  Preserve the lexical scope
    // in the native glob request; authorization below applies the same
    // workspace/repository fence without requiring the directory to exist
    // before the first Agent creates it.
    if scoped_identity.is_none() && is_virtual_directory_scope(scope) {
        object.insert("path".to_string(), serde_json::Value::String(scope.clone()));
        if !suffix.is_empty() {
            object.insert(
                "pattern".to_string(),
                serde_json::Value::String(suffix.to_string()),
            );
        }
        return parsed;
    }
    let Some(scoped_identity) = scoped_identity else {
        return parsed;
    };
    if scoped_identity.object_kind == harness_contract::context::WorkspaceObjectKind::File {
        let Some(file_name) = std::path::Path::new(&scoped_identity.workspace_relative_path)
            .file_name()
            .and_then(|name| name.to_str())
        else {
            return parsed;
        };
        let parent = std::path::Path::new(scope)
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map_or_else(
                || ".".to_string(),
                |parent| parent.to_string_lossy().into_owned(),
            );
        object.insert("path".to_string(), serde_json::Value::String(parent));
        object.insert(
            "pattern".to_string(),
            serde_json::Value::String(file_name.to_string()),
        );
        return parsed;
    }
    if scoped_identity.object_kind != harness_contract::context::WorkspaceObjectKind::Directory {
        return parsed;
    }
    object.insert("path".to_string(), serde_json::Value::String(scope.clone()));
    if !suffix.is_empty() {
        object.insert(
            "pattern".to_string(),
            serde_json::Value::String(suffix.to_string()),
        );
    }
    parsed
}

pub(super) fn glob_pattern_has_no_explicit_root(pattern: &str) -> bool {
    pattern
        .split('/')
        .next()
        .is_some_and(|segment| segment.contains(['*', '?', '[', '{']))
}

/// Virtual delivery namespaces are declared as directory scopes before any
/// artifact exists.  They are still lexical workspace-relative paths and are
/// never allowed to contain traversal or an absolute prefix.
pub(super) fn is_virtual_directory_scope(scope: &str) -> bool {
    let Some(parts) = normalized_relative_parts(scope) else {
        return false;
    };
    !parts.is_empty()
        && parts
            .first()
            .is_some_and(|part| part == "team.dependencies")
}

pub(super) fn workspace_root_request(path: &str, workspace_root: &std::path::Path) -> bool {
    let trimmed = path.trim();
    if trimmed.is_empty() || trimmed == "." || trimmed == "./" {
        return true;
    }
    let requested = std::path::Path::new(trimmed);
    requested.is_absolute()
        && requested
            .canonicalize()
            .ok()
            .zip(workspace_root.canonicalize().ok())
            .is_some_and(|(requested, root)| requested == root)
}

/// Native file tools take workspace-root-relative paths, while a delegated
/// objective may phrase a file relative to its sole authorized directory.
/// Normalize only that unambiguous read case: never search sibling scopes,
/// never rewrite an already-resolvable path, and require the exact scoped
/// candidate to satisfy the typed authorization boundary.
pub(super) fn normalize_single_scope_relative_read_value(
    tool_name: &str,
    mut parsed: serde_json::Value,
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    resource_scopes: Option<&[String]>,
) -> serde_json::Value {
    if tool_name != "read_file" {
        return parsed;
    }
    let Some(scopes) = resource_scopes else {
        return parsed;
    };
    let mut directories = scopes
        .iter()
        .filter_map(|scope| {
            let (mode, path) = scope.split_once(':')?;
            matches!(mode, "read" | "write" | "workspace").then_some(path.trim().replace('\\', "/"))
        })
        .filter(|path| !matches!(path.as_str(), "" | "." | "./"))
        .filter(|path| {
            resolver.resolve_existing(path).is_ok_and(|identity| {
                identity.object_kind == harness_contract::context::WorkspaceObjectKind::Directory
            })
        })
        .collect::<Vec<_>>();
    directories.sort();
    directories.dedup();
    let [scope] = directories.as_slice() else {
        return parsed;
    };

    let replacements = resource_paths_from_input(&parsed)
        .into_iter()
        .filter_map(|requested| {
            let parts = normalized_relative_parts(&requested)?;
            let requested = parts.join("/");
            if requested.is_empty() || path_within_scope(&requested, scope) {
                return None;
            }
            if let Ok(identity) = resolver.resolve_existing(&requested) {
                let canonical = identity.workspace_relative_path;
                return (canonical != requested
                    && resource_path_is_authorized(resolver, &canonical, scopes, false))
                .then_some((requested, canonical));
            }
            let candidate = format!("{scope}/{requested}");
            resource_path_is_authorized(resolver, &candidate, scopes, false)
                .then_some((requested, candidate))
        })
        .collect::<BTreeMap<_, _>>();
    rewrite_resource_path_fields(&mut parsed, &replacements);
    parsed
}

pub(super) fn rewrite_resource_path_fields(
    value: &mut serde_json::Value,
    replacements: &BTreeMap<String, String>,
) {
    match value {
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                if matches!(key.as_str(), "path" | "file" | "file_path") {
                    if let Some(path) = value.as_str() {
                        let normalized = path.trim().replace('\\', "/");
                        if let Some(replacement) = replacements.get(&normalized) {
                            *value = serde_json::Value::String(replacement.clone());
                        }
                    }
                } else {
                    rewrite_resource_path_fields(value, replacements);
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                rewrite_resource_path_fields(value, replacements);
            }
        }
        serde_json::Value::Null
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => {}
    }
}

/// Normalize workspace-internal absolute resource paths before both effect
/// description and execution. Keeping those two inputs byte-equivalent is a
/// security invariant: an input-sensitive ToolHost must be able to verify the
/// authorization descriptor without treating the safe relative rewrite as a
/// stale or escalated effect.
pub(super) fn normalize_workspace_internal_resource_value(
    _tool_name: &str,
    mut parsed: serde_json::Value,
    workspace_root: &std::path::Path,
) -> serde_json::Value {
    let replacements = resource_paths_from_input(&parsed)
        .iter()
        .filter_map(|path| {
            let absolute = std::path::Path::new(path);
            if !absolute.is_absolute() {
                return None;
            }
            let relative = absolute.strip_prefix(workspace_root).ok()?;
            let parts = normalized_relative_parts(&relative.to_string_lossy())?;
            Some((
                path.clone(),
                if parts.is_empty() {
                    ".".to_string()
                } else {
                    parts.join("/")
                },
            ))
        })
        .collect::<BTreeMap<_, _>>();
    if replacements.is_empty() {
        return parsed;
    }

    rewrite_resource_path_fields(&mut parsed, &replacements);
    parsed
}

pub(super) fn resource_paths_from_input(input: &serde_json::Value) -> Vec<String> {
    fn collect(value: &serde_json::Value, paths: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(map) => {
                for (key, value) in map {
                    if matches!(key.as_str(), "path" | "file" | "file_path") {
                        if let Some(path) = value.as_str() {
                            paths.push(path.trim().replace('\\', "/"));
                        }
                    } else {
                        collect(value, paths);
                    }
                }
            }
            serde_json::Value::Array(values) => {
                for value in values {
                    collect(value, paths);
                }
            }
            _ => {}
        }
    }
    let mut paths = Vec::new();
    collect(input, &mut paths);
    paths.sort();
    paths.dedup();
    paths
}

pub(super) fn delegated_tool_supports_bounded_scope(
    host: &dyn RuntimeExecutionHost,
    tool_name: &str,
) -> bool {
    host.delegated_tool_effect_descriptor(tool_name, &serde_json::json!({}))
        .is_some_and(|descriptor| crate::delegated_tool_effect_is_bounded(&descriptor))
}

pub(super) fn resource_path_is_authorized(
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    requested: &str,
    allowed_scopes: &[String],
    write: bool,
) -> bool {
    let requested = if write {
        resolver.resolve_planned_file(requested)
    } else {
        resolver.resolve_existing(requested)
    };
    let Ok(requested) = requested else {
        return false;
    };
    allowed_scopes.iter().any(|scope| {
        let (mode, allowed_scope) = scope.split_once(':').unwrap_or(("", ""));
        if (write && !matches!(mode, "write" | "workspace"))
            || (!write && !matches!(mode, "read" | "write" | "workspace"))
        {
            return false;
        }
        // `read:.` / `write:.` are whole-workspace leases issued only by the
        // full-trust session policy. Reaching this point with a `.` scope
        // therefore proves Runtime authorized the entire workspace.
        // The workspace identity check below still bounds them to this
        // workspace and never to absolute or traversing paths.
        let allowed_existing = resolver.resolve_existing(allowed_scope).ok();
        let allowed = if matches!(mode, "write" | "workspace") {
            resolver.resolve_planned_file(allowed_scope).ok()
        } else {
            allowed_existing.clone()
        };
        let Some(allowed) = allowed else {
            return false;
        };
        if requested.workspace_id != allowed.workspace_id
            || requested.repository_id != allowed.repository_id
        {
            return false;
        }
        if allowed.object_kind == harness_contract::context::WorkspaceObjectKind::Directory {
            path_within_scope(
                &requested.repository_relative_path,
                &allowed.repository_relative_path,
            )
        } else if allowed_existing.is_none() && is_virtual_directory_scope(allowed_scope) {
            // `resolve_planned_file` intentionally classifies a missing path
            // as File.  For the reserved virtual delivery namespace, a
            // descendant is nevertheless a valid directory member; retain
            // identity and repository checks above and compare lexically.
            path_within_scope(
                &requested.repository_relative_path,
                &allowed.repository_relative_path,
            )
        } else {
            requested.repository_relative_path == allowed.repository_relative_path
        }
    })
}

pub(super) fn normalized_relative_parts(value: &str) -> Option<Vec<String>> {
    let normalized = value.trim().replace('\\', "/");
    if normalized.is_empty() {
        return None;
    }
    let mut parts = Vec::new();
    for component in std::path::Path::new(&normalized).components() {
        match component {
            std::path::Component::Normal(part) => {
                parts.push(part.to_string_lossy().into_owned());
            }
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir
            | std::path::Component::RootDir
            | std::path::Component::Prefix(_) => return None,
        }
    }
    Some(parts)
}

pub(super) async fn settle_failed_agentic_attempt(
    services: &Arc<RuntimeServices>,
    packet: &AgentTaskPacket,
    reason: &str,
) {
    let program_id = packet
        .context_refs
        .iter()
        .find_map(|reference| reference.strip_prefix("agentic_program:"));
    let task_ref = packet
        .context_refs
        .iter()
        .find_map(|reference| reference.strip_prefix("agentic_task:"));
    let agent_id = packet
        .context_refs
        .iter()
        .find_map(|reference| reference.strip_prefix("agentic_member:"));
    let attempt_mode = packet.context_refs.iter().find_map(|reference| {
        reference
            .strip_prefix("agentic_mode:")
            .and_then(|mode| match mode {
                "execute" => Some(harness_contract::agent_action::AgentAttemptMode::Execute),
                "review" => Some(harness_contract::agent_action::AgentAttemptMode::Review),
                _ => None,
            })
    });
    let (Some(program_id), Some(task_ref), Some(agent_id), Some(attempt_mode)) =
        (program_id, task_ref, agent_id, attempt_mode)
    else {
        return;
    };
    let service = services.agent_action_service();
    let Ok(projection) = service.project(program_id) else {
        return;
    };
    let Some(task) = projection.tasks.get(task_ref) else {
        return;
    };
    let attempt_is_current = match attempt_mode {
        harness_contract::agent_action::AgentAttemptMode::Execute => {
            task.status == crate::AgenticTaskStatus::Claimed
                && task.claimant.as_deref() == Some(agent_id)
                && task.claim_execution_id.as_deref() == Some(packet.graph_id())
        }
        harness_contract::agent_action::AgentAttemptMode::Review => {
            task.status == crate::AgenticTaskStatus::Submitted
        }
    };
    if !attempt_is_current {
        return;
    }
    let Some(member) = projection.agents.get(agent_id) else {
        return;
    };
    let team_id = member.team_id.clone();
    let context = crate::AgenticDispatchContext {
        session_id: projection.session_id.clone(),
        turn_id: projection.turn_id.clone(),
        model_lease: projection.model_lease.clone(),
        permission_ceiling: projection.permission_ceiling,
        resource_scopes: projection.resource_scopes.clone(),
    };
    let envelope = harness_contract::agent_action::AgentActionEnvelope {
        action_id: format!("runtime-release:{}", packet.run_id()),
        actor: harness_contract::agent_action::AgentActorBinding {
            objective_id: projection.objective_id.clone(),
            program_id: projection.program_id.clone(),
            session_id: projection.session_id.clone(),
            turn_id: projection.turn_id.clone(),
            root_execution_id: projection.root_execution_id.clone(),
            required_team_count: projection.required_team_count,
            objective_summary: projection.objective_summary.clone(),
            model_lease: projection.model_lease.clone(),
            permission_ceiling: Some(projection.permission_ceiling),
            resource_scopes: projection.resource_scopes.clone(),
            actor_id: "runtime.program-supervisor".to_string(),
            kind: harness_contract::agent_action::AgentActorKind::Supervisor,
            execution_id: Some(packet.graph_id().to_string()),
            team_id: Some(team_id),
            agent_id: None,
        },
        expected_revision: None,
        action: harness_contract::agent_action::AgentAction::TaskAttemptFail(
            harness_contract::agent_action::TaskAttemptFailInput {
                task_ref: task_ref.to_string(),
                execution_id: packet.graph_id().to_string(),
                mode: attempt_mode,
                reason: reason.to_string(),
                retryable: true,
            },
        ),
    };
    match service.apply(&envelope) {
        Ok(observation)
            if observation.status == harness_contract::agent_action::AgentActionStatus::Applied =>
        {
            if let Err(error) = services
                .dispatch_agentic_followups(&envelope, context)
                .await
            {
                tracing::warn!(%error, task_ref, agent_id, "failed to reconcile retryable Agent-first attempt");
            }
        }
        Ok(_) => {}
        Err(error) => {
            tracing::warn!(%error, task_ref, agent_id, "failed to settle abandoned Agent-first claim");
        }
    }
}
