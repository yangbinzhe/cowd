fn gateway_observed_evidence(
    executor: &GatewayToolExecutor,
    request: &runtime::RuntimeToolExecutionRequest,
    output: &str,
    outcome_evidence_ref: &str,
) -> Vec<harness_contract::context::ObservedEvidence> {
    let Some(services) = executor.runtime_services.get() else {
        return Vec::new();
    };
    // Only adapters whose successful structured output contains the actual
    // object identity and digest may mint an observation. Request arguments,
    // generic `path` keys, categories and the current filesystem are never
    // evidence of what a tool really observed.
    let Ok(output) = serde_json::from_str::<serde_json::Value>(output) else {
        return Vec::new();
    };
    let resolver = services.path_identity_resolver();
    let sequence = request.observation_wave_sequence;
    if sequence == 0 {
        return Vec::new();
    }
    let mut observed = match request.tool_name.as_str() {
        "read_file" => complete_read_evidence(resolver, "read_file", &output, sequence)
            .into_iter()
            .collect(),
        "read_many" => batch_success_outputs(&output)
            .filter_map(|child| complete_read_evidence(resolver, "read_file", child, sequence))
            .collect(),
        "write_file" => write_file_evidence(resolver, "write_file", &output, sequence)
            .into_iter()
            .collect(),
        "edit_file" => edit_file_evidence(resolver, &output, sequence)
            .into_iter()
            .collect(),
        "apply_patch_transaction" => patch_transaction_evidence(resolver, &output, sequence),
        "glob_search" => discovery_evidence(
            resolver,
            "glob_search",
            &output,
            "basePath",
            "scanComplete",
            true,
            harness_contract::context::EvidenceCoverageKind::GlobDiscovery,
            sequence,
        )
        .into_iter()
        .collect(),
        "glob_many" => batch_success_outputs(&output)
            .filter_map(|child| {
                discovery_evidence(
                    resolver,
                    "glob_search",
                    child,
                    "basePath",
                    "scanComplete",
                    true,
                    harness_contract::context::EvidenceCoverageKind::GlobDiscovery,
                    sequence,
                )
            })
            .collect(),
        "notebook_edit" => output
            .get("error")
            .is_none_or(serde_json::Value::is_null)
            .then(|| {
                write_file_evidence_from_fields(
                    resolver,
                    "notebook_edit",
                    &output,
                    "notebook_path",
                    "updated_file",
                    sequence,
                )
            })
            .flatten()
            .into_iter()
            .collect(),
        "web_fetch" => output
            .get("url")
            .and_then(serde_json::Value::as_str)
            .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
            .filter(|_| {
                output
                    .get("code")
                    .and_then(serde_json::Value::as_u64)
                    .is_some_and(|v| v > 0)
            })
            .filter(|_| {
                output
                    .pointer("/networkPolicy/denied")
                    .and_then(serde_json::Value::as_bool)
                    == Some(false)
                    && output
                        .pointer("/networkPolicy/requires_approval")
                        .and_then(serde_json::Value::as_bool)
                        == Some(false)
            })
            .map(|url| network_evidence("web_fetch", url, sequence))
            .into_iter()
            .collect(),
        // These tools either do not observe an acceptance target, or their
        // current output cannot prove completeness/digest. Fail closed rather
        // than reconstructing truth from request arguments.
        _ => Vec::new(),
    };
    let access_ref = harness_contract::context::EvidenceAccessRef::unavailable(
        harness_contract::reality::EvidenceRef::observed("tool_execution", outcome_evidence_ref),
        "application/vnd.cowd.tool-receipt+json",
        request
            .session_id
            .as_deref()
            .map_or_else(|| "runtime".to_string(), |id| format!("session:{id}")),
    );
    for item in &mut observed {
        item.evidence_ref = Some(access_ref.clone());
    }
    observed
}

fn batch_success_outputs(output: &serde_json::Value) -> impl Iterator<Item = &serde_json::Value> {
    output
        .get("results")
        .and_then(serde_json::Value::as_array)
        .into_iter()
        .flatten()
        .filter(|item| item.get("status").and_then(serde_json::Value::as_str) == Some("success"))
        .filter_map(|item| item.get("output"))
}

fn complete_read_evidence(
    resolver: &runtime::path_identity::WorkspacePathIdentityResolver,
    tool_name: &str,
    output: &serde_json::Value,
    sequence: u64,
) -> Option<harness_contract::context::ObservedEvidence> {
    resolver
        .observe_complete_read_tool_output(tool_name, output, sequence)
        .ok()
}

fn write_file_evidence(
    resolver: &runtime::path_identity::WorkspacePathIdentityResolver,
    tool_name: &str,
    output: &serde_json::Value,
    sequence: u64,
) -> Option<harness_contract::context::ObservedEvidence> {
    let content = output.get("content")?.as_str()?;
    let prior_state = match (
        output.get("type").and_then(serde_json::Value::as_str),
        output.get("originalFile"),
    ) {
        (Some("create"), Some(value)) if value.is_null() => {
            harness_contract::context::WorkspacePriorState::Absent
        }
        (Some("update"), Some(value)) => {
            let original = value.as_str()?;
            harness_contract::context::WorkspacePriorState::Existing {
                sha256: format!("{:x}", Sha256::digest(original.as_bytes())),
            }
        }
        _ => return None,
    };
    resolver
        .observe_trusted_tool_output_file(
            tool_name,
            harness_contract::context::WorkspaceAccessMode::Write,
            output.get("filePath")?.as_str()?,
            &format!("{:x}", Sha256::digest(content.as_bytes())),
            sequence,
        )
        .ok()
        .map(|mut evidence| {
            evidence.workspace_prior_state = Some(prior_state);
            evidence
        })
}

fn write_file_evidence_from_fields(
    resolver: &runtime::path_identity::WorkspacePathIdentityResolver,
    tool_name: &str,
    output: &serde_json::Value,
    path_key: &str,
    content_key: &str,
    sequence: u64,
) -> Option<harness_contract::context::ObservedEvidence> {
    let content = output.get(content_key)?.as_str()?;
    let original = output.get("original_file")?.as_str()?;
    resolver
        .observe_trusted_tool_output_file(
            tool_name,
            harness_contract::context::WorkspaceAccessMode::Write,
            output.get(path_key)?.as_str()?,
            &format!("{:x}", Sha256::digest(content.as_bytes())),
            sequence,
        )
        .ok()
        .map(|mut evidence| {
            evidence.workspace_prior_state =
                Some(harness_contract::context::WorkspacePriorState::Existing {
                    sha256: format!("{:x}", Sha256::digest(original.as_bytes())),
                });
            evidence
        })
}

fn patch_transaction_evidence(
    resolver: &runtime::path_identity::WorkspacePathIdentityResolver,
    output: &serde_json::Value,
    sequence: u64,
) -> Vec<harness_contract::context::ObservedEvidence> {
    let Some(applied) = output.get("applied").and_then(serde_json::Value::as_array) else {
        return Vec::new();
    };
    if output
        .get("appliedCount")
        .and_then(serde_json::Value::as_u64)
        != u64::try_from(applied.len()).ok()
    {
        return Vec::new();
    }
    let mut seen = std::collections::BTreeSet::new();
    let facts = applied
        .iter()
        .map(|file| {
            let path = file.get("resolvedPath")?.as_str()?;
            seen.insert(path.to_string()).then_some(())?;
            let digest = file.get("sha256")?.as_str()?;
            is_sha256_hex(digest).then_some(())?;
            let previous = file.get("previousSha256")?.as_str()?;
            is_sha256_hex(previous).then_some(())?;
            resolver
                .observe_trusted_tool_output_file(
                    "apply_patch_transaction",
                    harness_contract::context::WorkspaceAccessMode::Write,
                    path,
                    digest,
                    sequence,
                )
                .ok()
                .map(|mut evidence| {
                    evidence.workspace_prior_state =
                        Some(harness_contract::context::WorkspacePriorState::Existing {
                            sha256: previous.to_string(),
                        });
                    evidence
                })
        })
        .collect::<Option<Vec<_>>>();
    facts.unwrap_or_default()
}

fn edit_file_evidence(
    resolver: &runtime::path_identity::WorkspacePathIdentityResolver,
    output: &serde_json::Value,
    sequence: u64,
) -> Option<harness_contract::context::ObservedEvidence> {
    let original = output.get("originalFile")?.as_str()?;
    let old = output.get("oldString")?.as_str()?;
    let new = output.get("newString")?.as_str()?;
    if output
        .get("userModified")
        .and_then(serde_json::Value::as_bool)
        != Some(false)
        || old == new
        || !original.contains(old)
    {
        return None;
    }
    let updated = if output
        .get("replaceAll")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        original.replace(old, new)
    } else {
        original.replacen(old, new, 1)
    };
    resolver
        .observe_trusted_tool_output_file(
            "edit_file",
            harness_contract::context::WorkspaceAccessMode::Write,
            output.get("filePath")?.as_str()?,
            &format!("{:x}", Sha256::digest(updated.as_bytes())),
            sequence,
        )
        .ok()
        .map(|mut evidence| {
            evidence.workspace_prior_state =
                Some(harness_contract::context::WorkspacePriorState::Existing {
                    sha256: format!("{:x}", Sha256::digest(original.as_bytes())),
                });
            evidence
        })
}

#[allow(clippy::too_many_arguments)]
fn discovery_evidence(
    resolver: &runtime::path_identity::WorkspacePathIdentityResolver,
    tool_name: &str,
    output: &serde_json::Value,
    path_key: &str,
    complete_key: &str,
    expected_complete: bool,
    coverage: harness_contract::context::EvidenceCoverageKind,
    sequence: u64,
) -> Option<harness_contract::context::ObservedEvidence> {
    (output
        .get(complete_key)
        .and_then(serde_json::Value::as_bool)
        == Some(expected_complete))
    .then_some(())?;
    if coverage == harness_contract::context::EvidenceCoverageKind::GlobDiscovery
        && output.get("truncated").and_then(serde_json::Value::as_bool) != Some(false)
    {
        return None;
    }
    resolver
        .observe_trusted_tool_output_scope(
            tool_name,
            harness_contract::context::WorkspaceAccessMode::Read,
            output.get(path_key)?.as_str()?,
            harness_contract::context::WorkspaceObjectKind::Directory,
            coverage,
            None,
            sequence,
        )
        .ok()
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn network_evidence(
    tool_name: &str,
    endpoint: &str,
    sequence: u64,
) -> harness_contract::context::ObservedEvidence {
    harness_contract::context::ObservedEvidence {
        obligation_id: format!("network:{:x}", Sha256::digest(endpoint.as_bytes())),
        target: harness_contract::context::EvidenceTargetIdentity::Network {
            endpoint: endpoint.to_string(),
        },
        observed_at_sequence: sequence,
        tool_name: tool_name.to_string(),
        provenance: harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
        evidence_ref: None,
        model_observation: None,
        workspace_prior_state: None,
    }
}

