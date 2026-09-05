use super::*;

pub(super) fn agent_evidence_refs(
    packet: &AgentTaskPacket,
    audits: &[harness_contract::context::EvidenceAuditProjection],
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::context::EvidenceAccessRef> {
    let mut refs = packet.evidence_refs.clone();
    refs.extend(audits.iter().filter_map(|audit| audit.access.clone()));
    refs.extend(
        receipts
            .iter()
            .flat_map(|receipt| receipt.observed_evidence.iter())
            .filter_map(|evidence| evidence.evidence_ref.clone()),
    );
    refs.sort_by(|left, right| {
        left.evidence_ref
            .ref_type
            .cmp(&right.evidence_ref.ref_type)
            .then_with(|| left.evidence_ref.id.cmp(&right.evidence_ref.id))
    });
    refs.dedup_by(|left, right| left.evidence_ref == right.evidence_ref);
    refs
}

/// Exact acquisition and exact model observation are separate facts. The
/// ToolHost receipt proves the former; a non-omitting model receipt proves
/// the latter. Only model-observed exact evidence may satisfy an Agent's
/// semantic acceptance contract.
pub(super) fn model_observed_evidence(
    required: &harness_contract::context::RequiredAcceptance,
    model_observations: &[harness_contract::context::ProviderModelObservationAttestation],
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::context::ObservedEvidence> {
    receipts
        .iter()
        .flat_map(|receipt| {
            receipt.observed_evidence.iter().cloned().map(|mut observed| {
                let Some(provider_invocation_id) = receipt.provider_invocation_id.as_deref()
                else {
                    return observed;
                };
                let matching_attestation = model_observations.iter().find(|attestation| {
                    attestation.provider_invocation_id == provider_invocation_id
                        && required.evidence_obligations.iter().any(|obligation| {
                            obligation.observation_requirement
                                == harness_contract::context::EvidenceObservationRequirement::ProviderModel
                                && attestation
                                    .obligation_ids
                                    .contains(&obligation.obligation_id)
                                && crate::path_identity::observed_evidence_satisfies(
                                    obligation,
                                    &harness_contract::context::ObservedEvidence {
                                        model_observation: Some((*attestation).clone()),
                                        ..observed.clone()
                                    },
                                )
                        })
                });
                if let Some(attestation) = matching_attestation {
                    observed.model_observation = Some(attestation.clone());
                }
                observed
            })
        })
        .collect()
}

/// Derive structured-field criteria from the terminal answer and canonical
/// ToolHost receipts.  Obligation matching itself remains exclusively owned
/// by `AcceptanceEvaluator::evaluate_required` at the terminal boundary.
pub(super) fn derive_receipt_backed_satisfied_criteria(
    packet: &AgentTaskPacket,
    summary: &crate::TurnSummary,
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
    tool_executor: &ScopedRuntimeToolExecutor,
    model_observed_evidence: &[harness_contract::context::ObservedEvidence],
) -> (
    Vec<String>,
    Vec<harness_contract::agent::AgentChangeReceipt>,
) {
    let mut receipts = tool_executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    receipts.sort_by_key(|receipt| receipt.sequence);
    // A fresh, successfully committed scoped tool receipt is independent
    // evidence even when its content-addressed EvidenceRef equals an upstream
    // read of the same unchanged file. Comparing only EvidenceRef identity
    // incorrectly erased reviewer verification from the acceptance result.
    let produced_evidence = produced_runtime_evidence(packet, evidence_refs, &receipts);
    let changes = materialized_change_receipts(&receipts);
    let scope_observed = |scope: &str| {
        let raw = if scope.contains(':') {
            scope.to_string()
        } else {
            format!("read:{scope}")
        };
        // The whole-workspace read alias is only minted under a full-trust
        // lease. Compile it with root-alias tolerance so any Runtime-attested
        // descendant exact read satisfies it; the strict compiler would keep
        // the obligation unsatisfiable and fail every Team role terminal.
        let required = if matches!(raw.trim(), "read:." | "read:./") {
            tool_executor
                .path_identity_resolver
                .compile_obligation_with_root_alias(&raw, true)
                .unwrap_or_else(|_| {
                    tool_executor
                        .path_identity_resolver
                        .compile_obligation_or_unresolved(&raw)
                })
        } else {
            tool_executor
                .path_identity_resolver
                .compile_obligation_or_unresolved(&raw)
        };
        crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
            &required,
            model_observed_evidence,
        )
    };
    let required_fields = packet_acceptance_contract(packet)
        .iter()
        .filter_map(|requirement| match &requirement.check {
            harness_contract::agent::OutputAcceptanceCheck::StructuredField { field }
            | harness_contract::agent::OutputAcceptanceCheck::WorkspaceChange { field, .. } => {
                Some(field.as_str().to_string())
            }
            harness_contract::agent::OutputAcceptanceCheck::StructuredArtifact { name } => {
                Some(name.clone())
            }
            harness_contract::agent::OutputAcceptanceCheck::SourceVerification { .. } => {
                Some("source_verification".to_string())
            }
            harness_contract::agent::OutputAcceptanceCheck::UpstreamReview => {
                Some("review".to_string())
            }
            harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence { .. }
            | harness_contract::agent::OutputAcceptanceCheck::UpstreamEvidence => None,
        })
        .collect::<Vec<_>>();
    let output = structured_agent_output_for_fields(&summary.final_answer, &required_fields);
    let field_present = |field: harness_contract::agent::StructuredOutputField| {
        let value = output
            .as_ref()
            .and_then(|object| object.get(field.as_str()));
        structured_field_materialized(field, value)
    };
    let artifact_present = |name: &str| {
        output
            .as_ref()
            .and_then(|object| object.get(name))
            .is_some_and(materialized_json_value)
    };
    let changes_in_scopes = |scopes: &[String]| {
        !changes.is_empty()
            && changes.iter().all(|change| {
                scopes
                    .iter()
                    .any(|scope| path_within_scope(&change.path, scope))
            })
    };
    let upstream_changes = packet_upstream_change_receipts(packet);
    let upstream_evidence = packet
        .evidence_refs
        .iter()
        .any(crate::agent_result_validator::is_materialized_durable_evidence);
    let acceptance = packet_acceptance_contract(packet)
        .into_iter()
        .filter(|requirement| match &requirement.check {
            harness_contract::agent::OutputAcceptanceCheck::StructuredField { field } => {
                // A pure reducer is grounded by the immutable predecessor
                // evidence carried in its packet. Requiring it to reacquire
                // the same source solely to populate a structured synthesis
                // field would defeat the upstream-only acceptance contract.
                (produced_evidence || upstream_evidence) && field_present(*field)
            }
            harness_contract::agent::OutputAcceptanceCheck::StructuredArtifact { name } => {
                artifact_present(name)
            }
            harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence { scopes } => {
                produced_evidence
                    && !scopes.is_empty()
                    && scopes.iter().all(|scope| scope_observed(scope))
            }
            harness_contract::agent::OutputAcceptanceCheck::WorkspaceChange { field, scopes } => {
                produced_evidence && field_present(*field) && changes_in_scopes(&scopes)
            }
            harness_contract::agent::OutputAcceptanceCheck::SourceVerification { scopes } => {
                produced_evidence
                    && field_present(
                        harness_contract::agent::StructuredOutputField::SourceVerification,
                    )
                    && changes_in_scopes(&scopes)
                    && changes.iter().all(|change| {
                        has_matching_pre_write_evidence(change, &receipts)
                            && has_matching_read_receipt(change, &receipts, true)
                    })
            }
            harness_contract::agent::OutputAcceptanceCheck::UpstreamReview => {
                produced_evidence
                    && field_present(harness_contract::agent::StructuredOutputField::Review)
                    && upstream_evidence
                    && !upstream_changes.is_empty()
                    && upstream_changes
                        .iter()
                        .all(|change| has_matching_read_receipt(change, &receipts, false))
            }
            harness_contract::agent::OutputAcceptanceCheck::UpstreamEvidence => upstream_evidence,
        })
        .map(|requirement| requirement.criterion)
        .collect::<Vec<_>>();
    (acceptance, changes)
}

pub(super) fn packet_upstream_change_receipts(
    packet: &AgentTaskPacket,
) -> Vec<harness_contract::agent::AgentChangeReceipt> {
    let changes = packet
        .evidence_refs
        .iter()
        .filter_map(|evidence| {
            (crate::agent_result_validator::is_materialized_durable_evidence(evidence)
                && evidence.evidence_ref.ref_type == "runtime_change")
                .then(|| {
                    serde_json::from_str::<harness_contract::agent::AgentChangeReceipt>(
                        &evidence.evidence_ref.id,
                    )
                    .ok()
                })
                .flatten()
        })
        .collect::<Vec<_>>();
    let mut by_path = BTreeMap::<String, Vec<harness_contract::agent::AgentChangeReceipt>>::new();
    for change in changes {
        by_path.entry(change.path.clone()).or_default().push(change);
    }
    let mut terminal = Vec::new();
    for (_, mut receipts) in by_path {
        receipts.sort_by(|left, right| {
            left.write_sequence
                .cmp(&right.write_sequence)
                .then_with(|| left.after_sha256.cmp(&right.after_sha256))
        });
        receipts.dedup();
        let candidates = receipts
            .iter()
            .filter(|candidate| {
                !receipts.iter().any(|successor| {
                    successor.before_sha256.as_deref() == Some(candidate.after_sha256.as_str())
                        && successor.after_sha256 != candidate.after_sha256
                })
            })
            .collect::<Vec<_>>();
        let digest = candidates
            .first()
            .map(|change| change.after_sha256.as_str());
        if digest.is_none()
            || candidates
                .iter()
                .any(|change| Some(change.after_sha256.as_str()) != digest)
        {
            // A divergent terminal must fail UpstreamReview acceptance, not
            // be guessed from lexical receipt order.
            return Vec::new();
        }
        if let Some(change) = candidates
            .into_iter()
            .max_by_key(|change| change.write_sequence)
        {
            terminal.push(change.clone());
        }
    }
    terminal.sort_by(|left, right| left.path.cmp(&right.path));
    terminal
}

pub(super) fn produced_runtime_evidence(
    packet: &AgentTaskPacket,
    evidence_refs: &[harness_contract::context::EvidenceAccessRef],
    receipts: &[ScopedToolExecutionReceipt],
) -> bool {
    !receipts.is_empty()
        || evidence_refs.iter().any(|evidence| {
            crate::agent_result_validator::is_materialized_durable_evidence(evidence)
                && !packet
                    .evidence_refs
                    .iter()
                    .any(|input| input.evidence_ref == evidence.evidence_ref)
        })
}

pub(super) fn structured_field_materialized(
    field: harness_contract::agent::StructuredOutputField,
    value: Option<&serde_json::Value>,
) -> bool {
    structured_contract_field_materialized(field.as_str(), value)
}

/// Canonical materialization semantics for fixed Team presentation fields.
///
/// Disclosure fields distinguish an explicit empty list (reviewed, with no
/// items found) from an omitted or null field. Host presentation recovery and
/// delegated Agent acceptance must share this exact rule so a valid terminal
/// cannot be accepted by one boundary and rejected by the other.
pub(crate) fn structured_contract_field_materialized(
    field: &str,
    value: Option<&serde_json::Value>,
) -> bool {
    if matches!(field, "risks" | "unresolved" | "unresolved_or_risks") {
        value.is_some_and(|value| {
            matches!(value, serde_json::Value::Array(_)) || materialized_json_value(value)
        })
    } else {
        value.is_some_and(materialized_json_value)
    }
}

pub(super) fn materialized_change_receipts(
    receipts: &[ScopedToolExecutionReceipt],
) -> Vec<harness_contract::agent::AgentChangeReceipt> {
    receipts
        .iter()
        .filter(|receipt| receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write)
        .flat_map(|receipt| {
            receipt.paths.iter().filter_map(|path| {
                let prior = receipt.prior_states.get(path)?;
                let before = match prior {
                    harness_contract::context::WorkspacePriorState::Existing { sha256 } => {
                        Some(sha256.clone())
                    }
                    harness_contract::context::WorkspacePriorState::Absent => None,
                };
                let after = receipt.after_digests.get(path).cloned().flatten()?;
                (before.as_deref() != Some(after.as_str())).then(|| {
                    let reread = receipts.iter().find(|candidate| {
                        candidate.sequence > receipt.sequence
                            && candidate.effect_kind == harness_contract::tool::ToolEffectKind::Read
                            && candidate.paths.iter().any(|candidate_path| {
                                path_within_scope(candidate_path, path)
                                    && path_within_scope(path, candidate_path)
                                    && candidate
                                        .after_digests
                                        .get(candidate_path)
                                        .and_then(|digest| digest.as_deref())
                                        == Some(after.as_str())
                            })
                    });
                    let bytes = reread.and_then(|candidate| {
                        candidate.paths.iter().find_map(|candidate_path| {
                            (path_within_scope(candidate_path, path)
                                && path_within_scope(path, candidate_path))
                            .then(|| candidate.observed_bytes.get(candidate_path).copied())
                            .flatten()
                        })
                    });
                    let reread_evidence_ref = reread.and_then(|candidate| {
                        candidate.observed_evidence.iter().find_map(|evidence| {
                            let matches_path = match &evidence.target {
                                harness_contract::context::EvidenceTargetIdentity::Workspace {
                                    scope,
                                } => {
                                    path_within_scope(&scope.path.workspace_relative_path, path)
                                        && path_within_scope(
                                            path,
                                            &scope.path.workspace_relative_path,
                                        )
                                }
                                _ => false,
                            };
                            matches_path
                                .then(|| {
                                    evidence
                                        .evidence_ref
                                        .as_ref()
                                        .map(|reference| reference.retrieval_selector.clone())
                                })
                                .flatten()
                        })
                    });
                    harness_contract::agent::AgentChangeReceipt {
                        path: path.clone(),
                        before_sha256: before,
                        after_sha256: after,
                        write_sequence: receipt.sequence,
                        bytes,
                        reread_sequence: reread.map(|candidate| candidate.sequence),
                        reread_evidence_ref,
                    }
                })
            })
        })
        .collect()
}

pub(super) fn has_matching_read_receipt(
    change: &harness_contract::agent::AgentChangeReceipt,
    receipts: &[ScopedToolExecutionReceipt],
    require_later_sequence: bool,
) -> bool {
    receipts.iter().any(|receipt| {
        if (require_later_sequence && receipt.sequence <= change.write_sequence)
            || receipt.effect_kind != harness_contract::tool::ToolEffectKind::Read
        {
            return false;
        }
        // Tool effect planning may retain an absolute or `./`-prefixed key
        // while the public receipt path is workspace-relative. Resolve the
        // digest through the receipt's own key after scope normalization;
        // looking it up with the upstream spelling made a valid independent
        // review fail intermittently even though both paths named the same
        // workspace file.
        receipt.paths.iter().any(|receipt_path| {
            path_within_scope(receipt_path, &change.path)
                && path_within_scope(&change.path, receipt_path)
                && receipt
                    .after_digests
                    .get(receipt_path)
                    .and_then(|digest| digest.as_deref())
                    == Some(change.after_sha256.as_str())
        })
    })
}

pub(super) fn has_matching_pre_write_evidence(
    change: &harness_contract::agent::AgentChangeReceipt,
    receipts: &[ScopedToolExecutionReceipt],
) -> bool {
    let Some(before_sha256) = change.before_sha256.as_deref() else {
        // For a new file, the write receipt itself is the Runtime-owned
        // absence proof: the tool host captured `None` before committing the
        // exact write whose sequence and after digest produced this change.
        // A later matching read is still required by SourceVerification.
        return receipts.iter().any(|receipt| {
            receipt.sequence == change.write_sequence
                && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Write
                && receipt.paths.iter().any(|receipt_path| {
                    path_within_scope(receipt_path, &change.path)
                        && path_within_scope(&change.path, receipt_path)
                        && receipt.prior_states.get(receipt_path).is_some_and(|state| {
                            matches!(
                                state,
                                harness_contract::context::WorkspacePriorState::Absent
                            )
                        })
                        && receipt
                            .after_digests
                            .get(receipt_path)
                            .and_then(|digest| digest.as_deref())
                            == Some(change.after_sha256.as_str())
                })
        });
    };
    receipts.iter().any(|receipt| {
        receipt.sequence < change.write_sequence
            && receipt.effect_kind == harness_contract::tool::ToolEffectKind::Read
            && receipt.paths.iter().any(|receipt_path| {
                path_within_scope(receipt_path, &change.path)
                    && path_within_scope(&change.path, receipt_path)
                    && receipt
                        .after_digests
                        .get(receipt_path)
                        .and_then(|digest| digest.as_deref())
                        == Some(before_sha256)
            })
    })
}

pub(super) fn packet_acceptance_contract(
    packet: &AgentTaskPacket,
) -> Vec<harness_contract::agent::OutputAcceptanceRequirement> {
    packet.output_acceptance.clone()
}

pub(super) fn materialized_json_value(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::String(value) => !value.trim().is_empty(),
        serde_json::Value::Array(values) => !values.is_empty(),
        serde_json::Value::Object(values) => !values.is_empty(),
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => true,
    }
}

/// Repair only the common syntactic drift where a provider leaves a trailing
/// comma before `}` or `]`. This deliberately does not invent keys, values, or
/// quote unquoted prose, so acceptance semantics remain model-authored.
pub(super) fn without_json_trailing_commas(text: &str) -> String {
    let characters = text.chars().collect::<Vec<_>>();
    let mut repaired = String::with_capacity(text.len());
    let mut index = 0;
    let mut in_string = false;
    let mut escaped = false;
    while index < characters.len() {
        let character = characters[index];
        if in_string {
            repaired.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
            index += 1;
            continue;
        }
        if character == '"' {
            in_string = true;
            repaired.push('"');
            index += 1;
            continue;
        }
        if character == ',' {
            let mut lookahead = index + 1;
            while lookahead < characters.len() && characters[lookahead].is_ascii_whitespace() {
                lookahead += 1;
            }
            if lookahead < characters.len() && matches!(characters[lookahead], '}' | ']') {
                index += 1;
                continue;
            }
        }
        repaired.push(character);
        index += 1;
    }
    repaired
}

pub(super) fn parse_first_contract_json(text: &str) -> Option<serde_json::Value> {
    let text = text.trim_start_matches('\u{feff}').trim();
    serde_json::Deserializer::from_str(text)
        .into_iter::<serde_json::Value>()
        .next()
        .and_then(Result::ok)
        .or_else(|| {
            let repaired = without_json_trailing_commas(text);
            (repaired != text).then(|| {
                serde_json::Deserializer::from_str(&repaired)
                    .into_iter::<serde_json::Value>()
                    .next()
                    .and_then(Result::ok)
            })?
        })
}

pub(crate) fn structured_agent_output(
    text: &str,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    // Keep this list aligned with `StructuredOutputField`. The parser
    // remains allow-listed: an arbitrary prose label cannot become an
    // acceptance field merely because it contains a colon. Runtime evidence,
    // rather than a model-authored compatibility field, is the sole proof for
    // custom acceptance criteria.
    const CONTRACT_FIELDS: [&str; 15] = [
        "summary",
        "findings",
        "evidence",
        "plan",
        "implementation",
        "source_verification",
        "review",
        "risks",
        "unresolved",
        "key_decisions",
        "unresolved_or_risks",
        "proposal",
        "critique",
        "mitigation",
        "checkpoint",
    ];
    let has_contract_field = |object: &serde_json::Map<String, serde_json::Value>| {
        CONTRACT_FIELDS
            .iter()
            .any(|field| object.contains_key(*field))
    };
    let canonicalize = |mut object: serde_json::Map<String, serde_json::Value>| {
        const ALIASES: [(&str, &str); 13] = [
            ("conclusion", "summary"),
            ("result", "summary"),
            ("摘要", "summary"),
            ("总结", "summary"),
            ("finding", "findings"),
            ("observations", "findings"),
            ("发现", "findings"),
            ("proof", "evidence"),
            ("证据", "evidence"),
            ("open_questions", "unresolved"),
            ("gaps", "unresolved"),
            ("未解决", "unresolved"),
            ("risk", "risks"),
        ];
        for (alias, canonical) in ALIASES {
            if object.contains_key(canonical) {
                continue;
            }
            if let Some(key) = object
                .keys()
                .find(|key| key.eq_ignore_ascii_case(alias))
                .cloned()
            {
                if let Some(value) = object.remove(&key) {
                    object.insert(canonical.to_string(), value);
                }
            }
        }
        object
    };
    let contract_object = |object: serde_json::Map<String, serde_json::Value>| {
        let object = canonicalize(object);
        has_contract_field(&object).then_some(object)
    };
    if let Some(serde_json::Value::Object(object)) = parse_first_contract_json(text) {
        if let Some(object) = contract_object(object.clone()) {
            return Some(object);
        }
        // Providers commonly wrap an otherwise valid response in a single
        // `output`/`data`/`response`/`answer` envelope. Unwrap only a typed
        // object (or a string containing one) with a known contract field;
        // arbitrary prose and unrelated JSON remain untrusted.
        for wrapper in ["output", "data", "response", "answer"] {
            let Some(value) = object.get(wrapper) else {
                continue;
            };
            let nested = match value {
                serde_json::Value::Object(nested) => Some(nested.clone()),
                serde_json::Value::String(encoded) => parse_first_contract_json(encoded)
                    .and_then(|value: serde_json::Value| value.as_object().cloned()),
                _ => None,
            };
            if let Some(object) = nested.and_then(&contract_object) {
                return Some(object);
            }
        }
    }
    let embedded_contract = text
        .char_indices()
        .filter(|(_, character)| *character == '{')
        .filter_map(|(start, _)| parse_first_contract_json(&text[start..]))
        .filter_map(|value| value.as_object().cloned())
        // An agent may quote an upstream JSON result before returning its own
        // terminal object. The terminal contract is the last matching object,
        // while exact whole-response JSON was already handled above.
        .filter_map(&contract_object)
        // Nested rows inside the terminal object can individually match a
        // contract field (for example an `unresolved_or_risks` item shaped as
        // `{"id","title","mitigation"}`). Prefer the outermost terminal
        // object: it carries the primary `summary` field and the largest
        // field set, so a quoted or nested fragment never wins.
        .max_by_key(|object| (object.contains_key("summary"), object.len()));

    // Some providers occasionally honor the requested field names but return
    // exact level-two Markdown sections instead of JSON. Normalize only those
    // explicit contract headings; arbitrary prose remains non-structured.
    // Runtime acceptance still requires independent tool/change receipts, so
    // this cannot turn a self-reported review into verified evidence.
    let mut object = serde_json::Map::new();
    let mut active_field: Option<&str> = None;
    let mut active_lines = Vec::new();
    let flush = |object: &mut serde_json::Map<String, serde_json::Value>,
                 field: Option<&str>,
                 lines: &mut Vec<&str>| {
        if let Some(field) = field {
            let value = lines.join("\n").trim().to_string();
            if !value.is_empty() {
                object.insert(field.to_string(), serde_json::Value::String(value));
            }
        }
        lines.clear();
    };
    for line in text.lines() {
        let trimmed = line.trim();
        let heading = trimmed
            .strip_prefix('#')
            .map(|value| value.trim_start_matches('#').trim())
            .or_else(|| {
                trimmed
                    .strip_prefix("**")
                    .and_then(|value| value.strip_suffix("**"))
                    .map(|value| value.trim_end_matches(':').trim())
            });
        if let Some(heading) = heading {
            flush(&mut object, active_field, &mut active_lines);
            // Providers commonly render a requested label as a bold heading
            // (`**Field: summary**`) rather than a bare heading.  `Field:`
            // is presentation syntax, not part of the allow-listed field.
            let heading = heading.trim();
            let heading = heading
                .strip_prefix("Field:")
                .or_else(|| heading.strip_prefix("field:"))
                .unwrap_or(heading)
                .trim();
            let normalized = heading.to_ascii_lowercase().replace([' ', '-'], "_");
            active_field = CONTRACT_FIELDS
                .iter()
                .copied()
                .find(|field| *field == normalized)
                .or_else(|| match normalized.as_str() {
                    "conclusion" | "result" | "摘要" | "总结" => Some("summary"),
                    "finding" | "observations" | "发现" => Some("findings"),
                    "proof" | "证据" => Some("evidence"),
                    "open_questions" | "gaps" | "未解决" => Some("unresolved"),
                    "risk" | "风险" => Some("risks"),
                    _ => None,
                });
        } else if let Some((label, value)) = trimmed.split_once(':') {
            let normalized = label
                .trim()
                .trim_start_matches(['-', '*'])
                .trim()
                .to_ascii_lowercase()
                .replace([' ', '-'], "_");
            let field = CONTRACT_FIELDS
                .iter()
                .copied()
                .find(|field| *field == normalized)
                .or_else(|| match normalized.as_str() {
                    "conclusion" | "result" | "摘要" | "总结" => Some("summary"),
                    "finding" | "observations" | "发现" => Some("findings"),
                    "proof" | "证据" => Some("evidence"),
                    "open_questions" | "gaps" | "未解决" => Some("unresolved"),
                    "risk" | "风险" => Some("risks"),
                    _ => None,
                });
            if let Some(field) = field {
                flush(&mut object, active_field, &mut active_lines);
                active_field = Some(field);
                if !value.trim().is_empty() {
                    active_lines.push(value.trim());
                }
            } else if active_field.is_some() {
                active_lines.push(line);
            }
        } else if active_field.is_some() {
            active_lines.push(line);
        }
    }
    flush(&mut object, active_field, &mut active_lines);
    // An explicit outer presentation contract is more authoritative than a
    // JSON example embedded in its findings. This matters especially when an
    // Agent is reviewing parsers, protocol fixtures, or data files whose
    // source text legitimately contains allow-listed contract keys. Exact
    // whole-response JSON and supported envelopes were already handled above;
    // embedded JSON remains the final compatibility fallback.
    (!object.is_empty()).then_some(object).or(embedded_contract)
}

/// Parse the fixed Team presentation contract plus any exact, Runtime-declared
/// artifact names for this role. Custom names remain closed by default: an
/// arbitrary JSON key or prose label is accepted only when it appears in the
/// immutable role contract passed by Runtime.
pub(crate) fn structured_agent_output_for_fields(
    text: &str,
    required: &[String],
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut output = structured_agent_output(text).unwrap_or_default();
    if required.is_empty() {
        return (!output.is_empty()).then_some(output);
    }

    let required_name = |candidate: &str| {
        let normalized = candidate
            .trim()
            .trim_end_matches(':')
            .trim()
            .to_ascii_lowercase()
            .replace([' ', '-'], "_");
        required
            .iter()
            .find(|field| field.to_ascii_lowercase() == normalized)
            .cloned()
    };
    let mut merge_required = |object: &serde_json::Map<String, serde_json::Value>| {
        for required_field in required {
            if output.contains_key(required_field) {
                continue;
            }
            if let Some((_, value)) = object
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(required_field))
            {
                output.insert(required_field.clone(), value.clone());
            }
        }
    };

    // Exact JSON and the four supported provider envelopes may carry a custom
    // artifact. Do not recursively trust arbitrary nested objects.
    if let Some(serde_json::Value::Object(object)) = parse_first_contract_json(text) {
        merge_required(&object);
        for wrapper in ["output", "data", "response", "answer"] {
            let Some(value) = object.get(wrapper) else {
                continue;
            };
            match value {
                serde_json::Value::Object(nested) => merge_required(nested),
                serde_json::Value::String(encoded) => {
                    if let Some(serde_json::Value::Object(nested)) =
                        parse_first_contract_json(encoded)
                    {
                        merge_required(&nested);
                    }
                }
                _ => {}
            }
        }
    }

    // Providers also commonly use exact Markdown sections. Unlike the fixed
    // parser, this scanner treats non-contract subheadings as content so a
    // rich custom report can contain its own hierarchy and tables.
    let mut custom = serde_json::Map::new();
    let mut active_field: Option<String> = None;
    let mut active_lines = Vec::new();
    let flush = |object: &mut serde_json::Map<String, serde_json::Value>,
                 field: &mut Option<String>,
                 lines: &mut Vec<&str>| {
        if let Some(field) = field.take() {
            let value = lines.join("\n").trim().to_string();
            if !value.is_empty() {
                object.insert(field, serde_json::Value::String(value));
            }
        }
        lines.clear();
    };
    for line in text.lines() {
        let trimmed = line.trim();
        let heading = trimmed
            .strip_prefix('#')
            .map(|value| value.trim_start_matches('#').trim())
            .or_else(|| {
                trimmed
                    .strip_prefix("**")
                    .and_then(|value| value.strip_suffix("**"))
                    .map(str::trim)
            });
        let labeled = heading.map(|label| (label, None)).or_else(|| {
            trimmed
                .split_once(':')
                .map(|(label, value)| (label, Some(value)))
        });
        if let Some((label, inline_value)) = labeled {
            let label = label.trim().trim_start_matches(['-', '*']).trim();
            if let Some(field) = required_name(label) {
                flush(&mut custom, &mut active_field, &mut active_lines);
                active_field = Some(field);
                if let Some(value) = inline_value.filter(|value| !value.trim().is_empty()) {
                    active_lines.push(value.trim());
                }
                continue;
            }
        }
        if active_field.is_some() {
            active_lines.push(line);
        }
    }
    flush(&mut custom, &mut active_field, &mut active_lines);
    merge_required(&custom);

    (!output.is_empty()).then_some(output)
}
