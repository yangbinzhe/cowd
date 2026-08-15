import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const checkMode = process.argv.includes("--check");
function emit(path, value) {
  const bytes = `${JSON.stringify(value, null, 2)}\n`;
  if (checkMode) {
    if (readFileSync(path, "utf8") !== bytes) throw new Error(`generated contract drift: ${path}`);
  } else {
    writeFileSync(path, bytes);
  }
}
const repositoryPath = join(here, "legacy-schema.rs.txt");
const repository = readFileSync(repositoryPath, "utf8");

const mfg = new Set([
  "mfg_cockpit_profile", "mfg_cockpit_view_draft", "mfg_cockpit_view_proposal",
  "mfg_cockpit_view_version", "mfg_cockpit_view_active", "mfg_cockpit_report",
  "mfg_report_delivery_review", "mfg_report_delivery_review_transition",
  "mfg_report_delivery_review_effect_outbox", "mfg_alert_rule", "mfg_alert_occurrence",
  "mfg_alert_subscription", "mfg_assignment", "mfg_command_receipt",
  "mfg_mutation_receipt", "mfg_mutation_receipt_alias",
  "mfg_mutation_receipt_repair_report", "mfg_incident", "mfg_operational_analysis",
  "mfg_action_execution", "mfg_memory_case", "mfg_playbook", "mfg_skill_execution",
  "mfg_workflow_graph",
]);
const excluded = new Set(["mfg_projection_event", "mfg_live_epoch", "mfg_live_secret"]);

function migrationTables() {
  const body = repository.match(/const MFG_MIGRATION_TABLES: &\[&str\] = &\[([\s\S]*?)\n\];/);
  if (!body) throw new Error("MFG_MIGRATION_TABLES not found");
  return [...body[1].matchAll(/"([^"]+)"/g)].map((match) => match[1]);
}

function splitSqlList(body) {
  const parts = [];
  let depth = 0;
  let start = 0;
  for (let index = 0; index < body.length; index += 1) {
    if (body[index] === "(") depth += 1;
    if (body[index] === ")") depth -= 1;
    if (body[index] === "," && depth === 0) {
      parts.push(body.slice(start, index).trim());
      start = index + 1;
    }
  }
  parts.push(body.slice(start).trim());
  return parts;
}

function ddlTables() {
  const tables = new Map();
  const batch = repository.slice(repository.indexOf('connection.execute_batch(\n        r"'));
  const pattern = /CREATE TABLE IF NOT EXISTS\s+(\w+)\s*\(([\s\S]*?)\n\s{8}\);/g;
  for (const match of batch.matchAll(pattern)) {
    const [, name, body] = match;
    const columns = [];
    let tablePrimaryKey = [];
    for (const part of splitSqlList(body)) {
      const normalized = part.replace(/\s+/g, " ").trim();
      const tablePk = normalized.match(/^PRIMARY KEY\(([^)]+)\)/i);
      if (tablePk) {
        tablePrimaryKey = tablePk[1].split(",").map((value) => value.trim());
        continue;
      }
      if (/^(UNIQUE|FOREIGN KEY|CHECK)\b/i.test(normalized)) continue;
      const column = normalized.match(/^(\w+)\s+(\w+)([\s\S]*)$/);
      if (!column) throw new Error(`cannot parse ${name}: ${normalized}`);
      columns.push({
        name: column[1],
        sql_type: column[2].toUpperCase(),
        nullable: !/\bNOT NULL\b/i.test(column[3]) && !/\bPRIMARY KEY\b/i.test(column[3]),
        source_default: column[3].match(/\bDEFAULT\s+([^ ]+)/i)?.[1] ?? null,
        inline_primary_key: /\bPRIMARY KEY\b/i.test(column[3]),
      });
    }
    const primaryKey = tablePrimaryKey.length
      ? tablePrimaryKey
      : columns.filter((column) => column.inline_primary_key).map((column) => column.name);
    tables.set(name, { name, primary_key: primaryKey, columns });
  }
  return tables;
}

const migration = migrationTables();
const ddl = ddlTables();
const core = new Set(migration.filter((table) => table.startsWith("matrix_")));
if (migration.length !== 46 || mfg.size !== 24 || core.size !== 19 || excluded.size !== 3) {
  throw new Error("ownership cardinality changed");
}
for (const table of migration) {
  if (!ddl.has(table)) throw new Error(`missing DDL for ${table}`);
  const memberships = [mfg, core, excluded].filter((set) => set.has(table)).length;
  if (memberships !== 1) throw new Error(`ownership is not exclusive for ${table}`);
}

function ownerOf(table) {
  if (mfg.has(table)) return "mfg";
  if (core.has(table)) return "core";
  return "excluded";
}

function targetOf(table, owner) {
  if (owner === "excluded") return null;
  return `${owner}.${table}`;
}

const tableOwnership = {
  contract: "cowd.ownership-split/v1.2-final",
  source: {
    repository: "crates/app-mfg-core/src/repository.rs",
    table_inventory: "MFG_MIGRATION_TABLES",
    schema_ddl: "initialize_schema",
  },
  expected_counts: { total: 46, mfg: 24, core: 19, excluded: 3 },
  tables: migration.map((table) => {
    const owner = ownerOf(table);
    const definition = ddl.get(table);
    const revisionFields = definition.columns
      .map((column) => column.name)
      .filter((column) => column === "revision" || column.endsWith("_revision"));
    return {
      source_table: table,
      owner,
      target: targetOf(table, owner),
      disposition: owner === "excluded" ? "regenerate" : owner === "core" ? "typed_import" : "canonical_import",
      primary_key: definition.primary_key,
      stable_id: { fields: definition.primary_key, required: true },
      revision: { fields: revisionFields, embedded_revision_must_be_preserved: true },
      conflict: owner === "excluded"
        ? "never_import"
        : revisionFields.length
          ? "compare_revision_then_reject_divergence"
          : "reject_duplicate_stable_id_unless_canonical_payload_equal",
      evidence: `crates/app-mfg-core/src/repository.rs::initialize_schema::${table}`,
    };
  }),
  metadata: [{
    source_table: "matrix_schema",
    owner: "excluded",
    disposition: "delete_after_verified_cutover",
    replacement: "target-owned schema migration journals",
    precondition: "both target stores report their own accepted schema revision and contract digest",
    evidence: "crates/app-mfg-core/src/repository.rs::initialize_schema::matrix_schema",
  }],
};

function roleFor(table, column, primaryKey) {
  const roles = [];
  const isStableId = primaryKey.includes(column);
  if (isStableId) roles.push("stable_id");
  if (column === "revision" || column.endsWith("_revision")) roles.push("revision");
  if (column.endsWith("_json")) roles.push("structured_payload");
  if (/(^|_)(evidence|receipt|source)(_id|_ref)?$/.test(column) || column.includes("evidence")) roles.push("evidence_or_provenance");
  if (!isStableId && (column.endsWith("_id") || column.endsWith("_ref"))) roles.push("reference_candidate");
  if (column.endsWith("_at")) roles.push("timestamp");
  return roles.length ? roles : ["value"];
}

const fieldMapping = {
  contract: "cowd.ownership-fields/v1",
  policy: {
    unknown_table: "reject",
    unknown_column: "reject",
    missing_column: "reject",
    implicit_default: "forbidden",
    one_source_multiple_owner: "reject",
    json_decode_error: "reject",
  },
  tables: Object.fromEntries(migration.map((table) => {
    const definition = ddl.get(table);
    const owner = ownerOf(table);
    const orderBy = definition.primary_key.map((field) => `"${field}"`).join(", ");
    return [table, {
      owner,
      target: targetOf(table, owner),
      fields: Object.fromEntries(definition.columns.map((column) => [column.name, {
        owner,
        target: owner === "excluded" ? null : `${targetOf(table, owner)}.${column.name}`,
        target_aggregate: owner === "excluded" ? null : owner === "core" ? "core_matrix_domain" : "mfg_domain",
        target_table: owner === "excluded" ? null : table,
        target_field: owner === "excluded" ? null : column.name,
        stable_id: definition.primary_key.includes(column.name),
        revision: column.name === "revision" || column.name.endsWith("_revision"),
        source: `sqlite.${table}.${column.name}`,
        source_table: table,
        source_column: column.name,
        source_type: column.sql_type,
        source_nullable: column.nullable,
        source_reference_mapping: column.name.includes("source")
          ? "preserve_and_validate_provenance"
          : "not_a_source_reference",
        evidence_reference_mapping: column.name.includes("evidence") || column.name.includes("receipt")
          || (table === "mfg_mutation_receipt" && column.name === "response_json")
          ? "preserve_and_reconcile_reference"
          : "not_an_evidence_reference",
        evidence: `crates/app-mfg-core/src/repository.rs::initialize_schema::${table}.${column.name}`,
        roles: roleFor(table, column.name, definition.primary_key),
        transform: owner === "excluded"
          ? "discard_and_regenerate"
          : column.name.endsWith("_json")
            ? "parse_validate_and_emit_canonical_json"
            : "lossless_typed_value",
        conflict: owner === "excluded"
          ? "source_value_must_not_be_imported"
          : "reject_non_identical_value_for_same_stable_id_and_revision",
        no_default: true,
        source_default_observed: column.source_default,
        nullable: column.nullable,
        semantic_query: `SELECT "${column.name}" FROM "${table}" ORDER BY ${orderBy}`,
      }])),
    }];
  })),
};

const externalTargets = {
  "core.task": { boundary: "Cowd Core task contract", key: "task_id" },
  "core.principal": { boundary: "Cowd Core identity contract", key: "principal_ref" },
  "core.approval": { boundary: "Cowd Core approval contract", key: "approval_id" },
  "core.lease": { boundary: "Cowd Core lease contract", key: "lease_ref" },
  "core.receipt": { boundary: "Cowd Core receipt contract", key: "receipt_ref" },
  "core.runtime_execution": { boundary: "Cowd Core runtime execution contract", key: "execution_ref" },
  "core.entity_reference": { boundary: "Cowd Core entity reference resolver", key: "entity_ref" },
  "core.metric_reference": { boundary: "Cowd Core metric reference resolver", key: "metric_ref" },
  "evidence.reference": { boundary: "versioned evidence/provenance resolver", key: "evidence_ref" },
  "surface.destination": { boundary: "Cowd Surface notification contract", key: "destination_ref" },
};

const edges = [];
function column(sourceTable, sourceField, targetTable, targetField, evidence, options = {}) {
  edges.push({
    source: { table: sourceTable, field: sourceField },
    target: targetTable ? { table: targetTable, field: targetField } : { external: options.external },
    kind: options.kind ?? "semantic_column_reference",
    cardinality: options.cardinality ?? "zero_or_one",
    resolution: options.resolution ?? "identity",
    on_dangling: "reject_snapshot",
    evidence,
  });
}
function json(sourceTable, sourceField, path, targetTable, targetField, evidence, options = {}) {
  edges.push({
    source: { table: sourceTable, field: sourceField, json_path: path },
    target: targetTable ? { table: targetTable, field: targetField, json_path: options.targetPath } : { external: options.external },
    kind: "hidden_json_reference",
    cardinality: options.cardinality ?? "zero_or_many",
    resolution: options.resolution ?? "identity",
    on_dangling: "reject_snapshot",
    evidence,
  });
}

// Physical and semantic columns whose relationships are demonstrated by DDL or typed repository models.
column("mfg_cockpit_view_draft", "profile_id", "mfg_cockpit_profile", "profile_id", "cockpit.rs::MfgCockpitDraft");
column("mfg_cockpit_view_proposal", "profile_id", "mfg_cockpit_profile", "profile_id", "cockpit_proposal.rs::MfgCockpitProposal");
column("mfg_cockpit_view_proposal", "task_id", null, null, "cockpit_proposal.rs::MfgCockpitProposal::task_id", { external: "core.task" });
column("mfg_cockpit_view_version", "profile_id", "mfg_cockpit_profile", "profile_id", "cockpit.rs::MfgCockpitVersion");
column("mfg_cockpit_view_active", "profile_id", "mfg_cockpit_profile", "profile_id", "repository.rs::publish_cockpit_draft");
column("mfg_cockpit_report", "profile_id", "mfg_cockpit_profile", "profile_id", "cockpit.rs::MfgCockpitReportSnapshot");
column("mfg_report_delivery_review", "report_id", "mfg_cockpit_report", "report_id", "review.rs::MfgReportDeliveryReview");
column("mfg_report_delivery_review", "approval_id", null, null, "review.rs::MfgReportDeliveryReview::approval_id", { external: "core.approval" });
column("mfg_report_delivery_review", "decision_lease_ref", null, null, "review.rs::MfgReportDeliveryReview::decision_lease_ref", { external: "core.lease" });
column("mfg_report_delivery_review", "effect_receipt_ref", null, null, "review.rs::MfgReportDeliveryReview::effect_receipt_ref", { external: "core.receipt" });
column("mfg_report_delivery_review_transition", "review_id", "mfg_report_delivery_review", "review_id", "repository.rs::insert_report_delivery_review_transition");
column("mfg_report_delivery_review_effect_outbox", "review_id", "mfg_report_delivery_review", "review_id", "review.rs::MfgReportDeliveryReviewEffect");
column("mfg_report_delivery_review_effect_outbox", "receipt_ref", null, null, "review.rs::MfgReportDeliveryReviewEffect::receipt_ref", { external: "core.receipt" });
column("mfg_alert_occurrence", "rule_id", "mfg_alert_rule", "rule_id", "operations.rs::MfgAlertOccurrence");
column("mfg_alert_subscription", "rule_id", "mfg_alert_rule", "rule_id", "operations.rs::MfgAlertSubscription");
column("mfg_assignment", "workflow_id", "mfg_workflow_graph", "workflow_id", "operations.rs::MfgAssignment");
column("mfg_assignment", "incident_id", "mfg_incident", "incident_id", "operations.rs::MfgAssignment");
column("mfg_assignment", "task_ref", null, null, "operations.rs::MfgAssignment::task_ref", { external: "core.task" });
column("mfg_mutation_receipt_alias", "receipt_id", "mfg_mutation_receipt", "receipt_id", "repository.rs::initialize_schema FOREIGN KEY");
column("matrix_entity_source_key", "entity_id", "matrix_entity", "entity_id", "repository.rs::initialize_schema FOREIGN KEY");
column("matrix_relation", "from_entity_id", "matrix_entity", "entity_id", "repository.rs::initialize_schema FOREIGN KEY");
column("matrix_relation", "to_entity_id", "matrix_entity", "entity_id", "repository.rs::initialize_schema FOREIGN KEY");
column("matrix_fact", "metric_key", "matrix_metric_definition", "metric_id", "repository.rs::recompute_metrics_for_fact_type");
column("matrix_evidence_packet", "attention_id", "matrix_attention_item", "attention_id", "repository.rs::build_evidence_packet");
column("matrix_metric_state", "metric_id", "matrix_metric_definition", "metric_id", "repository.rs::upsert_metric_state");
column("matrix_metric_dependency", "upstream_metric_id", "matrix_metric_definition", "metric_id", "repository.rs::metric_lineage");
column("matrix_metric_dependency", "downstream_metric_id", "matrix_metric_definition", "metric_id", "repository.rs::metric_lineage");
column("matrix_change_event", "metric_id", "matrix_metric_definition", "metric_id", "repository.rs::recompute_metrics_for_fact_type");
column("mfg_incident", "attention_id", "matrix_attention_item", "attention_id", "incident.rs::MfgIncident");
column("mfg_incident", "evidence_packet_id", "matrix_evidence_packet", "packet_id", "incident.rs::MfgIncident");
column("mfg_incident", "task_id", null, null, "incident.rs::MfgIncident::task_id", { external: "core.task" });
column("mfg_incident", "workflow_graph_id", "mfg_workflow_graph", "workflow_id", "incident.rs::MfgIncident");
column("mfg_operational_analysis", "incident_id", "mfg_incident", "incident_id", "analysis.rs::MfgOperationalAnalysis");
column("mfg_operational_analysis", "evidence_packet_id", "matrix_evidence_packet", "packet_id", "analysis.rs::MfgOperationalAnalysis");
column("mfg_action_execution", "analysis_id", "mfg_operational_analysis", "analysis_id", "execution.rs::MfgActionExecution");
column("mfg_action_execution", "incident_id", "mfg_incident", "incident_id", "execution.rs::MfgActionExecution");
column("mfg_memory_case", "incident_id", "mfg_incident", "incident_id", "memory_case.rs::MfgMemoryCase");
column("matrix_connector_run", "source_pack_id", "matrix_source_pack", "source_pack_id", "repository.rs::start_connector_run");
column("matrix_entity_match_candidate", "left_entity_id", "matrix_entity", "entity_id", "repository.rs::propose_entity_match");
column("matrix_entity_match_candidate", "right_entity_id", "matrix_entity", "entity_id", "repository.rs::propose_entity_match");
column("matrix_entity_conflict_decision", "candidate_id", "matrix_entity_match_candidate", "candidate_id", "repository.rs::resolve_entity_conflict");
column("matrix_entity_conflict_decision", "survivor_entity_id", "matrix_entity", "entity_id", "repository.rs::resolve_entity_conflict");
column("matrix_entity_conflict_decision", "retired_entity_id", "matrix_entity", "entity_id", "repository.rs::resolve_entity_conflict");
column("mfg_skill_execution", "incident_id", "mfg_incident", "incident_id", "skill.rs::MfgSkillRun");
column("mfg_workflow_graph", "incident_id", "mfg_incident", "incident_id", "workflow.rs::MfgWorkflowGraph");
column("mfg_workflow_graph", "task_id", null, null, "workflow.rs::MfgWorkflowGraph::task_id", { external: "core.task" });

// References hidden inside canonical JSON payloads; each path is backed by the named serialized Rust field.
json("mfg_cockpit_profile", "profile_json", "$.owner_ref", null, null, "cockpit.rs::MfgCockpitProfile::owner_ref", { external: "core.principal" });
json("mfg_cockpit_profile", "profile_json", "$.focus_refs[*]", null, null, "cockpit.rs::MfgCockpitProfile::focus_refs", { external: "core.entity_reference" });
json("mfg_cockpit_profile", "profile_json", "$.focus_metric_ids[*]", null, null, "cockpit.rs::MfgCockpitProfile::focus_metric_ids", { external: "core.metric_reference" });
json("mfg_cockpit_view_draft", "draft_json", "$.profile_id", "mfg_cockpit_profile", "profile_id", "cockpit.rs::MfgCockpitDraft::profile_id");
json("mfg_cockpit_view_draft", "draft_json", "$.actor_ref", null, null, "cockpit.rs::MfgCockpitDraft::actor_ref", { external: "core.principal" });
json("mfg_cockpit_view_proposal", "proposal_json", "$.profile_id", "mfg_cockpit_profile", "profile_id", "cockpit_proposal.rs::MfgCockpitProposal::profile_id");
json("mfg_cockpit_view_proposal", "proposal_json", "$.actor_ref", null, null, "cockpit_proposal.rs::MfgCockpitProposal::actor_ref", { external: "core.principal" });
json("mfg_cockpit_view_proposal", "proposal_json", "$.task_id", null, null, "cockpit_proposal.rs::MfgCockpitProposal::task_id", { external: "core.task" });
json("mfg_cockpit_view_version", "version_json", "$.profile_id", "mfg_cockpit_profile", "profile_id", "cockpit.rs::MfgCockpitVersion::profile_id");
json("mfg_cockpit_view_version", "version_json", "$.actor_ref", null, null, "cockpit.rs::MfgCockpitVersion::actor_ref", { external: "core.principal" });
json("mfg_cockpit_report", "report_json", "$.profile_id", "mfg_cockpit_profile", "profile_id", "cockpit.rs::MfgCockpitReportSnapshot::profile_id");
json("mfg_cockpit_report", "report_json", "$.owner_ref", null, null, "cockpit.rs::MfgCockpitReportSnapshot::owner_ref", { external: "core.principal" });
json("mfg_cockpit_report", "report_json", "$.delivery_receipts[*].cross_plane_receipt_id", null, null, "cockpit.rs::MfgCockpitReportDeliveryReceipt::cross_plane_receipt_id", { external: "core.receipt" });
json("mfg_report_delivery_review", "review_json", "$.report_id", "mfg_cockpit_report", "report_id", "review.rs::MfgReportDeliveryReview::report_id");
json("mfg_report_delivery_review", "review_json", "$.requester_principal", null, null, "review.rs::MfgReportDeliveryReview::requester_principal", { external: "core.principal" });
json("mfg_report_delivery_review", "review_json", "$.reviewer_principal", null, null, "review.rs::MfgReportDeliveryReview::reviewer_principal", { external: "core.principal" });
json("mfg_report_delivery_review", "review_json", "$.approval_id", null, null, "review.rs::MfgReportDeliveryReview::approval_id", { external: "core.approval" });
json("mfg_report_delivery_review", "review_json", "$.decision_lease_ref", null, null, "review.rs::MfgReportDeliveryReview::decision_lease_ref", { external: "core.lease" });
json("mfg_report_delivery_review", "review_json", "$.effect_receipt_ref", null, null, "review.rs::MfgReportDeliveryReview::effect_receipt_ref", { external: "core.receipt" });
json("mfg_report_delivery_review", "review_json", "$.evidence_refs[*]", null, null, "review.rs::MfgReportDeliveryReview::evidence_refs", { external: "evidence.reference" });
json("mfg_alert_rule", "rule_json", "$.metric_refs[*]", null, null, "operations.rs::MfgAlertRule::metric_refs", { external: "core.metric_reference" });
json("mfg_alert_rule", "rule_json", "$.entity_refs[*]", null, null, "operations.rs::MfgAlertRule::entity_refs", { external: "core.entity_reference" });
json("mfg_alert_occurrence", "occurrence_json", "$.attention_ref", "matrix_attention_item", "attention_id", "repository.rs::evaluate_alert_rule emits matrix:attention:<attention_id>", { resolution: "strip_uri_prefix:matrix:attention:" });
json("mfg_alert_occurrence", "occurrence_json", "$.incident_ref", "mfg_incident", "incident_id", "operations.rs::MfgAlertOccurrence::incident_ref");
json("mfg_alert_occurrence", "occurrence_json", "$.evidence_refs[*]", null, null, "operations.rs::MfgAlertOccurrence::evidence_refs", { external: "evidence.reference" });
json("mfg_alert_subscription", "subscription_json", "$.rule_id", "mfg_alert_rule", "rule_id", "operations.rs::MfgAlertSubscription::rule_id");
json("mfg_assignment", "assignment_json", "$.workflow_id", "mfg_workflow_graph", "workflow_id", "operations.rs::MfgAssignment::workflow_id");
json("mfg_assignment", "assignment_json", "$.incident_id", "mfg_incident", "incident_id", "operations.rs::MfgAssignment::incident_id");
json("mfg_assignment", "assignment_json", "$.task_ref", null, null, "operations.rs::MfgAssignment::task_ref", { external: "core.task" });
json("matrix_fact", "entity_refs_json", "$[*]", null, null, "repository.rs::list_facts decodes Vec<String> as entity_refs", { external: "core.entity_reference" });
json("matrix_relation", "relation_json", "$.from_entity_id", "matrix_entity", "entity_id", "repository.rs::upsert_relation serializes MatrixRelation.from_entity_id");
json("matrix_relation", "relation_json", "$.to_entity_id", "matrix_entity", "entity_id", "repository.rs::upsert_relation serializes MatrixRelation.to_entity_id");
json("matrix_attention_item", "attention_json", "$.entity_ref", null, null, "repository.rs::attention_from_change::MatrixAttentionItem.entity_ref", { external: "core.entity_reference" });
json("matrix_attention_item", "attention_json", "$.metric_refs[*]", null, null, "repository.rs::attention_from_change::MatrixAttentionItem.metric_refs", { external: "core.metric_reference" });
json("matrix_attention_item", "attention_json", "$.linked_changes[*]", "matrix_change_event", "change_id", "repository.rs::attention_from_change emits matrix:change:<change_id>", { resolution: "strip_uri_prefix:matrix:change:" });
json("matrix_evidence_packet", "packet_json", "$.attention_id", "matrix_attention_item", "attention_id", "repository.rs::build_evidence_packet_transaction::packet.attention_id");
json("matrix_evidence_packet", "packet_json", "$.business_context.entity_ref", null, null, "repository.rs::build_evidence_packet_transaction::business_context.entity_ref", { external: "core.entity_reference" });
json("matrix_evidence_packet", "packet_json", "$.source_refs[*].reference", null, null, "repository.rs::build_evidence_packet_transaction::MatrixEvidenceSourceRef.reference", { external: "evidence.reference" });
json("matrix_metric_state", "state_json", "$.metric_id", "matrix_metric_definition", "metric_id", "repository.rs::upsert_metric_state serializes MatrixMetricState");
json("matrix_metric_state", "state_json", "$.entity_scope", null, null, "repository.rs::MatrixMetricState.entity_scope", { external: "core.entity_reference" });
json("matrix_metric_dependency", "dependency_json", "$.upstream_metric_id", "matrix_metric_definition", "metric_id", "repository.rs::upsert_metric_dependency::upstream_metric_id");
json("matrix_metric_dependency", "dependency_json", "$.downstream_metric_id", "matrix_metric_definition", "metric_id", "repository.rs::upsert_metric_dependency::downstream_metric_id");
json("matrix_change_event", "change_json", "$.metric_id", "matrix_metric_definition", "metric_id", "repository.rs::insert_change_event serializes MatrixChangeEvent");
json("matrix_change_event", "change_json", "$.entity_ref", null, null, "repository.rs::MatrixChangeEvent.entity_ref", { external: "core.entity_reference" });
json("matrix_metric_snapshot", "metric_ids_json", "$[*]", "matrix_metric_definition", "metric_id", "repository.rs::insert_metric_snapshot");
json("matrix_metric_snapshot", "snapshot_json", "$.metric_ids[*]", "matrix_metric_definition", "metric_id", "repository.rs::build_metric_snapshot::metric_ids");
json("matrix_metric_snapshot", "snapshot_json", "$.items[*].metric_id", "matrix_metric_definition", "metric_id", "repository.rs::build_metric_snapshot::MatrixMetricSnapshotItem.metric_id");
json("matrix_connector_run", "run_json", "$.source_pack_id", "matrix_source_pack", "source_pack_id", "repository.rs::insert_connector_run::MatrixConnectorRun.source_pack_id");
json("matrix_entity_match_candidate", "candidate_json", "$.left_entity_id", "matrix_entity", "entity_id", "repository.rs::insert_entity_match_candidate::left_entity_id");
json("matrix_entity_match_candidate", "candidate_json", "$.right_entity_id", "matrix_entity", "entity_id", "repository.rs::insert_entity_match_candidate::right_entity_id");
json("matrix_entity_conflict_decision", "decision_json", "$.candidate_id", "matrix_entity_match_candidate", "candidate_id", "repository.rs::insert_entity_conflict_decision::candidate_id");
json("matrix_entity_conflict_decision", "decision_json", "$.survivor_entity_id", "matrix_entity", "entity_id", "repository.rs::insert_entity_conflict_decision::survivor_entity_id");
json("matrix_entity_conflict_decision", "decision_json", "$.retired_entity_id", "matrix_entity", "entity_id", "repository.rs::insert_entity_conflict_decision::retired_entity_id");
json("mfg_incident", "incident_json", "$.attention_id", "matrix_attention_item", "attention_id", "incident.rs::MfgIncident::attention_id");
json("mfg_incident", "incident_json", "$.evidence_packet_id", "matrix_evidence_packet", "packet_id", "incident.rs::MfgIncident::evidence_packet_id");
json("mfg_incident", "incident_json", "$.task_id", null, null, "incident.rs::MfgIncident::task_id", { external: "core.task" });
json("mfg_incident", "incident_json", "$.workflow_graph_id", "mfg_workflow_graph", "workflow_id", "incident.rs::MfgIncident::workflow_graph_id");
json("mfg_operational_analysis", "analysis_json", "$.incident_id", "mfg_incident", "incident_id", "analysis.rs::MfgOperationalAnalysis::incident_id");
json("mfg_operational_analysis", "analysis_json", "$.evidence_packet_id", "matrix_evidence_packet", "packet_id", "analysis.rs::MfgOperationalAnalysis::evidence_packet_id");
json("mfg_operational_analysis", "analysis_json", "$.attribution_candidates[*].metric_id", null, null, "analysis.rs::MfgAttributionCandidate::metric_id", { external: "core.metric_reference" });
json("mfg_operational_analysis", "analysis_json", "$.attribution_candidates[*].entity_ref", null, null, "analysis.rs::MfgAttributionCandidate::entity_ref", { external: "core.entity_reference" });
json("mfg_operational_analysis", "analysis_json", "$.attribution_candidates[*].evidence_refs[*]", null, null, "analysis.rs::MfgAttributionCandidate::evidence_refs", { external: "evidence.reference" });
json("mfg_operational_analysis", "analysis_json", "$.impact_paths[*].evidence_refs[*]", null, null, "analysis.rs::MfgImpactPath::evidence_refs", { external: "evidence.reference" });
json("mfg_operational_analysis", "analysis_json", "$.recommended_actions[*].required_evidence[*]", null, null, "analysis.rs::MfgRecommendedAction::required_evidence", { external: "evidence.reference" });
json("mfg_action_execution", "execution_json", "$.analysis_id", "mfg_operational_analysis", "analysis_id", "execution.rs::MfgActionExecution::analysis_id");
json("mfg_action_execution", "execution_json", "$.incident_id", "mfg_incident", "incident_id", "execution.rs::MfgActionExecution::incident_id");
json("mfg_action_execution", "execution_json", "$.cross_plane_receipts[*].cross_plane_receipt_id", null, null, "execution.rs::MfgCrossPlaneBridgeReceipt::cross_plane_receipt_id", { external: "core.receipt" });
json("mfg_memory_case", "memory_case_json", "$.incident_id", "mfg_incident", "incident_id", "memory_case.rs::MfgMemoryCase::incident_id");
json("mfg_memory_case", "memory_case_json", "$.analysis_id", "mfg_operational_analysis", "analysis_id", "memory_case.rs::MfgMemoryCase::analysis_id");
json("mfg_memory_case", "memory_case_json", "$.evidence_packet_id", "matrix_evidence_packet", "packet_id", "memory_case.rs::MfgMemoryCase::evidence_packet_id");
json("mfg_memory_case", "memory_case_json", "$.playbook_id", "mfg_playbook", "playbook_id", "memory_case.rs::MfgMemoryCase::playbook_id");
json("mfg_memory_case", "memory_case_json", "$.entity_refs[*]", null, null, "memory_case.rs::MfgMemoryCase::entity_refs", { external: "core.entity_reference" });
json("mfg_memory_case", "memory_case_json", "$.metric_keys[*]", null, null, "memory_case.rs::MfgMemoryCase::metric_keys", { external: "core.metric_reference" });
json("mfg_playbook", "playbook_json", "$.created_from_case_id", "mfg_memory_case", "case_id", "memory_case.rs::MfgPlaybook::created_from_case_id");
json("mfg_playbook", "playbook_json", "$.metric_keys[*]", null, null, "memory_case.rs::MfgPlaybook::metric_keys", { external: "core.metric_reference" });
json("mfg_playbook", "playbook_json", "$.required_evidence[*]", null, null, "memory_case.rs::MfgPlaybook::required_evidence", { external: "evidence.reference" });
json("mfg_skill_execution", "execution_json", "$.incident_id", "mfg_incident", "incident_id", "skill.rs::MfgSkillRun::incident_id");
json("mfg_skill_execution", "execution_json", "$.execution_context.attention_id", "matrix_attention_item", "attention_id", "skill.rs::MfgSkillExecutionContext::attention_id");
json("mfg_skill_execution", "execution_json", "$.execution_context.evidence_packet_id", "matrix_evidence_packet", "packet_id", "skill.rs::MfgSkillExecutionContext::evidence_packet_id");
json("mfg_skill_execution", "execution_json", "$.execution_context.analysis_id", "mfg_operational_analysis", "analysis_id", "skill.rs::MfgSkillExecutionContext::analysis_id");
json("mfg_skill_execution", "execution_json", "$.execution_context.evidence_refs[*]", null, null, "skill.rs::MfgSkillExecutionContext::evidence_refs", { external: "evidence.reference" });
json("mfg_skill_execution", "execution_json", "$.execution_context.metric_keys[*]", null, null, "skill.rs::MfgSkillExecutionContext::metric_keys", { external: "core.metric_reference" });
json("mfg_skill_execution", "execution_json", "$.execution_context.entity_refs[*]", null, null, "skill.rs::MfgSkillExecutionContext::entity_refs", { external: "core.entity_reference" });
json("mfg_skill_execution", "execution_json", "$.tool_results[*].evidence_refs[*]", null, null, "skill.rs::MfgSkillToolResult::evidence_refs", { external: "evidence.reference" });
json("mfg_skill_execution", "execution_json", "$.runtime_execution_ref", null, null, "skill.rs::MfgSkillRun::runtime_execution_ref", { external: "core.runtime_execution" });
json("mfg_workflow_graph", "graph_json", "$.incident_id", "mfg_incident", "incident_id", "workflow.rs::MfgWorkflowGraph::incident_id");
json("mfg_workflow_graph", "graph_json", "$.task_id", null, null, "workflow.rs::MfgWorkflowGraph::task_id", { external: "core.task" });
json("mfg_workflow_graph", "graph_json", "$.evidence[*].reference", null, null, "workflow.rs::MfgWorkflowEvidence::reference", { external: "evidence.reference" });

const referenceGraph = {
  contract: "cowd.ownership-references/v1",
  policy: { unknown_reference_kind: "reject", dangling_reference: "reject_snapshot", hidden_json_scan: "required" },
  external_targets: externalTargets,
  edges,
};

const reconcileRules = {
  contract: "cowd.ownership-reconcile/v1",
  invariant: "one source record has exactly one authoritative owner; projections and runtime state are never promoted to authority",
  preflight: [
    "freeze_source_writes_or_acquire_migration_fence",
    "export_all_46_tables_with_explicit_columns_and_no_defaults",
    "verify_source_schema_and_contract_digest",
    "validate_all_json_payloads_and_reference_edges",
  ],
  stages: [
    { order: 1, action: "import_core_owned_tables", tables: migration.filter((table) => core.has(table)), atomic: "core_store_transaction" },
    { order: 2, action: "import_mfg_owned_tables", tables: migration.filter((table) => mfg.has(table)), atomic: "mfg_private_store_transaction" },
    { order: 3, action: "resolve_cross_owner_references", source: "reference-graph.json", on_failure: "rollback_both_unpublished_imports" },
    { order: 4, action: "regenerate_runtime_state", tables: migration.filter((table) => excluded.has(table)), rule: "new_epoch_new_secret_empty_projection_cursor" },
    { order: 5, action: "delete_legacy_matrix_schema_metadata", table: "matrix_schema", precondition: "both_owner_schema_journals_and_semantic_queries_verified" },
    { order: 6, action: "publish_ownership_cutover", precondition: "all_semantic_queries_match_and_source_fence_is_held" },
  ],
  excluded_regeneration: {
    mfg_projection_event: "start_empty_then_rebuild_from_authoritative_events",
    mfg_live_epoch: "create_new_epoch_at_cutover_with_zero_retention_window",
    mfg_live_secret: "generate_new_secret_in_target_secret_store_never_copy_source",
  },
  conflict_resolution: {
    stable_id_collision: "reject_unless_canonical_payload_and_revision_are_identical",
    revision_regression: "reject",
    same_revision_divergence: "reject",
    missing_revision: "preserve_embedded_revision_or_reject_if_owner_requires_revision",
    unknown_table: "reject",
    unknown_column: "reject",
    missing_column: "reject",
    implicit_default: "reject",
    multiple_owners: "reject",
    dangling_reference: "reject",
  },
  semantic_verification: {
    query_source: "field-mapping.json#tables.*.fields.*.semantic_query",
    comparison: "typed_canonical_value_equality",
    excluded_tables: "verify_regeneration_invariants_instead_of_row_equality",
  },
  rollback: {
    before_publish: "discard_unpublished_target_transactions_and_release_source_fence",
    after_publish: "forward_reconcile_only; never silently restore split-brain source ownership",
  },
};

const ownershipSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://cowd.dev/contracts/ownership/ownership-split-v1.schema.json",
  title: "MfgOwnershipSplitSnapshotV1",
  description: "Atomic, fail-closed split snapshot. Importers must additionally enforce table-ownership.json, field-mapping.json, reference-graph.json and reconcile-rules.json.",
  type: "object",
  additionalProperties: false,
  required: ["contract_version", "source", "mfg_domain", "core_matrix_domain", "reconciliation", "excluded", "whole_snapshot_digest"],
  properties: {
    contract_version: { const: "cowd.ownership-split/v1.2-final" },
    source: { "$ref": "#/$defs/source" },
    mfg_domain: { "$ref": "#/$defs/mfgSection" },
    core_matrix_domain: { "$ref": "#/$defs/coreSection" },
    reconciliation: { "$ref": "#/$defs/reconciliation" },
    excluded: {
      type: "array", minItems: 3, maxItems: 3,
      items: { "$ref": "#/$defs/excludedRecord" },
    },
    whole_snapshot_digest: { "$ref": "#/$defs/digest" },
  },
  "$defs": {
    nonEmpty: { type: "string", minLength: 1 },
    digest: { type: "string", pattern: "^sha256:[0-9a-f]{64}$" },
    scalar: { type: ["string", "integer", "number", "boolean", "null"] },
    source: {
      type: "object", additionalProperties: false,
      required: ["app_id", "source_version", "schema_version", "exported_at", "maintenance_fence_id", "ownership_contract_digest"],
      properties: {
        app_id: { const: "mfg" }, source_version: { "$ref": "#/$defs/nonEmpty" }, schema_version: { type: "integer", minimum: 1 },
        exported_at: { type: "string", format: "date-time" }, maintenance_fence_id: { "$ref": "#/$defs/nonEmpty" },
        ownership_contract_digest: { "$ref": "#/$defs/digest" },
      },
    },
    object: {
      type: "object", additionalProperties: false,
      required: ["source_table", "stable_id", "revision", "source_references", "evidence_references", "payload", "payload_digest"],
      properties: {
        source_table: { enum: migration.filter((table) => !excluded.has(table)) },
        stable_id: { type: "object", minProperties: 1, additionalProperties: { "$ref": "#/$defs/scalar" } },
        revision: {
          type: "object", additionalProperties: false, required: ["mapping", "value"],
          properties: { mapping: { enum: ["column", "embedded", "none"] }, value: { "$ref": "#/$defs/scalar" } },
        },
        source_references: { type: "array", uniqueItems: true, items: { "$ref": "#/$defs/nonEmpty" } },
        evidence_references: { type: "array", uniqueItems: true, items: { "$ref": "#/$defs/nonEmpty" } },
        payload: { type: "object" }, payload_digest: { "$ref": "#/$defs/digest" },
      },
    },
    sectionBase: {
      type: "object", additionalProperties: false,
      required: ["owner", "object_count", "section_digest", "objects"],
      properties: {
        owner: { enum: ["mfg", "core"] }, object_count: { type: "integer", minimum: 0 },
        section_digest: { "$ref": "#/$defs/digest" }, objects: { type: "array", items: { "$ref": "#/$defs/object" } },
      },
    },
    mfgSection: { allOf: [{ "$ref": "#/$defs/sectionBase" }, { properties: { owner: { const: "mfg" } } }] },
    coreSection: { allOf: [{ "$ref": "#/$defs/sectionBase" }, { properties: { owner: { const: "core" } } }] },
    reconcileRecord: {
      type: "object", additionalProperties: false, required: ["stable_ref", "payload_digest", "status"],
      properties: { stable_ref: { "$ref": "#/$defs/nonEmpty" }, payload_digest: { "$ref": "#/$defs/digest" }, status: { "$ref": "#/$defs/nonEmpty" } },
    },
    reconciliation: {
      type: "object", additionalProperties: false,
      required: ["pending_outbox", "command_receipts", "mutation_receipts", "set_digest"],
      properties: {
        pending_outbox: { type: "array", items: { "$ref": "#/$defs/reconcileRecord" } },
        command_receipts: { type: "array", items: { "$ref": "#/$defs/reconcileRecord" } },
        mutation_receipts: { type: "array", items: { "$ref": "#/$defs/reconcileRecord" } },
        set_digest: { "$ref": "#/$defs/digest" },
      },
    },
    excludedRecord: {
      type: "object", additionalProperties: false, required: ["source_table", "reason", "regeneration"],
      properties: {
        source_table: { enum: [...excluded] }, reason: { "$ref": "#/$defs/nonEmpty" }, regeneration: { "$ref": "#/$defs/nonEmpty" },
      },
    },
  },
};

const FINAL_CONTRACT_VERSION = "cowd.ownership-split/v1.2-final";
const digestPattern = "^sha256:[0-9a-f]{64}$";

function sourceType(table, field) {
  return ddl.get(table).columns.find((column) => column.name === field)?.sql_type;
}

const specialRevisionRules = {
  mfg_cockpit_view_draft: {
    strategy: "monotonic_authority", authority: { field: "draft_revision", type: "INTEGER", comparison: "unsigned_integer" },
    context: [{ field: "base_revision", type: "INTEGER", role: "optimistic_base_only" }],
  },
  mfg_cockpit_view_version: {
    strategy: "monotonic_authority", authority: { field: "revision", type: "INTEGER", comparison: "unsigned_integer" },
    context: [{ field: "base_revision", type: "INTEGER", role: "draft_base_only" }],
  },
  mfg_report_delivery_review: {
    strategy: "monotonic_authority", authority: { field: "revision", type: "INTEGER", comparison: "unsigned_integer" },
    context: [
      { field: "report_revision", type: "INTEGER", role: "reviewed_report_context" },
      { field: "delivery_revision", type: "INTEGER", role: "reviewed_delivery_context" },
    ],
  },
  mfg_mutation_receipt: {
    strategy: "immutable_payload", authority: null,
    context: [
      { field: "expected_revision", type: "INTEGER", role: "request_precondition_only" },
      { field: "result_revision", type: "INTEGER", role: "result_context_only" },
    ],
  },
};

const revisionProjection = {
  contract: "cowd.ownership-revision-projection/v1.2-final",
  policy: {
    unknown_table: "reject", missing_table: "reject", multiple_authorities: "reject",
    missing_authority: "reject", revision_regression: "reject",
    context_affects_ordering: false,
  },
  tables: Object.fromEntries(migration.map((table) => {
    const revisionFields = ddl.get(table).columns
      .map((column) => column.name)
      .filter((field) => field === "revision" || field.endsWith("_revision"));
    const rule = specialRevisionRules[table] ?? (revisionFields.length === 1
      ? {
          strategy: "monotonic_authority",
          authority: { field: revisionFields[0], type: sourceType(table, revisionFields[0]), comparison: "unsigned_integer" },
          context: [],
        }
      : { strategy: "immutable_payload", authority: null, context: [] });
    return [table, { source_table: table, owner: ownerOf(table), ...rule,
      evidence: `legacy-schema.rs.txt::${table}; field-mapping.json#tables.${table}` }];
  })),
};

const sourceReferenceFields = Object.values(fieldMapping.tables).flatMap((table) =>
  Object.values(table.fields).filter((field) => field.source_reference_mapping !== "not_a_source_reference")
    .map((field) => `${field.source_table}.${field.source_column}`));
const evidenceReferenceFields = Object.values(fieldMapping.tables).flatMap((table) =>
  Object.values(table.fields).filter((field) => field.evidence_reference_mapping !== "not_an_evidence_reference")
    .map((field) => `${field.source_table}.${field.source_column}`));
evidenceReferenceFields.push("mfg_mutation_receipt.response_json");

const typedReferenceSchema = {
  type: "object", additionalProperties: false,
  required: ["namespace", "aggregate", "stable_id", "source"],
  properties: {
    namespace: { type: "string", minLength: 1 }, aggregate: { type: "string", minLength: 1 },
    stable_id: { type: "object", minProperties: 1, additionalProperties: { "$ref": "#/$defs/scalar" } },
    revision: { type: ["integer", "string", "null"] }, digest: { anyOf: [{ "$ref": "#/$defs/digest" }, { type: "null" }] },
    source: {
      type: "object", additionalProperties: false, required: ["table", "field", "json_pointer"],
      properties: { table: { type: "string", minLength: 1 }, field: { type: "string", minLength: 1 }, json_pointer: { type: ["string", "null"] } },
    },
  },
};

const referenceEncoding = {
  contract: "cowd.ownership-reference-encoding/v1.2-final",
  typed_reference: {
    fields: ["namespace", "aggregate", "stable_id", "revision", "digest", "source"],
    optional: ["revision", "digest"], stable_id: "canonical object keyed by target primary-key field",
  },
  canonical_order: "ascending canonical UTF-8 encoded bytes",
  deduplication: "reject duplicate canonical encoded bytes",
  internal_resolution: "every internal reference must resolve within the same complete snapshot",
  external_resolution: "every external reference must resolve against the bound ExternalReferenceCatalogV1",
  external_catalog: {
    contract: "ExternalReferenceCatalogV1", required: ["schema", "digest", "owner", "exported_at", "entries"],
    binding: "source.external_catalog_digest MUST equal catalog.digest",
  },
  source_fields: [...new Set(sourceReferenceFields)].sort(utf8ByteCompare),
  evidence_fields: [...new Set(evidenceReferenceFields)].sort(utf8ByteCompare),
  column_reference_edges: referenceGraph.edges.filter((edge) => !edge.source.json_path),
  json_reference_edges: referenceGraph.edges.filter((edge) => edge.source.json_path),
};

const jsonFields = Object.values(fieldMapping.tables).flatMap((table) => Object.values(table.fields))
  .filter((field) => field.roles.includes("structured_payload"))
  .filter((field) => `${field.source_table}.${field.source_column}` !== "mfg_live_secret.cursor_key_json");
const jsonSchemaRegistry = {
  contract: "cowd.ownership-json-schema-registry/v1.2-final",
  policy: { unknown_schema_id: "reject", missing_schema_id: "reject", invalid_json: "reject", max_encoded_bytes: 16_777_216 },
  fields: Object.fromEntries(jsonFields.map((field) => {
    const key = `${field.source_table}.${field.source_column}`;
    return [key, {
      schema_id: `cowd.ownership-json/${key}/v1`, mode: "opaque_canonical",
      validation: { type: ["object", "array", "string", "number", "integer", "boolean", "null"], valid_json_required: true, canonicalization_required: true },
      evidence: field.evidence,
      opaque_reason: "legacy persisted business payload; source Rust type is evidence but the migration contract must not invent a narrower schema",
    }];
  })),
};

const canonicalization = {
  contract: "cowd.ownership-canonicalization/v1.2-final",
  encoding: "UTF-8", unicode_normalization: "none; preserve scalar values byte-for-byte",
  object_keys: "ascending UTF-8 byte order; duplicate keys reject",
  arrays: "preserve input order except reference arrays, which use reference-encoding canonical order",
  numbers: "shortest RFC 8785-compatible finite JSON representation; reject NaN, Infinity, -Infinity, and negative zero",
  strings: "JSON escaping with no insignificant whitespace", null: "literal null",
  digest: { algorithm: "sha256", representation: "sha256:<lowerhex>" },
  domains: {
    payload_digest: "cowd.ownership.payload.v1\\0 + canonical object with payload_digest removed",
    section_digest: "cowd.ownership.section.v1\\0 + canonical section with section_digest removed",
    reconciliation_digest: "cowd.ownership.reconciliation.v1\\0 + canonical reconciliation object with set_digest removed; typed arrays retained independently",
    whole_snapshot_digest: "cowd.ownership.snapshot.v1\\0 + canonical snapshot with whole_snapshot_digest removed",
    external_catalog_digest: "cowd.ownership.external-catalog.v1\\0 + canonical catalog with digest removed",
  },
};

const reconciliationMapping = {
  contract: "cowd.ownership-reconciliation/v1.2-final",
  ordering: "each typed array ascending by stable_ref canonical UTF-8 bytes; duplicate stable_ref rejects",
  state_classification: {
    pending: ["mfg_report_delivery_review_effect_outbox:pending", "mfg_report_delivery_review_effect_outbox:retry_wait", "mfg_mutation_receipt:accepted", "mfg_mutation_receipt:effect_retryable"],
    started_or_leased: ["mfg_report_delivery_review_effect_outbox:processing", "mfg_mutation_receipt:effect_started", "mfg_mutation_receipt:business_completed"],
    failed: ["mfg_report_delivery_review_effect_outbox:retry_wait"],
    dead_letter: ["mfg_report_delivery_review:delivery_dead_lettered"],
    evidence: "repository.rs claim/fail/complete outbox SQL; mutation durable lease SQL; cockpit.rs delivery_dead_lettered projection",
  },
  arrays: {
    pending_outbox: {
      table: "mfg_report_delivery_review_effect_outbox", statuses: ["pending", "retry_wait", "processing"],
      terminal_excluded_statuses: ["completed"], stable_ref: "effect_id", status: "status",
      payload_digest: "canonical payload_json plus action/effect_key/attempt_count/next_attempt_at/receipt_ref",
    },
    command_receipts: {
      table: "mfg_command_receipt", statuses: ["recorded"], stable_ref: "domain + U+001F + idempotency_key",
      status: "constant:recorded", payload_digest: "canonical receipt_json plus subject_ref",
    },
    mutation_receipts: {
      table: "mfg_mutation_receipt", statuses: ["accepted", "effect_started", "effect_retryable", "business_completed", "preview", "completed"],
      stable_ref: "receipt_id", status: "status", payload_digest: "stored payload_digest must equal canonical response_json envelope digest",
    },
    mutation_receipt_aliases: {
      table: "mfg_mutation_receipt_alias", statuses: ["bound"], stable_ref: "legacy_idempotency_key",
      status: "constant:bound", payload_digest: "digest of canonical resolved mfg_mutation_receipt; dangling receipt_id rejects",
    },
    mutation_receipt_repairs: {
      table: "mfg_mutation_receipt_repair_report", statuses: ["conflict_preserved"], stable_ref: "report_id",
      status: "constant:conflict_preserved", payload_digest: "canonical existing_receipt_json and incoming_receipt_json conflict summary; neither side may be dropped",
    },
  },
};

function utf8ByteCompare(left, right) {
  return Buffer.compare(Buffer.from(left, "utf8"), Buffer.from(right, "utf8"));
}

function canonical(value) {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value) || Object.is(value, -0)) throw new Error("non-canonical number");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  return `{${Object.keys(value).sort(utf8ByteCompare).map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
}

function hashDomain(domain, value) {
  return `sha256:${createHash("sha256").update(`${domain}\0`).update(canonical(value)).digest("hex")}`;
}

function withDigest(value, field, domain) {
  const body = structuredClone(value); delete body[field];
  return { ...value, [field]: hashDomain(domain, body) };
}

const digestVectors = {
  contract: "cowd.ownership-digest-vectors/v1.2-final",
  vectors: [
    { domain: "cowd.ownership.payload.v1", value: { a: 1, b: [true, null, "é"] } },
    { domain: "cowd.ownership.section.v1", value: { owner: "mfg", object_count: 0, objects: [] } },
  ].map((vector) => ({ ...vector, canonical: canonical(vector.value), digest: hashDomain(vector.domain, vector.value) })),
};

tableOwnership.contract = "cowd.ownership-split/v1.2-final";
fieldMapping.contract = "cowd.ownership-fields/v1.2-final";
referenceGraph.contract = "cowd.ownership-references/v1.2-final";
reconcileRules.contract = "cowd.ownership-reconcile/v1.2-final";
ownershipSchema.properties.contract_version.const = FINAL_CONTRACT_VERSION;
ownershipSchema.properties.source = { "$ref": "#/$defs/source" };
ownershipSchema.$defs.source.required.push("external_catalog_digest");
ownershipSchema.$defs.source.properties.external_catalog_digest = { "$ref": "#/$defs/digest" };
ownershipSchema.$defs.reference = typedReferenceSchema;
ownershipSchema.$defs.externalReferenceCatalogEntry = {
  type: "object", additionalProperties: false, required: ["namespace", "aggregate", "stable_id"],
  properties: {
    namespace: { "$ref": "#/$defs/nonEmpty" }, aggregate: { "$ref": "#/$defs/nonEmpty" },
    stable_id: { type: "object", minProperties: 1, additionalProperties: { "$ref": "#/$defs/scalar" } },
    revision: { "$ref": "#/$defs/scalar" }, digest: { "$ref": "#/$defs/digest" },
  },
};
ownershipSchema.$defs.externalReferenceCatalog = {
  type: "object", additionalProperties: false, required: ["schema", "digest", "owner", "exported_at", "entries"],
  properties: {
    schema: { const: "ExternalReferenceCatalogV1" }, digest: { "$ref": "#/$defs/digest" },
    owner: { "$ref": "#/$defs/nonEmpty" }, exported_at: { type: "string", format: "date-time" },
    entries: { type: "array", items: { "$ref": "#/$defs/externalReferenceCatalogEntry" } },
  },
};
ownershipSchema.$defs.object.properties.revision = {
  type: "object", additionalProperties: false, required: ["strategy", "authority", "context"],
  properties: {
    strategy: { enum: ["monotonic_authority", "immutable_payload"] },
    authority: { anyOf: [{ type: "null" }, { type: "object", additionalProperties: false, required: ["field", "type", "comparison", "value"], properties: { field: { "$ref": "#/$defs/nonEmpty" }, type: { enum: ["INTEGER", "TEXT"] }, comparison: { enum: ["unsigned_integer", "lexicographic_utf8"] }, value: { "$ref": "#/$defs/scalar" } } }] },
    context: { type: "object", additionalProperties: { "$ref": "#/$defs/scalar" } },
  },
};
ownershipSchema.$defs.object.properties.source_references = { type: "array", items: { "$ref": "#/$defs/reference" } };
ownershipSchema.$defs.object.properties.evidence_references = { type: "array", items: { "$ref": "#/$defs/reference" } };
for (const name of ["mutation_receipt_aliases", "mutation_receipt_repairs"]) {
  ownershipSchema.$defs.reconciliation.required.splice(-1, 0, name);
  ownershipSchema.$defs.reconciliation.properties[name] = { type: "array", items: { "$ref": "#/$defs/reconcileRecord" } };
}

// F7.2 is the sole terminal contract. Everything below replaces the unpublished
// v1.1 shapes without retaining a compatibility branch.
const FINAL_SUFFIX = "v1.2-final";
tableOwnership.contract = `cowd.ownership-split/${FINAL_SUFFIX}`;
fieldMapping.contract = `cowd.ownership-fields/${FINAL_SUFFIX}`;
referenceGraph.contract = `cowd.ownership-references/${FINAL_SUFFIX}`;
reconcileRules.contract = `cowd.ownership-reconcile/${FINAL_SUFFIX}`;
revisionProjection.contract = `cowd.ownership-revision-projection/${FINAL_SUFFIX}`;
referenceEncoding.contract = `cowd.ownership-reference-encoding/${FINAL_SUFFIX}`;
reconciliationMapping.contract = `cowd.ownership-reconciliation/${FINAL_SUFFIX}`;
jsonSchemaRegistry.contract = `cowd.ownership-json-schema-registry/${FINAL_SUFFIX}`;
canonicalization.contract = `cowd.ownership-canonicalization/${FINAL_SUFFIX}`;

function targetIdentity(edge) {
  if (edge.target.table) {
    const target = ddl.get(edge.target.table);
    return { target_scope: "snapshot", aggregate_type: edge.target.table, key_fields: target.primary_key };
  }
  const target = externalTargets[edge.target.external];
  return { target_scope: "external_catalog", aggregate_type: edge.target.external, key_fields: [target.key] };
}

function referenceClass(edge) {
  const proof = edge.target.external === "evidence.reference"
    || edge.target.external === "core.receipt"
    || edge.target.table === "matrix_evidence_packet"
    || edge.source.field.includes("evidence")
    || edge.source.field.includes("receipt")
    || /receipt|repair_report/.test(edge.source.table);
  return proof ? "evidence" : "source";
}

function freezeExtractor(edge, index, kind) {
  const reference_class = referenceClass(edge);
  return {
    extractor_id: `${kind}-${String(index + 1).padStart(3, "0")}`,
    ...edge,
    reference_class,
    destination_field: `${reference_class}_references`,
    ...targetIdentity(edge),
  };
}

referenceEncoding.typed_reference = {
  required_fields: ["aggregate_type", "stable_id", "revision", "payload_digest", "source"],
  stable_id: "<aggregate_type>:<base64url-no-pad(RFC8785-canonical key-object bytes)>",
  internal_copy: "revision and payload_digest MUST exactly equal the resolved snapshot target",
  external_copy: "revision and payload_digest MUST exactly equal the resolved ExternalReferenceCatalogV1 entry, including explicit null",
};
referenceEncoding.classification = {
  allowed: ["source", "evidence"], destinations: { source: "source_references", evidence: "evidence_references" },
  arrays_are_mutually_exclusive: true, unclassified_or_third_class: "reject",
};
referenceEncoding.external_catalog.binding = "source.external_catalog_digest MUST equal catalog.digest";
referenceEncoding.column_reference_edges = referenceGraph.edges.filter((edge) => !edge.source.json_path).map((edge, index) => freezeExtractor(edge, index, "column"));
referenceEncoding.json_reference_edges = referenceGraph.edges.filter((edge) => edge.source.json_path).map((edge, index) => freezeExtractor(edge, index, "hidden"));
referenceEncoding.closure = { total: 134, column: 46, hidden_json: 88, source_plus_evidence_must_equal_total: true };
delete referenceEncoding.source_fields;
delete referenceEncoding.evidence_fields;

const identityContract = {
  contract: `cowd.ownership-identity/${FINAL_SUFFIX}`,
  algorithm: "<aggregate_type>:<base64url-no-pad(RFC8785-canonical key-object bytes)>",
  aggregate_type: "source_table for snapshot objects; frozen external namespace for catalog entries",
  key_object: "exact target primary-key fields in table-ownership primary_key order; no missing, extra, or null key",
  canonicalization: "RFC8785 rules from canonicalization.json",
  base64: "RFC4648 URL-safe alphabet without padding",
  duplicate_identity: "reject",
  tables: Object.fromEntries(migration.map((table) => [table, { aggregate_type: table, key_fields: ddl.get(table).primary_key }])),
};

function stableId(aggregateType, keyObject) {
  return `${aggregateType}:${Buffer.from(canonical(keyObject), "utf8").toString("base64url")}`;
}

for (const [table, rule] of Object.entries(revisionProjection.tables)) {
  const pk = ddl.get(table).primary_key;
  const axis = rule.authority ? [rule.authority] : [];
  rule.projection_key_fields = pk.filter((field) => !axis.some((entry) => entry.field === field));
  if (rule.projection_key_fields.length === 0) rule.projection_key_fields = [...pk];
  rule.ordered_revision_axis = axis;
  rule.immutable_context_fields = rule.context;
  delete rule.authority;
  delete rule.context;
}
revisionProjection.policy = {
  projection_key: "canonical object of projection_key_fields",
  within_snapshot: "per aggregate_type+projection_key ordered_revision_axis is strictly increasing with no duplicate; immutable context digest is identical",
  baseline: "snapshot max axis MUST be >= RevisionBaselineCatalogV1 max and context_digest MUST equal",
  immutable_payload: "one object per projection key; divergent duplicate rejects",
  first_export: "only initial=true plus an empty, validly digested baseline catalog; absence never means initial",
  pk_revision_rule: "full primary key still determines stable_id even when a revision field is removed from projection_key_fields",
};

const revisionBaselineSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema", title: "RevisionBaselineCatalogV1",
  type: "object", additionalProperties: false,
  required: ["schema", "digest", "owner", "exported_at", "initial", "entries"],
  properties: {
    schema: { const: "RevisionBaselineCatalogV1" }, digest: { pattern: digestPattern }, owner: { type: "string", minLength: 1 },
    exported_at: { type: "string", format: "date-time" }, initial: { type: "boolean" },
    entries: { type: "array", items: { type: "object", additionalProperties: false, required: ["aggregate_type", "projection_key", "axis_max", "context_digest"], properties: {
      aggregate_type: { type: "string", minLength: 1 }, projection_key: { type: "string", minLength: 1 },
      axis_max: { type: "array", items: { type: ["integer", "string"] } }, context_digest: { pattern: digestPattern },
    } } },
  },
  allOf: [
    { if: { properties: { initial: { const: true } }, required: ["initial"] }, then: { properties: { entries: { maxItems: 0 } } } },
    { if: { properties: { initial: { const: false } }, required: ["initial"] }, then: { properties: { entries: { minItems: 1 } } } },
  ],
};

const reconciliationDomains = {
  pending_outbox: "cowd.ownership.reconcile.outbox.v1",
  command_receipts: "cowd.ownership.reconcile.command-receipt.v1",
  mutation_receipts: "cowd.ownership.reconcile.mutation-receipt.v1",
  mutation_receipt_aliases: "cowd.ownership.reconcile.mutation-alias.v1",
  mutation_receipt_repairs: "cowd.ownership.reconcile.mutation-repair.v1",
};
const reconciliationRecords = {
  pending_outbox: {
    required: ["stable_ref", "status", "action", "effect_key", "attempt_count", "next_attempt_at", "last_error", "receipt_ref", "payload", "payload_digest"],
    sources: { stable_ref: "effect_id", status: "status", action: "action", effect_key: "effect_key", attempt_count: "attempt_count", next_attempt_at: "next_attempt_at", last_error: "last_error", receipt_ref: "receipt_ref", payload: "payload_json" },
  },
  command_receipts: {
    required: ["stable_ref", "status", "domain", "idempotency_key", "subject_ref", "receipt", "created_at", "payload_digest"],
    sources: { stable_ref: "domain+U+001F+idempotency_key", status: "constant:recorded", domain: "domain", idempotency_key: "idempotency_key", subject_ref: "subject_ref", receipt: "receipt_json", created_at: "created_at" },
  },
  mutation_receipts: {
    required: ["stable_ref", "status", "receipt_id", "idempotency_key", "actor_principal", "action_id", "resource_ref", "expected_revision", "result_revision", "mutation_payload_digest", "lease_token", "response", "contract_version", "created_at", "updated_at", "payload_digest"],
    sources: { stable_ref: "receipt_id", status: "status", receipt_id: "receipt_id", idempotency_key: "idempotency_key", actor_principal: "actor_principal", action_id: "action_id", resource_ref: "resource_ref", expected_revision: "expected_revision", result_revision: "result_revision", mutation_payload_digest: "payload_digest", lease_token: "lease_token", response: "response_json", contract_version: "contract_version", created_at: "created_at", updated_at: "updated_at" },
  },
  mutation_receipt_aliases: {
    required: ["stable_ref", "status", "legacy_idempotency_key", "canonical_receipt_stable_id", "canonical_receipt_payload_digest", "created_at", "payload_digest"],
    sources: { stable_ref: "legacy_idempotency_key", status: "constant:bound", legacy_idempotency_key: "legacy_idempotency_key", canonical_receipt_stable_id: "resolved receipt_id stable_id", canonical_receipt_payload_digest: "resolved receipt payload_digest", created_at: "created_at" },
  },
  mutation_receipt_repairs: {
    required: ["stable_ref", "status", "report_id", "idempotency_key", "existing_receipt", "incoming_receipt", "existing_digest", "incoming_digest", "conflict_fields", "created_at", "payload_digest"],
    sources: { stable_ref: "report_id", status: "constant:conflict_preserved", report_id: "report_id", idempotency_key: "idempotency_key", existing_receipt: "existing_receipt_json", incoming_receipt: "incoming_receipt_json", created_at: "created_at", existing_digest: "canonical existing_receipt", incoming_digest: "canonical incoming_receipt", conflict_fields: "sorted differing JSON pointers" },
  },
};
for (const [name, record] of Object.entries(reconciliationRecords)) {
  record.additional_properties = false;
  record.hash = { domain_separator: `${reconciliationDomains[name]}\\0`, envelope: record.required.filter((field) => field !== "payload_digest"), excluded_fields: ["payload_digest"] };
}
reconciliationMapping.records = reconciliationRecords;
reconciliationMapping.set_digest = { domain_separator: "cowd.ownership.reconciliation.v1\\0", envelope: Object.keys(reconciliationRecords), excluded_fields: ["set_digest"] };

const executionProfile = {
  contract: `cowd.ownership-execution-profile/${FINAL_SUFFIX}`,
  digest: "",
  source_metadata_inputs: {
    additional_properties: false,
    required: ["source_version", "exported_at", "maintenance_fence_id", "expected_legacy_schema_version"],
    fields: {
      source_version: { type: "string", min_length: 1, max_length: 256, semantic: "caller-supplied opaque source watermark" },
      exported_at: { type: "string", format: "RFC3339 UTC with terminal Z", semantic: "caller-supplied deterministic export timestamp" },
      maintenance_fence_id: { type: "string", min_length: 1, max_length: 256, semantic: "caller-supplied maintenance fence identifier verified at start and end" },
      expected_legacy_schema_version: { type: "integer", minimum: 1, semantic: "caller-supplied exact matrix_schema.schema_version" },
    },
    forbidden_sources: ["wall clock", "randomness", "database inference", "implicit default"],
  },
  backend_inputs: {
    additional_properties: false,
    required: ["backend"],
    fields: {
      backend: { type: "string", enum: ["sqlite", "postgres"] },
      sqlite_namespace: { type: "string", const: "main", required_when: "backend=sqlite" },
      postgres_source_schema: { type: "string", pattern: "^[a-z_][a-z0-9_]{0,62}$", required_when: "backend=postgres" },
    },
  },
  publication_inputs: {
    additional_properties: false,
    required: ["generation", "output_parent"],
    fields: {
      generation: { type: "string", pattern: "^[A-Za-z0-9._-]+$" },
      output_parent: { type: "path", semantic: "existing writable parent directory" },
    },
    contract: `cowd.ownership-publication/${FINAL_SUFFIX}`,
    published_files: ["snapshot.json", "receipt.json"],
  },
  fixed_digest_semantics: "digest covers this closed static rule contract only; per-run input values are carried in snapshot.json and receipt.json and MUST NOT change this digest",
};
executionProfile.digest = hashDomain("cowd.ownership.execution-profile.v1", Object.fromEntries(Object.entries(executionProfile).filter(([key]) => key !== "digest")));

const executionInputs = {
  schema: "OwnershipExecutionInputsV1",
  source_metadata: {
    source_version: "fixture-1", exported_at: "2026-08-15T00:00:00Z", maintenance_fence_id: "fixture-fence",
    expected_legacy_schema_version: 1,
  },
  sqlite: { backend: "sqlite", namespace: "main" },
  postgres: { backend: "postgres", source_schema: "mfg" },
  publication: { generation: "comprehensive-1", output_parent: "/deterministic-fixture-output" },
};

const legacySchemaMetadata = { namespace: "main", id: 1, schema_version: 1, updated_at: "2026-08-15T00:00:00Z", disposition: "validate_and_record_never_copy" };
const baselineEmpty = withDigest({ schema: "RevisionBaselineCatalogV1", digest: "", owner: "migration-controller", exported_at: executionInputs.source_metadata.exported_at, initial: true, entries: [] }, "digest", "cowd.ownership.revision-baseline.v1");

const exportReceiptSchema = {
  "$schema": "https://json-schema.org/draft/2020-12/schema", title: "OwnershipExportReceiptV1", type: "object", additionalProperties: false,
  required: ["schema", "generation", "snapshot_file_digest", "contract_digest", "schema_digest", "external_catalog_digest", "revision_baseline_digest", "execution_profile_digest", "source", "counts", "excluded_actions", "receipt_digest"],
  properties: {
    schema: { const: "OwnershipExportReceiptV1" }, generation: { type: "string", pattern: "^[A-Za-z0-9._-]+$" }, snapshot_file_digest: { pattern: digestPattern },
    contract_digest: { pattern: digestPattern }, schema_digest: { pattern: digestPattern }, external_catalog_digest: { pattern: digestPattern }, revision_baseline_digest: { pattern: digestPattern }, execution_profile_digest: { pattern: digestPattern },
    source: { type: "object", additionalProperties: false, required: ["backend", "namespace", "source_version", "schema_version", "maintenance_fence_id", "exported_at"], properties: { backend: { enum: ["sqlite", "postgres"] }, namespace: { type: "string", minLength: 1, maxLength: 63 }, source_version: { type: "string", minLength: 1, maxLength: 256 }, schema_version: { type: "integer", minimum: 1 }, maintenance_fence_id: { type: "string", minLength: 1, maxLength: 256 }, exported_at: { type: "string", format: "date-time", pattern: "Z$" } } },
    counts: { type: "object", additionalProperties: false, required: ["tables", "mfg_objects", "core_objects", "reconciliation", "excluded"], properties: { tables: { const: 46 }, mfg_objects: { type: "integer", minimum: 0 }, core_objects: { type: "integer", minimum: 0 }, reconciliation: { type: "integer", minimum: 0 }, excluded: { const: 3 } } },
    excluded_actions: { type: "array", minItems: 3, maxItems: 3 }, receipt_digest: { pattern: digestPattern },
  },
  digest: { domain_separator: "cowd.ownership.export-receipt.v1\\0", excluded_fields: ["receipt_digest"] },
  importer_receipt: { target_journal_verification_owner: "importer", exporter_claims_target_state: false },
};

const rejectionCodes = {
  contract: `cowd.ownership-rejection-codes/${FINAL_SUFFIX}`,
  codes: ["E_CONTRACT_VERSION", "E_DIGEST", "E_REFERENCE_CLASS", "E_DANGLING_INTERNAL", "E_DANGLING_EXTERNAL", "E_REFERENCE_COPY", "E_REVISION_ORDER", "E_REVISION_BASELINE", "E_MATRIX_SCHEMA", "E_EXECUTION_PROFILE", "E_DB_NAMESPACE", "E_DB_TYPE", "E_RECONCILIATION", "E_PUBLICATION_EXISTS", "E_UNKNOWN_FIELD"],
  rule: "every rejection returns exactly one stable code plus non-authoritative diagnostic text",
  validation_order: ["strict JSON/schema and unknown fields", "contract version", "execution profile and catalog/baseline bindings", "matrix_schema metadata", "reference class and resolution", "revision projection and baseline", "typed reconciliation", "section/set/whole digests"],
  precedence: "the first failing stage owns the rejection code; later digest mismatch MUST NOT mask a more specific earlier semantic rejection",
};

canonicalization.domains.stable_id = "RFC8785 canonical key-object bytes before base64url-no-pad";
canonicalization.domains.revision_context_digest = "cowd.ownership.revision-context.v1\\0 + canonical immutable context object";
canonicalization.domains.revision_baseline_digest = "cowd.ownership.revision-baseline.v1\\0 + canonical catalog excluding digest";
canonicalization.domains.execution_profile_digest = "cowd.ownership.execution-profile.v1\\0 + canonical profile excluding digest";
canonicalization.domains.reconciliation_record_digests = reconciliationDomains;
canonicalization.domains.export_receipt_digest = "cowd.ownership.export-receipt.v1\\0 + canonical receipt excluding receipt_digest";
canonicalization.domains.snapshot_file_digest = "sha256 of exact UTF-8 snapshot.json file bytes";
canonicalization.domains.receipt_file_digest = "sha256 of exact UTF-8 receipt.json file bytes; bound by publication manifest, never self-referential";
canonicalization.timestamp = { syntax: "RFC3339", timezone: "UTC only with terminal Z", fractional_seconds: "preserve input precision losslessly", invalid_or_offset_timestamp: "reject" };

const referenceV12Schema = {
  type: "object", additionalProperties: false, required: ["aggregate_type", "stable_id", "revision", "payload_digest", "source"],
  properties: { aggregate_type: { type: "string", minLength: 1 }, stable_id: { type: "string", minLength: 3 }, revision: { type: ["object", "null"] }, payload_digest: { anyOf: [{ pattern: digestPattern }, { type: "null" }] }, source: { type: "object", additionalProperties: false, required: ["table", "field", "json_pointer", "extractor_id"], properties: { table: { type: "string", minLength: 1 }, field: { type: "string", minLength: 1 }, json_pointer: { type: ["string", "null"] }, extractor_id: { type: "string", minLength: 1 } } } },
};
ownershipSchema.properties.contract_version.const = FINAL_CONTRACT_VERSION;
ownershipSchema.$defs.source.required = ["app_id", "source_version", "schema_version", "exported_at", "maintenance_fence_id", "expected_legacy_schema_version", "ownership_contract_digest", "external_catalog_digest", "revision_baseline_digest", "execution_profile_digest", "legacy_schema"];
Object.assign(ownershipSchema.$defs.source.properties, {
  source_version: { type: "string", minLength: 1, maxLength: 256 }, exported_at: { type: "string", format: "date-time", pattern: "Z$" }, maintenance_fence_id: { type: "string", minLength: 1, maxLength: 256 },
  expected_legacy_schema_version: { type: "integer", minimum: 1 }, external_catalog_digest: { pattern: digestPattern }, revision_baseline_digest: { pattern: digestPattern }, execution_profile_digest: { pattern: digestPattern },
  legacy_schema: { type: "object", additionalProperties: false, required: ["namespace", "id", "schema_version", "updated_at", "disposition"], properties: { namespace: { type: "string", minLength: 1 }, id: { const: 1 }, schema_version: { type: "integer", minimum: 1 }, updated_at: { type: "string", format: "date-time" }, disposition: { const: "validate_and_record_never_copy" } } },
});
ownershipSchema.$defs.object.properties.stable_id = { type: "string", minLength: 3 };
ownershipSchema.$defs.object.properties.revision = { type: "object", additionalProperties: false, required: ["projection_key", "axis", "context", "context_digest"], properties: { projection_key: { type: "string", minLength: 1 }, axis: { type: "array", items: { type: ["integer", "string"] } }, context: { type: "object" }, context_digest: { pattern: digestPattern } } };
ownershipSchema.$defs.reference = referenceV12Schema;
ownershipSchema.$defs.object.properties.source_references = { type: "array", items: { "$ref": "#/$defs/reference" } };
ownershipSchema.$defs.object.properties.evidence_references = { type: "array", items: { "$ref": "#/$defs/reference" } };
ownershipSchema.$defs.externalReferenceCatalogEntry = { type: "object", additionalProperties: false, required: ["aggregate_type", "stable_id", "revision", "payload_digest"], properties: { aggregate_type: { type: "string", minLength: 1 }, stable_id: { type: "string", minLength: 3 }, revision: { type: ["object", "null"] }, payload_digest: { anyOf: [{ pattern: digestPattern }, { type: "null" }] } } };
ownershipSchema.$defs.externalReferenceCatalog = { type: "object", additionalProperties: false, required: ["schema", "digest", "owner", "exported_at", "entries"], properties: { schema: { const: "ExternalReferenceCatalogV1" }, digest: { pattern: digestPattern }, owner: { type: "string", minLength: 1 }, exported_at: { type: "string", format: "date-time" }, entries: { type: "array", items: { "$ref": "#/$defs/externalReferenceCatalogEntry" } } } };
ownershipSchema.$defs.reconcileRecord = { oneOf: Object.entries(reconciliationRecords).map(([name, record]) => ({ title: name, type: "object", additionalProperties: false, required: record.required, properties: Object.fromEntries(record.required.map((field) => [field, field.endsWith("payload_digest") ? { pattern: digestPattern } : field.endsWith("_at") ? { type: ["string", "null"] } : field === "attempt_count" ? { type: "integer", minimum: 0 } : field === "expected_revision" || field === "result_revision" ? { type: ["integer", "null"] } : field === "payload" || field === "receipt" || field === "response" || field.endsWith("_receipt") ? {} : field === "conflict_fields" ? { type: "array", items: { type: "string" } } : { type: ["string", "null"] }])) })) };
const reconciliationDefinitionNames = {
  pending_outbox: "pendingOutboxRecord", command_receipts: "commandReceiptRecord",
  mutation_receipts: "mutationReceiptRecord", mutation_receipt_aliases: "mutationAliasRecord",
  mutation_receipt_repairs: "mutationRepairRecord",
};
Object.keys(reconciliationRecords).forEach((name, index) => {
  const definition = reconciliationDefinitionNames[name];
  ownershipSchema.$defs[definition] = ownershipSchema.$defs.reconcileRecord.oneOf[index];
  ownershipSchema.$defs.reconciliation.properties[name] = { type: "array", items: { "$ref": `#/$defs/${definition}` } };
});
delete ownershipSchema.$defs.reconcileRecord;

const databaseProfile = withDigest(
  {
    contract: `cowd.ownership-database-profile/${FINAL_SUFFIX}`,
    digest: "",
    execution_profile_digest: executionProfile.digest,
    sqlite: { namespace: "main", transaction: "BEGIN", schema_scan: "main.sqlite_schema only" },
    postgres: { source_schema_pattern: "^[a-z_][a-z0-9_]{0,62}$", transaction: "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY DEFERRABLE", identifiers: "all references explicitly source_schema-qualified; search_path ignored" },
    namespace_inventory: { required_authority_tables: 46, required_metadata_tables: ["matrix_schema"], extra_or_missing: "reject" },
    matrix_schema: { row_count: 1, id: 1, schema_version: "must equal positive expected_legacy_schema_version input", updated_at: "parse by canonical timestamp rule; validate and record only; never copy" },
    source_type_equivalence: {
      "text|varchar|character varying|json|jsonb": "sqlite.text", "smallint|integer|bigint": "sqlite.integer",
      "real|double precision": "sqlite.real", numeric: "sqlite.real only when finite and exactly IEEE-754 representable; otherwise reject",
      "bytea|blob": "reject", "boolean": "sqlite.integer constrained to 0|1", "timestamp with time zone": "sqlite.text RFC3339 UTC",
    },
    cross_backend_equivalence: "with identical explicit source metadata, catalogs, logical rows, and execution inputs, SQLite and PostgreSQL MUST emit byte-identical snapshot.json; receipt.json remains backend-attributed as required audit evidence",
    column_equivalence: "name, canonical source type, nullability, absence/presence and canonical expression of default, and ordered PK position must exactly match field-mapping/legacy DDL",
  },
  "digest",
  "cowd.ownership.database-profile.v1",
);

const publicationContract = {
  contract: `cowd.ownership-publication/${FINAL_SUFFIX}`,
  unit: "one sibling staging generation directory",
  staging_name: ".<generation>.ownership-split.staging",
  required_files: ["snapshot.json", "receipt.json"],
  durability_order: ["write snapshot.json", "fsync snapshot.json", "write receipt.json", "fsync receipt.json", "fsync staging directory", "rename directory once to <generation>.ownership-split", "fsync parent directory"],
  existing_generation: "E_PUBLICATION_EXISTS",
  crash_rule: "only an invisible staging directory may remain",
  forbidden: ["overwrite", "two independent file renames", "publish before both fsyncs"],
  manifest_binding: ["snapshot_file_digest", "receipt_file_digest"],
};

const contractDocuments = {
  "table-ownership.json": tableOwnership, "field-mapping.json": fieldMapping,
  "reference-graph.json": referenceGraph, "reconcile-rules.json": reconcileRules,
  "revision-projection.json": revisionProjection, "reference-encoding.json": referenceEncoding,
  "reconciliation-mapping.json": reconciliationMapping, "json-schema-registry.json": jsonSchemaRegistry,
  "canonicalization.json": canonicalization, "identity.json": identityContract,
  "revision-baseline-v1.schema.json": revisionBaselineSchema,
  "ownership-export-receipt-v1.schema.json": exportReceiptSchema,
  "execution-profile.json": executionProfile, "database-profile.json": databaseProfile,
  "publication.json": publicationContract, "rejection-codes.json": rejectionCodes,
  "ownership-split-v1.schema.json": ownershipSchema,
};
const contractDocumentValues = Object.values(contractDocuments);
const contractHasher = createHash("sha256");
for (const document of contractDocumentValues) {
  contractHasher.update(`${JSON.stringify(document, null, 2)}\n`);
  contractHasher.update(Buffer.from([0]));
}
const finalContractDigest = `sha256:${contractHasher.digest("hex")}`;

function externalKeyValue(namespace) { return `external-${namespace.replaceAll(".", "-")}`; }
const externalReferenceCatalogV12 = withDigest({
  schema: "ExternalReferenceCatalogV1", digest: "", owner: "cowd-core", exported_at: executionInputs.source_metadata.exported_at,
  entries: Object.entries(externalTargets).sort(([left], [right]) => utf8ByteCompare(left, right)).map(([aggregate_type, target]) => {
    const key = { [target.key]: externalKeyValue(aggregate_type) };
    return { aggregate_type, stable_id: stableId(aggregate_type, key), revision: null, payload_digest: null };
  }),
}, "digest", "cowd.ownership.external-catalog.v1");

function sampleValue(table, column) {
  if (column.name.endsWith("_json")) return "{}";
  if (column.name.endsWith("_at")) return executionInputs.source_metadata.exported_at;
  if (column.sql_type === "INTEGER") return 1;
  if (column.sql_type === "REAL") return 1;
  return `${table}-${column.name}`;
}

function revisionFor(table, payload) {
  const rule = revisionProjection.tables[table];
  const projectionKeyObject = Object.fromEntries(rule.projection_key_fields.map((field) => [field, payload[field]]));
  const context = Object.fromEntries(rule.immutable_context_fields.map((field) => [field.field, payload[field.field]]));
  return {
    projection_key: stableId(`${table}.projection`, projectionKeyObject),
    axis: rule.ordered_revision_axis.map((axis) => payload[axis.field]), context,
    context_digest: hashDomain("cowd.ownership.revision-context.v1", context),
  };
}

const comprehensiveObjects = Object.fromEntries(migration.filter((table) => !excluded.has(table)).map((table) => {
  const payload = Object.fromEntries(ddl.get(table).columns.map((column) => [column.name, sampleValue(table, column)]));
  return [table, { source_table: table, stable_id: "", revision: null, source_references: [], evidence_references: [], payload, payload_digest: "" }];
}));

function edgeValue(edge) {
  if (edge.target.table) return comprehensiveObjects[edge.target.table].payload[edge.target.field];
  return externalKeyValue(edge.target.external);
}
for (const extractor of referenceEncoding.column_reference_edges) {
  comprehensiveObjects[extractor.source.table].payload[extractor.source.field] = edgeValue(extractor);
}
const activeHidden = [
  referenceEncoding.json_reference_edges.find((edge) => edge.source.table === "mfg_cockpit_profile" && edge.source.json_path === "$.owner_ref"),
  referenceEncoding.json_reference_edges.find((edge) => edge.target.external === "evidence.reference"),
];
for (const extractor of activeHidden) {
  const object = comprehensiveObjects[extractor.source.table];
  const value = edgeValue(extractor);
  const rootField = extractor.source.json_path.match(/^\$\.([^.[*]+)/)?.[1];
  object.payload[extractor.source.field] = JSON.stringify({ [rootField]: extractor.source.json_path.includes("[*]") ? [value] : value });
}
for (const [table, object] of Object.entries(comprehensiveObjects)) {
  const definition = ddl.get(table);
  const key = Object.fromEntries(definition.primary_key.map((field) => [field, object.payload[field]]));
  object.stable_id = stableId(table, key);
  object.revision = revisionFor(table, object.payload);
  object.payload_digest = hashDomain("cowd.ownership.payload.v1", object.payload);
}
function referenceFor(extractor) {
  const targetObject = extractor.target.table ? comprehensiveObjects[extractor.target.table] : null;
  const targetCatalog = extractor.target.external ? externalReferenceCatalogV12.entries.find((entry) => entry.aggregate_type === extractor.target.external) : null;
  return {
    aggregate_type: extractor.aggregate_type,
    stable_id: targetObject?.stable_id ?? targetCatalog.stable_id,
    revision: targetObject?.revision ?? targetCatalog.revision,
    payload_digest: targetObject?.payload_digest ?? targetCatalog.payload_digest,
    source: { table: extractor.source.table, field: extractor.source.field, json_pointer: extractor.source.json_path ?? null, extractor_id: extractor.extractor_id },
  };
}
for (const extractor of [...referenceEncoding.column_reference_edges, ...activeHidden]) {
  comprehensiveObjects[extractor.source.table][extractor.destination_field].push(referenceFor(extractor));
}
for (const object of Object.values(comprehensiveObjects)) {
  object.source_references.sort((left, right) => utf8ByteCompare(canonical(left), canonical(right)));
  object.evidence_references.sort((left, right) => utf8ByteCompare(canonical(left), canonical(right)));
}
const revisionSibling = structuredClone(comprehensiveObjects.mfg_cockpit_view_version);
revisionSibling.payload.revision = 2;
revisionSibling.stable_id = stableId("mfg_cockpit_view_version", { profile_id: revisionSibling.payload.profile_id, revision: 2 });
revisionSibling.revision = revisionFor("mfg_cockpit_view_version", revisionSibling.payload);
revisionSibling.payload_digest = hashDomain("cowd.ownership.payload.v1", revisionSibling.payload);

function reconcileRecord(name, value) {
  return { ...value, payload_digest: hashDomain(reconciliationDomains[name], value) };
}
const comprehensiveReconciliationBody = {
  pending_outbox: [reconcileRecord("pending_outbox", { stable_ref: "effect-1", status: "pending", action: "reroute", effect_key: "effect-key-1", attempt_count: 0, next_attempt_at: null, last_error: null, receipt_ref: null, payload: { review_id: "review-1" } })],
  command_receipts: [reconcileRecord("command_receipts", { stable_ref: "mfg\u001fcommand-1", status: "recorded", domain: "mfg", idempotency_key: "command-1", subject_ref: "subject-1", receipt: { ok: true }, created_at: executionInputs.source_metadata.exported_at })],
  mutation_receipts: [reconcileRecord("mutation_receipts", { stable_ref: "receipt-1", status: "completed", receipt_id: "receipt-1", idempotency_key: "mutation-1", actor_principal: "actor-1", action_id: "action-1", resource_ref: "resource-1", expected_revision: 1, result_revision: 2, mutation_payload_digest: `sha256:${"a".repeat(64)}`, lease_token: "", response: { completed: true }, contract_version: "mfg.mutation/v1", created_at: executionInputs.source_metadata.exported_at, updated_at: executionInputs.source_metadata.exported_at })],
  mutation_receipt_aliases: [],
  mutation_receipt_repairs: [],
};
const mutationTarget = comprehensiveReconciliationBody.mutation_receipts[0];
comprehensiveReconciliationBody.mutation_receipt_aliases.push(reconcileRecord("mutation_receipt_aliases", { stable_ref: "legacy-1", status: "bound", legacy_idempotency_key: "legacy-1", canonical_receipt_stable_id: mutationTarget.stable_ref, canonical_receipt_payload_digest: mutationTarget.payload_digest, created_at: executionInputs.source_metadata.exported_at }));
const existingReceipt = { status: "completed", revision: 1 };
const incomingReceipt = { status: "completed", revision: 2 };
comprehensiveReconciliationBody.mutation_receipt_repairs.push(reconcileRecord("mutation_receipt_repairs", { stable_ref: "repair-1", status: "conflict_preserved", report_id: "repair-1", idempotency_key: "mutation-1", existing_receipt: existingReceipt, incoming_receipt: incomingReceipt, existing_digest: hashDomain("cowd.ownership.repair-side.v1", existingReceipt), incoming_digest: hashDomain("cowd.ownership.repair-side.v1", incomingReceipt), conflict_fields: ["/revision"], created_at: executionInputs.source_metadata.exported_at }));
const comprehensiveReconciliation = withDigest({ ...comprehensiveReconciliationBody, set_digest: "" }, "set_digest", "cowd.ownership.reconciliation.v1");

const baselineObject = comprehensiveObjects.mfg_cockpit_view_version;
const revisionBaselineV12 = withDigest({ schema: "RevisionBaselineCatalogV1", digest: "", owner: "migration-controller", exported_at: executionInputs.source_metadata.exported_at, initial: false, entries: [{ aggregate_type: baselineObject.source_table, projection_key: baselineObject.revision.projection_key, axis_max: baselineObject.revision.axis, context_digest: baselineObject.revision.context_digest }] }, "digest", "cowd.ownership.revision-baseline.v1");

function section(owner, objects) { return withDigest({ owner, object_count: objects.length, section_digest: "", objects }, "section_digest", "cowd.ownership.section.v1"); }
function sourceEnvelope(baseline) { return { app_id: "mfg", source_version: executionInputs.source_metadata.source_version, schema_version: 1, exported_at: executionInputs.source_metadata.exported_at, maintenance_fence_id: executionInputs.source_metadata.maintenance_fence_id, expected_legacy_schema_version: executionInputs.source_metadata.expected_legacy_schema_version, ownership_contract_digest: finalContractDigest, external_catalog_digest: externalReferenceCatalogV12.digest, revision_baseline_digest: baseline.digest, execution_profile_digest: executionProfile.digest, legacy_schema: legacySchemaMetadata }; }
const excludedRecords = [...excluded].sort(utf8ByteCompare).map((source_table) => ({ source_table, reason: "runtime_or_secret_state_is_not_authority", regeneration: reconcileRules.excluded_regeneration[source_table] }));
function snapshotOf(mfgObjects, coreObjects, reconciliation, baseline) { return withDigest({ contract_version: FINAL_CONTRACT_VERSION, source: sourceEnvelope(baseline), mfg_domain: section("mfg", mfgObjects), core_matrix_domain: section("core", coreObjects), reconciliation, excluded: excludedRecords, whole_snapshot_digest: "" }, "whole_snapshot_digest", "cowd.ownership.snapshot.v1"); }
const minimalSnapshotV12 = snapshotOf([], [], withDigest({ pending_outbox: [], command_receipts: [], mutation_receipts: [], mutation_receipt_aliases: [], mutation_receipt_repairs: [], set_digest: "" }, "set_digest", "cowd.ownership.reconciliation.v1"), baselineEmpty);
const comprehensiveSnapshot = snapshotOf([...Object.values(comprehensiveObjects).filter((object) => mfg.has(object.source_table)), revisionSibling], Object.values(comprehensiveObjects).filter((object) => core.has(object.source_table)), comprehensiveReconciliation, revisionBaselineV12);

const schemaDigest = `sha256:${createHash("sha256").update(`${JSON.stringify(ownershipSchema, null, 2)}\n`).digest("hex")}`;
const comprehensiveSnapshotBytes = `${JSON.stringify(comprehensiveSnapshot, null, 2)}\n`;
const snapshotFileDigest = `sha256:${createHash("sha256").update(comprehensiveSnapshotBytes).digest("hex")}`;
const comprehensiveReceipt = withDigest({ schema: "OwnershipExportReceiptV1", generation: executionInputs.publication.generation, snapshot_file_digest: snapshotFileDigest, contract_digest: finalContractDigest, schema_digest: schemaDigest, external_catalog_digest: externalReferenceCatalogV12.digest, revision_baseline_digest: revisionBaselineV12.digest, execution_profile_digest: executionProfile.digest, source: { backend: executionInputs.sqlite.backend, namespace: executionInputs.sqlite.namespace, source_version: executionInputs.source_metadata.source_version, schema_version: executionInputs.source_metadata.expected_legacy_schema_version, maintenance_fence_id: executionInputs.source_metadata.maintenance_fence_id, exported_at: executionInputs.source_metadata.exported_at }, counts: { tables: 46, mfg_objects: 25, core_objects: 19, reconciliation: 5, excluded: 3 }, excluded_actions: excludedRecords, receipt_digest: "" }, "receipt_digest", "cowd.ownership.export-receipt.v1");
const receiptBytes = `${JSON.stringify(comprehensiveReceipt, null, 2)}\n`;
const publicationManifest = { generation: executionInputs.publication.generation, directory: `${executionInputs.publication.generation}.ownership-split`, snapshot_file_digest: snapshotFileDigest, receipt_file_digest: `sha256:${createHash("sha256").update(receiptBytes).digest("hex")}`, publish_operation: "single_directory_rename" };
[
  { domain: "cowd.ownership.revision-context.v1", value: { base_revision: 1 } },
  { domain: "cowd.ownership.reconciliation.v1", value: comprehensiveReconciliationBody },
  { domain: "cowd.ownership.execution-profile.v1", value: Object.fromEntries(Object.entries(executionProfile).filter(([key]) => key !== "digest")) },
  { domain: "cowd.ownership.revision-baseline.v1", value: Object.fromEntries(Object.entries(revisionBaselineV12).filter(([key]) => key !== "digest")) },
].forEach((vector) => digestVectors.vectors.push({ ...vector, canonical: canonical(vector.value), digest: hashDomain(vector.domain, vector.value) }));
digestVectors.identity_vectors = [{ aggregate_type: "matrix_entity", key_object: { entity_id: "entity-1" }, canonical: canonical({ entity_id: "entity-1" }), stable_id: stableId("matrix_entity", { entity_id: "entity-1" }) }];
digestVectors.file_vectors = { snapshot_file_digest: snapshotFileDigest, receipt_file_digest: publicationManifest.receipt_file_digest };

for (const [name, value] of Object.entries(contractDocuments)) {
  emit(join(here, name), value);
}

mkdirSync(join(here, "golden", "tamper"), { recursive: true });
for (const [name, value] of Object.entries({
  "minimal-snapshot.json": minimalSnapshotV12,
  "comprehensive-snapshot.json": comprehensiveSnapshot,
  "external-reference-catalog.json": externalReferenceCatalogV12,
  "revision-baseline-empty.json": baselineEmpty,
  "revision-baseline-comprehensive.json": revisionBaselineV12,
  "comprehensive-receipt.json": comprehensiveReceipt,
  "publication-manifest.json": publicationManifest,
  "execution-inputs.json": executionInputs,
  "digest-vectors.json": digestVectors,
})) emit(join(here, "golden", name), value);
for (const [name, code, mutate] of [
  ["unknown-contract-version.json", "E_CONTRACT_VERSION", (value) => { value.contract_version = "unknown"; }],
  ["catalog-digest-mismatch.json", "E_DIGEST", (value) => { value.source.external_catalog_digest = `sha256:${"f".repeat(64)}`; }],
  ["whole-digest-tamper.json", "E_DIGEST", (value) => { value.source.source_version = "tampered"; }],
  ["unknown-reconciliation-array.json", "E_UNKNOWN_FIELD", (value) => { value.reconciliation.unknown = []; }],
  ["reference-class.json", "E_REFERENCE_CLASS", (value) => { value.mfg_domain.objects[0].evidence_references = value.mfg_domain.objects[0].source_references.splice(0, 1); }],
  ["revision-baseline.json", "E_REVISION_BASELINE", (value) => { value.source.revision_baseline_digest = `sha256:${"e".repeat(64)}`; }],
  ["matrix-schema.json", "E_MATRIX_SCHEMA", (value) => { value.source.legacy_schema.id = 2; }],
  ["execution-profile.json", "E_EXECUTION_PROFILE", (value) => { value.source.execution_profile_digest = `sha256:${"d".repeat(64)}`; }],
  ["reconciliation.json", "E_RECONCILIATION", (value) => { value.reconciliation.pending_outbox[0].status = "unknown"; }],
]) {
  const value = structuredClone(comprehensiveSnapshot); mutate(value);
  emit(join(here, "golden", "tamper", name), { expected_rejection_code: code, snapshot: value });
}
