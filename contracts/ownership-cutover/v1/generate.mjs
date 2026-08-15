import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = dirname(fileURLToPath(import.meta.url));
const ownershipRoot = join(root, "../../ownership/v1");
const check = process.argv.includes("--check");
const files = new Map();
const OWNERSHIP_DIGEST = "sha256:61ed3c6becf145fcf1029b4ee39b2ac4d0aa39177ae2e195fe7ec2b052f270e5";
const CONTRACT_ID = "cowd.ownership-cutover/v1";

function canonical(value) {
  if (Array.isArray(value)) return `[${value.map(canonical).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${canonical(value[key])}`).join(",")}}`;
  }
  return JSON.stringify(value);
}

function digestBytes(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function domainDigest(domain, value) {
  return digestBytes(Buffer.concat([Buffer.from(`${domain}\0`), Buffer.from(canonical(value))]));
}

function pretty(value) {
  return `${JSON.stringify(value, null, 2)}\n`;
}

function emit(path, value) {
  files.set(path, typeof value === "string" ? value : pretty(value));
}

const execution = {
  schema_version: 1,
  contract_id: "cowd.ownership-cutover.execution/v1",
  scope: "offline_pointer_publication",
  preconditions: [
    "maintenance_lock_held",
    "gateway_stopped",
    "all_app_workers_stopped",
    "legacy_source_readonly",
    "core_generation_staged_invisible",
    "mfg_generation_staged_invisible",
  ],
  state_machine: {
    states: ["staged", "verified", "active", "rollback"],
    transitions: ["staged->verified", "verified->active", "active->rollback"],
    durable_receipt_states: ["staged", "verified"],
    active_fact: "active.json_after_publication_directory_fsync",
    rollback_fact: "a_new_active.json_pointer_after_publication_directory_fsync",
    publication_directory_final_entries: ["active.json"],
  },
  durability_sequence: [
    "validate_candidate_and_history",
    "fsync_source_and_import_receipt_files",
    "fsync_generation_directories",
    "write_active_json_temporary_in_publication_directory",
    "fsync_temporary_active_json",
    "fsync_publication_directory",
    "single_rename_temporary_to_active_json",
    "fsync_publication_directory_after_rename",
  ],
  atomicity: {
    cross_database_acid_claimed: false,
    visibility_unit: "one_active_json_pointer_to_exact_core_and_mfg_generation_pair",
    failure_before_rename: "old_active_json_remains_authoritative",
    failure_after_rename: "recover_by_validating_active_json_and_all_bound_receipts",
  },
  crash_recovery: [
    { point: "before_stage_complete", action: "discard_invisible_stage_and_keep_old_active" },
    { point: "after_stage_before_verify", action: "resume_verification_or_discard_invisible_stage" },
    { point: "after_verify_before_rename", action: "keep_old_active_and_replay_candidate_validation" },
    { point: "after_rename_before_directory_fsync", action: "treat_outcome_unknown_and validate_active_pointer" },
    { point: "after_directory_fsync", action: "active_pointer_is_authoritative" },
  ],
  rollback: {
    publication: "new_manifest_and_single_pointer_rename",
    target: "exact_historical_core_and_mfg_generation_pair",
    deletes_database_or_generation: false,
  },
};
emit("execution-contract.json", execution);
const executionDigest = domainDigest("cowd.ownership-cutover.execution.v1", execution);

emit("canonicalization.json", {
  schema_version: 1,
  manifest_domain_separator: "cowd.ownership-cutover.manifest.v1\\0",
  execution_domain_separator: "cowd.ownership-cutover.execution.v1\\0",
  object_keys: "UTF-8 lexical ascending",
  arrays: "preserve order",
  whitespace: "none",
  manifest_excluded_fields: ["manifest_digest"],
  digest_encoding: "sha256: followed by 64 lowercase hexadecimal digits",
});

const digestSchema = { type: "string", pattern: "^sha256:[0-9a-f]{64}$" };
const identifierSchema = { type: "string", minLength: 1, maxLength: 256, pattern: "^[A-Za-z0-9._:-]+$" };
const relativeSchema = { type: "string", minLength: 1, maxLength: 1024, pattern: "^(?!/)(?!.*(?:^|/)\\.\\.?/)(?!.*\\\\)[^\\u0000]+$" };
const schema = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  $id: "https://cowd.dev/contracts/ownership-cutover/v1/active.schema.json",
  title: "OwnershipCutoverManifestV1",
  type: "object",
  additionalProperties: false,
  required: ["schema_version", "contract_id", "ownership_contract_digest", "execution_contract_digest", "publication_generation", "activation_fence_id", "source", "core", "mfg", "execution_receipts", "state_receipts", "created_at", "activation_kind", "manifest_digest"],
  properties: {
    schema_version: { const: 1 },
    contract_id: { const: CONTRACT_ID },
    ownership_contract_digest: { const: OWNERSHIP_DIGEST },
    execution_contract_digest: digestSchema,
    publication_generation: identifierSchema,
    activation_fence_id: identifierSchema,
    source: { $ref: "#/$defs/source" },
    core: { $ref: "#/$defs/target" },
    mfg: { $ref: "#/$defs/target" },
    execution_receipts: { $ref: "#/$defs/executionReceipts" },
    state_receipts: { $ref: "#/$defs/stateReceipts" },
    previous: { $ref: "#/$defs/previous" },
    created_at: { type: "string", format: "date-time", pattern: "Z$" },
    activation_kind: { enum: ["active", "rollback"] },
    rollback_target_manifest_digest: digestSchema,
    manifest_digest: digestSchema,
  },
  $defs: {
    digest: digestSchema,
    identifier: identifierSchema,
    relativePath: relativeSchema,
    backend: { enum: ["sqlite", "postgres"] },
    counts: {
      type: "object", additionalProperties: false,
      required: ["tables", "mfg_objects", "core_objects", "reconciliation", "excluded"],
      properties: {
        tables: { const: 46 }, mfg_objects: { type: "integer", minimum: 0 },
        core_objects: { type: "integer", minimum: 0 }, reconciliation: { type: "integer", minimum: 0 }, excluded: { const: 3 },
      },
    },
    source: {
      type: "object", additionalProperties: false,
      required: ["backend", "namespace", "source_version", "schema_version", "maintenance_fence_id", "snapshot_relative_path", "snapshot_whole_digest", "snapshot_file_digest", "export_receipt_relative_path", "export_receipt_digest", "core_section_digest", "mfg_section_digest", "counts"],
      properties: {
        backend: { $ref: "#/$defs/backend" }, namespace: { $ref: "#/$defs/identifier" }, source_version: { $ref: "#/$defs/identifier" },
        schema_version: { type: "integer", minimum: 1 }, maintenance_fence_id: { $ref: "#/$defs/identifier" },
        snapshot_relative_path: { $ref: "#/$defs/relativePath" }, snapshot_whole_digest: { $ref: "#/$defs/digest" }, snapshot_file_digest: { $ref: "#/$defs/digest" },
        export_receipt_relative_path: { $ref: "#/$defs/relativePath" }, export_receipt_digest: { $ref: "#/$defs/digest" }, core_section_digest: { $ref: "#/$defs/digest" }, mfg_section_digest: { $ref: "#/$defs/digest" }, counts: { $ref: "#/$defs/counts" },
      },
    },
    target: {
      type: "object", additionalProperties: false,
      required: ["backend", "namespace", "generation", "relative_path", "section_digest", "durable_import_receipt_relative_path", "durable_import_receipt_digest"],
      properties: {
        backend: { $ref: "#/$defs/backend" }, namespace: { $ref: "#/$defs/identifier" }, generation: { $ref: "#/$defs/identifier" }, relative_path: { $ref: "#/$defs/relativePath" },
        section_digest: { $ref: "#/$defs/digest" }, durable_import_receipt_relative_path: { $ref: "#/$defs/relativePath" }, durable_import_receipt_digest: { $ref: "#/$defs/digest" },
      },
    },
    executionReceipts: {
      type: "object", additionalProperties: false,
      required: ["maintenance_lock_receipt_digest", "gateway_stopped_receipt_digest", "apps_stopped_receipt_digest", "legacy_readonly_receipt_digest"],
      properties: Object.fromEntries(["maintenance_lock_receipt_digest", "gateway_stopped_receipt_digest", "apps_stopped_receipt_digest", "legacy_readonly_receipt_digest"].map((key) => [key, digestSchema])),
    },
    stateReceipts: {
      type: "object", additionalProperties: false,
      required: ["staged_receipt_digest", "verified_receipt_digest"],
      properties: Object.fromEntries(["staged_receipt_digest", "verified_receipt_digest"].map((key) => [key, digestSchema])),
    },
    previous: {
      type: "object", additionalProperties: false,
      required: ["publication_generation", "manifest_digest", "core_generation", "core_relative_path", "mfg_generation", "mfg_relative_path"],
      properties: { publication_generation: identifierSchema, manifest_digest: digestSchema, core_generation: identifierSchema, core_relative_path: relativeSchema, mfg_generation: identifierSchema, mfg_relative_path: relativeSchema },
    },
  },
  allOf: [
    { if: { properties: { activation_kind: { const: "active" } } }, then: { not: { required: ["rollback_target_manifest_digest"] } } },
    { if: { properties: { activation_kind: { const: "rollback" } } }, then: { required: ["rollback_target_manifest_digest"] } },
  ],
};
emit("active.schema.json", schema);

function sourceFixture(label, timestamp) {
  const snapshot = JSON.parse(readFileSync(join(ownershipRoot, "golden/comprehensive-snapshot.json"), "utf8"));
  const frozenReceipt = JSON.parse(readFileSync(join(ownershipRoot, "golden/comprehensive-receipt.json"), "utf8"));
  if (label !== "v0") {
    snapshot.source.source_version = `fixture-${label}`;
    snapshot.source.maintenance_fence_id = `fence-${label}`;
    snapshot.source.exported_at = timestamp;
    snapshot.whole_snapshot_digest = "";
    snapshot.whole_snapshot_digest = domainDigest("cowd.ownership.snapshot.v1", snapshotWithout(snapshot, "whole_snapshot_digest"));
  }
  const snapshotText = pretty(snapshot);
  const snapshotFileDigest = digestBytes(Buffer.from(snapshotText));
  const counts = structuredClone(frozenReceipt.counts);
  const receipt = structuredClone(frozenReceipt);
  if (label !== "v0") {
    receipt.generation = `source-${label}`;
    receipt.snapshot_file_digest = snapshotFileDigest;
    receipt.source.source_version = snapshot.source.source_version;
    receipt.source.maintenance_fence_id = snapshot.source.maintenance_fence_id;
    receipt.source.exported_at = timestamp;
    receipt.receipt_digest = "";
    receipt.receipt_digest = domainDigest("cowd.ownership.export-receipt.v1", snapshotWithout(receipt, "receipt_digest"));
  }
  const base = `golden/source/${label}`;
  emit(`${base}/snapshot.json`, snapshotText);
  emit(`${base}/export-receipt.json`, receipt);
  return {
    backend: receipt.source.backend, namespace: receipt.source.namespace, source_version: snapshot.source.source_version, schema_version: 1,
    maintenance_fence_id: snapshot.source.maintenance_fence_id,
    snapshot_relative_path: `${base}/snapshot.json`, snapshot_whole_digest: snapshot.whole_snapshot_digest, snapshot_file_digest: snapshotFileDigest,
    export_receipt_relative_path: `${base}/export-receipt.json`, export_receipt_digest: receipt.receipt_digest,
    core_section_digest: snapshot.core_matrix_domain.section_digest, mfg_section_digest: snapshot.mfg_domain.section_digest, counts,
  };
}

function snapshotWithout(value, key) {
  const result = structuredClone(value);
  delete result[key];
  return result;
}

function target(owner, generation, source, timestamp) {
  const backend = "postgres";
  const namespace = owner === "core" ? "cowd_core" : "cowd_mfg";
  const sectionDigest = owner === "core" ? source.core_section_digest : source.mfg_section_digest;
  const relativePath = `golden/generations/${owner}-${generation}`;
  const receiptPath = `${relativePath}/import-receipt.json`;
  const importedObjectCount = owner === "core" ? source.counts.core_objects : source.counts.mfg_objects;
  const receipt = {
    schema_version: 1, owner, backend, namespace, generation: `${owner}-${generation}`,
    ownership_contract_digest: OWNERSHIP_DIGEST,
    section_digest: sectionDigest, source_snapshot_whole_digest: source.snapshot_whole_digest,
    source_version: source.source_version, source_schema_version: source.schema_version,
    maintenance_fence_id: source.maintenance_fence_id, counts: source.counts,
    target_checkpoint: {
      source_generation: source.source_version,
      imported_object_count: importedObjectCount,
      reconciliation_count: source.counts.reconciliation,
      journal_digest: digestBytes(Buffer.from(`${owner}:${generation}:${source.snapshot_whole_digest}:journal`)),
    },
    durable: true, completed_at: timestamp,
  };
  const receiptText = pretty(receipt);
  emit(receiptPath, receiptText);
  return { backend, namespace, generation: `${owner}-${generation}`, relative_path: relativePath, section_digest: sectionDigest, durable_import_receipt_relative_path: receiptPath, durable_import_receipt_digest: digestBytes(Buffer.from(receiptText)) };
}

const fixedReceipts = {
  maintenance_lock_receipt_digest: digestBytes(Buffer.from("maintenance-lock")),
  gateway_stopped_receipt_digest: digestBytes(Buffer.from("gateway-stopped")),
  apps_stopped_receipt_digest: digestBytes(Buffer.from("apps-stopped")),
  legacy_readonly_receipt_digest: digestBytes(Buffer.from("legacy-readonly")),
};
function stateReceipts(label) {
  return {
    staged_receipt_digest: digestBytes(Buffer.from(`${label}:staged`)),
    verified_receipt_digest: digestBytes(Buffer.from(`${label}:verified`)),
  };
}

function seal(manifest) {
  manifest.manifest_digest = domainDigest("cowd.ownership-cutover.manifest.v1", snapshotWithout(manifest, "manifest_digest"));
  return manifest;
}

function manifest(label, timestamp, source, core, mfg, previous = undefined, rollbackTarget = undefined) {
  const value = {
    schema_version: 1, contract_id: CONTRACT_ID, ownership_contract_digest: OWNERSHIP_DIGEST,
    execution_contract_digest: executionDigest, publication_generation: `publication-${label}`, activation_fence_id: `activation-fence-${label}`, source, core, mfg,
    execution_receipts: fixedReceipts, state_receipts: stateReceipts(label),
    created_at: timestamp, activation_kind: rollbackTarget === undefined ? "active" : "rollback", manifest_digest: "",
  };
  if (previous) value.previous = previous;
  if (rollbackTarget) value.rollback_target_manifest_digest = rollbackTarget;
  return seal(value);
}

function summary(value) {
  return { publication_generation: value.publication_generation, manifest_digest: value.manifest_digest, core_generation: value.core.generation, core_relative_path: value.core.relative_path, mfg_generation: value.mfg.generation, mfg_relative_path: value.mfg.relative_path };
}

const source0 = sourceFixture("v0", "2026-08-15T00:00:00Z");
const active0 = manifest("v0", "2026-08-15T01:00:00Z", source0, target("core", "v0", source0, "2026-08-15T00:30:00Z"), target("mfg", "v0", source0, "2026-08-15T00:31:00Z"));
const source1 = sourceFixture("v1", "2026-08-15T02:00:00Z");
const active1 = manifest("v1", "2026-08-15T03:00:00Z", source1, target("core", "v1", source1, "2026-08-15T02:30:00Z"), target("mfg", "v1", source1, "2026-08-15T02:31:00Z"), summary(active0));
const rollback2 = manifest("v2", "2026-08-15T05:00:00Z", source0, active0.core, active0.mfg, summary(active1), active0.manifest_digest);
emit("golden/history/active-v0.json", active0);
emit("golden/history/active-v1.json", active1);
emit("golden/history/rollback-v2.json", rollback2);
emit("publication/active.json", active0);

const tamperCases = new Map();
function tamper(name, mutate, reseal = true) {
  const value = structuredClone(active0);
  mutate(value);
  if (reseal) seal(value);
  tamperCases.set(name, value);
}
tamper("section.json", (v) => { v.core.section_digest = digestBytes(Buffer.from("wrong-section")); });
tamper("receipt.json", (v) => { v.core.durable_import_receipt_digest = digestBytes(Buffer.from("wrong-receipt")); });
tamper("fence.json", (v) => { v.source.maintenance_fence_id = "fence-tampered"; });
tamper("path.json", (v) => { v.source.snapshot_relative_path = "../escape.json"; });
tamper("backend.json", (v) => { v.core.backend = "sqlite"; v.core.namespace = "legacy_main"; });
tamper("count.json", (v) => { v.source.counts.tables = 45; });
tamper("generation.json", (v) => { v.mfg.generation = v.core.generation; });
tamper("previous.json", (v) => { v.previous = summary(v); });
tamper("manifest-digest.json", (v) => { v.source.source_version = "tampered"; }, false);
tamper("unknown-field.json", (v) => { v.delete_databases = true; });
tamper("half-stage.json", (v) => { v.mfg.durable_import_receipt_relative_path = "golden/generations/mfg-v0/missing.json"; });
tamper("replay.json", (v) => { v.previous = summary(v); });
tamper("rollback-delete-attempt.json", (v) => { v.activation_kind = "rollback"; v.rollback_target_manifest_digest = active0.manifest_digest; v.delete_previous_generation = true; });
tamper("rollback-target.json", (v) => { v.activation_kind = "rollback"; v.rollback_target_manifest_digest = digestBytes(Buffer.from("unknown-target")); });
const checkpointReceipt = JSON.parse(files.get(active0.core.durable_import_receipt_relative_path));
checkpointReceipt.target_checkpoint.imported_object_count += 1;
const checkpointReceiptText = pretty(checkpointReceipt);
const checkpointReceiptPath = "golden/tamper-assets/core-checkpoint-receipt.json";
emit(checkpointReceiptPath, checkpointReceiptText);
tamper("target-checkpoint.json", (v) => {
  v.core.durable_import_receipt_relative_path = checkpointReceiptPath;
  v.core.durable_import_receipt_digest = digestBytes(Buffer.from(checkpointReceiptText));
});
for (const [name, value] of tamperCases) emit(`golden/tamper/${name}`, value);

emit("contract-manifest.json", {
  schema_version: 1, contract_id: CONTRACT_ID, ownership_contract_digest: OWNERSHIP_DIGEST,
  execution_contract_digest: executionDigest,
  artifacts: [...files.keys()].sort(),
  tamper_cases: [...tamperCases.keys()].sort(),
  production_acceptance: "requires_an_offline_coordinator_that_uses_this_validator_and_execution_contract",
});

let failures = 0;
for (const [path, content] of files) {
  const destination = join(root, path);
  if (check) {
    if (!existsSync(destination) || readFileSync(destination, "utf8") !== content) {
      console.error(`stale generated artifact: ${path}`);
      failures += 1;
    }
  } else {
    mkdirSync(dirname(destination), { recursive: true });
    writeFileSync(destination, content);
  }
}
if (check) {
  const generated = new Set(files.keys());
  function walk(directory, prefix = "") {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      if (entry.name === "generate.mjs") continue;
      const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
      if (entry.isDirectory()) walk(join(directory, entry.name), relative);
      else if (!generated.has(relative)) {
        console.error(`unexpected generated artifact: ${relative}`);
        failures += 1;
      }
    }
  }
  walk(root);
}
if (failures > 0) process.exit(1);
