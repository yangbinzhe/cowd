# MFG Production Runbook

## Positioning

MFG is the operational intelligence and decision command layer above existing enterprise systems. It must not replace ERP, MES, PLM, SRM, WMS, QMS, CRM, OA, or financial systems. Its job is to connect facts, entities, metrics, evidence, decisions, actions, people, and feedback into one governed operational loop.

The production target is:

- see cross-system business state through an indicator and data network
- detect problems before they become incidents
- explain root cause with evidence and lineage
- generate a recommended handling plan from knowledge, rules, cases, and skills
- dispatch governed actions into people or systems
- close the loop through feedback and corrected data
- generate personal cockpit pages and reports based on role, attention, thresholds, and risk ownership

## Production Architecture

```text
source systems / files / messages / manual corrections
  -> source access and reconciliation layer
  -> entity and relation network
  -> fact ledger and snapshot timeline
  -> metric dependency graph
  -> incremental compute jobs
  -> attention and risk queue
  -> evidence packets and quality gates
  -> incident analysis and recommended actions
  -> governed cross-plane execution
  -> cockpit projection, reports, delivery, retry
  -> feedback, audit, and corrected source facts
```

## Required Production Inputs

Each source must have a data onboarding pack before production use:

- source name, owner, business steward, technical steward
- access mode: API, database view, file export, message topic, RPA/screen export, manual upload, or mixed
- refresh mode: batch, stream, event-driven, scheduled snapshot, or manual correction
- entity mapping: product, order, customer, supplier, component, BOM item, work order, inventory site, process, quality lot, shipment, and person
- fact mapping: fact type, dimensions, measures, timestamp, source ref, confidence, and dedup key
- metric impact: which metrics are affected by each fact type
- reconciliation rule: how conflicts are ranked across systems
- data quality rule: required fields, allowed ranges, freshness SLA, confidence downgrade, and blocking conditions
- evidence policy: what can be cited, what must be masked, and what requires approval

No API is not a blocker. MFG can ingest through database extracts, scheduled files, event copies, or operator-controlled uploads, as long as every fact has a stable source ref, timestamp, confidence, and reconciliation rule.

## Large Data Strategy

Server manufacturing data can explode quickly: BOM lines, weekly plans, components, suppliers, orders, substitutions, quality lots, inventory positions, and shipments multiply across time. Production operation must avoid asking AI to scan everything.

The core rule is: store all facts, compute only impacted metrics, surface only attention-worthy state.

Production handling:

- partition snapshots by business period, source, entity type, and product family
- generate stable signatures for BOM, plan, routing, and supplier allocation records
- ingest deltas instead of recomputing unchanged snapshots
- use metric dependency graph to recompute only affected downstream metrics
- keep hot attention queues small through thresholds, business ownership, impact score, freshness, and confidence
- keep evidence packets bounded by lineage and quality gates
- use AI for explanation, policy matching, plan generation, and exception reasoning, not as the primary bulk compute engine

For BOM and weekly plan monitoring, the minimum production metrics are:

- material shortage risk
- supplier recovery risk
- order delivery risk
- manufacturing capacity risk
- inventory exposure
- engineering change impact
- quality escape risk
- delivery commitment risk

## Operating Workflow

1. Confirm gateway and MFG health.
2. Confirm source onboarding packs and identity/grant setup.
3. Seed or load domain model and metric dependency graph.
4. Ingest facts by source snapshot or delta.
5. Run scoped metric recompute or incremental compute jobs.
6. Review hot attention queue.
7. Build evidence packets for material risks.
8. Run quality gates before analysis.
9. Create incident and operational analysis.
10. Execute recommended action through dry-run first.
11. Commit action only when grants and policy allow it.
12. Capture feedback, delivery receipt, audit record, and corrected facts.
13. Project cockpit state and deliver scheduled reports.

## Key Commands

```bash
curl http://127.0.0.1:8642/api/matrix/health
curl http://127.0.0.1:8642/api/matrix/attention/hot
curl http://127.0.0.1:8642/api/apps/mfg/cockpit/reports/<report-id>
curl http://127.0.0.1:8642/api/apps/mfg/cockpit/reports/<report-id>/delivery-state
curl -X POST http://127.0.0.1:8642/api/apps/mfg/cockpit/reports/schedules/run
```

Release validation:

```bash
scripts/v0998_mfg_production_release_gate.sh
```

## Governance

Production execution must follow these controls:

- every external action uses cross-plane preflight and idempotency
- every committed action has actor identity, grant, target ref, resource ref, receipt, and audit record
- every cockpit delivery records state and can be inspected before retry
- retry requires retryable state or explicit force mode
- report delivery should start in dry-run and move to commit only after channel configuration and approval
- production incidents must retain evidence packet id and quality gate result

## Rollout Phases

### Phase 1: Dry-Run Baseline

Outcome: MFG runs on simulated or exported data with no live commit.

Deliverables:

- source onboarding packs
- domain seed
- metric dependency graph
- release gate passing
- cockpit reports generated locally

### Phase 2: Real Data Read-Only

Outcome: MFG receives real enterprise data but does not write into systems.

Deliverables:

- real entity mapping
- reconciliation rules
- freshness and confidence dashboards
- role-based cockpit profiles
- scheduled dry-run reports

### Phase 3: Human-Governed Execution

Outcome: MFG recommends actions and sends governed notifications or tasks to people.

Deliverables:

- identity and grant registry
- policy-approved delivery channels
- retry and audit procedures
- incident feedback closure

### Phase 4: System-Governed Execution

Outcome: selected low-risk operations can be dispatched into enterprise systems.

Deliverables:

- connector action contracts
- preflight checks per action type
- rollback or compensating action plan
- operational SLA and escalation path

### Phase 5: Continuous Optimization

Outcome: MFG becomes the operating layer for strategic and daily decisions.

Deliverables:

- KPI ontology and metric ownership
- continuous case library updates
- domain skill and digital employee packs
- periodic benchmark and drift review
- executive cockpit and personal cockpit expansion

## Production Acceptance Checklist

- MFG health schema matches the current build.
- `production_operation_package` is present in health capabilities.
- Release gate passes.
- Source onboarding packs are approved by business and technical owners.
- Data reconciliation rules are approved for each cross-system conflict.
- Cockpit profiles and report cadences are approved by owners.
- Dry-run action bridge produces audit and receipt evidence.
- Commit mode is disabled unless identity, grant, target, and rollback policy are ready.

## Residual Risks

- Real source data may have inconsistent keys that require entity resolution tuning.
- Large BOM and plan workloads need production storage, indexing, and partition sizing beyond local SQLite.
- Some source systems may require non-API extraction; these paths need stronger evidence and reconciliation controls.
- AI-generated recommendations must remain gated by evidence quality, policy, and operator review before commit.
