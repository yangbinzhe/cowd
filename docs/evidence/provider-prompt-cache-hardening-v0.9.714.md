# Provider Prompt Cache Hardening v0.9.714 Evidence

Release status: pending

## Scope

This release closes the prompt-cache, autonomous collaboration, execution
topology, terminal evidence, installed WebUI, and release-governance work
defined by
`docs/architecture/provider-prompt-cache-hardening-v0.9.714.md`.

## Live provider evidence

- Provider/model: `deepseek/deepseek-v4-flash`, with no fallback.
- Session: `cache-canary-v0914-final3-20260902-014000`.
- Root execution: `session-ingress-graph:55de5bd715d4021403e1eb54e7fc545b`.
- Terminal projection: revision 31, cursor 1840, health `terminal`, presentation
  `committed/valid`.
- Topology: 3 Teams and 3 Agents completed; zero Skill executions. Producer
  Agents A and B started 10 ms apart. Consumer C started 252 ms after the
  later producer became terminal.
- Durable answer includes file evidence, risks, and `unresolved: []`.

Provider billing fields from all 11 known attempt outcomes:

| Cohort | Requests | Miss input | Cache read | Output | Hit ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cold-inclusive | 11 | 61,992 | 101,888 | 26,960 | 62.17% |
| Non-cold | 8 | 48,159 | 84,864 | 15,745 | 63.80% |
| Exact extension | 6 | 31,767 | 82,816 | 13,990 | 72.28% |
| Warmup not bypassed for low reuse | 7 | 36,905 | 80,512 | 20,241 | 68.57% |

The 101,888 provider-reported cache-read tokens prove that cache reuse is
active and material. The cold-inclusive 62.17% also proves that this
heterogeneous short collaboration does not meet the 90% target. First requests
for new identities, role-specific authority/evidence suffixes, and 26,960
output/reasoning tokens remain chargeable. The release does not relabel cold
requests, add padding, or omit evidence to manufacture a higher ratio.

## Deterministic evidence

- Runtime full suite: passed after repeating the asynchronous receipt test 20
  times; the complete suite passed with no failures.
- Gateway: 814 passed, 0 failed, 13 ignored; both isolation integration tests
  passed.
- Provider: 173 unit/integration tests passed; four live tests remained
  explicitly ignored.
- Edge: 390 adapter tests and 17 contract tests passed; four doc tests remained
  explicitly ignored.
- WebUI: 53 files and 443 tests passed, including API, projection, capability,
  i18n, governance and presentation gates.
- Workspace all-target check, formatting, diff and static architecture checks
  passed. The initial quick governance gate correctly rejected stale 0.9.713
  release metadata; this document and the inventory migrate that authority to
  0.9.714.
- The compiler v5 regression accepts a mechanically redundant cross-Team local
  handoff while preserving the canonical workstream dependency; review and
  dispute edges remain fail-closed.

## Installed product evidence

- Core and Edge/WebUI report version `0.9.714`.
- Playwright opened the installed Gateway WebUI, selected the durable live
  session, expanded its canonical activity tree, and observed 3 completed
  Teams, 3 completed Agents, zero Skill execution, the A/B/C identities and the
  final answer.
- The browser run had zero console errors and zero failed requests.
- Provider configuration retains Bailian only for `text-embedding-v4`;
  generation models contain no Bailian-native model and preserve the Qwen
  Token Plan provider.

The status remains pending until the final Core release binary is installed,
the service is restarted without evaluator-only environment overrides, the
installed smoke passes, both repositories are committed and the final release
gate is rerun. The release tag must not point at this pending state.
