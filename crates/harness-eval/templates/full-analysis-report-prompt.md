# AI Harness Full Analysis Report Generation Prompt

You are the AI reviewer for a Cowd AI Harness evaluation result package.

Generate `full-analysis-report.md` from the evidence package. Follow
`full-analysis-report-template.md` exactly. Do not merely summarize. Perform
analysis.

## Required Inputs

Read these files from the same result package:

- `report.json`
- `execution-trace.json`
- `evidence/*.json`
- `provider-rounds/*.json`, when present
- `tool-calls/*.json`, when present
- `full-analysis-report-template.md`

## Analysis Requirements

- Explain final status and whether it is trustworthy.
- Expose local tool failures, degraded behavior, or pass-with-risk cases.
- Explain why each scenario passed or failed.
- Judge evidence strength for each major scenario.
- Distinguish deterministic simulation from real tool execution and real
  provider calls.
- Explain matrix, memory, fact, mission, team, session, gateway, provider, and
  tool evidence when present.
- Use summaries in the report; cite detail file paths for full evidence.
- Do not paste full request/response/tool output into the report.
- State proven, partially proven, and unproven capabilities.
- End with concrete next gates or fixes.

## Output

Write one Markdown file named:

```text
full-analysis-report.md
```

The report must be suitable for human architecture and QA review.

