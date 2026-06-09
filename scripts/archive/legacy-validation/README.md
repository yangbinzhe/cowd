# Legacy Validation Scripts

This directory keeps historical validation scripts that are no longer active
entry points.

Use the unified validation runner instead:

```bash
scripts/validate.sh unit-fast
scripts/validate.sh contract
scripts/validate.sh scenario
scripts/validate.sh release
```

The archived scripts are preserved for forensic comparison only. They should not
be wired into release gates without first checking whether the current
`contract`, `scenario`, or `release` lanes already cover the same behavior.
