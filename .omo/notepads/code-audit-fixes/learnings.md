# Learnings — Code Audit Fixes

## 2026-05-26: Plan Created
- Audit found 5 crash bugs, 2 poison paths, 3 deprecated allow overrides, 4 dead code items, 17 warnings
- All fixes follow TDD: verify bug exists → minimal fix → verify fix
- Wave 1: 6 parallel independent tasks (foundation + independent fixes)
- Wave 2: 3 tasks depending on Wave 1
- Wave 3: 7 parallel cleanup tasks
- critical: server mod.rs Handle::current().block_on() bugs are identical to the main.rs crash we fixed earlier
