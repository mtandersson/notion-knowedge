# Reconciliation planner (retired, #713)

`AuthoritativeReconciliationPlanner` and its typed `ReconciliationPlan` were
removed. Reconciliation no longer builds a plan from a separate inventory pass;
it walks the root tree and applies differences directly through
`IndexRuntime::sync_from_walk`. See [manual reconciliation](manual-reconciliation.md),
[the sync design](design/bulk-listing-sync.md) and the
[retired runner note](reconciliation-runner.md).
