# Reconciliation runner (retired, #699)

The durable plan runner (`AuthoritativeReconciliationRunner`, per-run plans,
receipts-per-run, `resume`/`status`/per-run `retry`) was removed. Manual,
scheduled and cache-reset reconciliation all call `IndexRuntime::sync_from_walk`;
see [manual reconciliation](manual-reconciliation.md) and
[the sync design](design/bulk-listing-sync.md). Operational state that still
exists: the writer lease, schedule rows, the page inventory and sync records.
