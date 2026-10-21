# Offline operational state migration

Normal HTTP and retrieval startup opens an existing current-schema operational
database. It does not create a missing database or migrate an old one. Preserve
this state separately from the disposable retrieval index.

Before upgrading, stop every server, worker and maintenance process using the
database. Let any persisted index lease expire. Keep the complete intended
server configuration, including `NK_WEBHOOK_STATE_FILE`, and choose an unused
backup path in an existing operator-owned private directory (0700):

```sh
notion-knowledge-server --migrate-operational-state --backup /var/lib/notion-knowledge/backups/state-before-upgrade.sqlite
```

This command performs no HTTP, Notion or index work. It refuses missing state,
foreign schemas, WAL mode, an active SQLite writer, live index or source-intent leases, existing
backup destinations, unsafe permissions, and storage aliases to configured
credentials or other durable stores. State and backup must be outside the
configured disposable index. It never replaces the original database inode.

Under an exclusive SQLite lock, it saves and synchronizes a private pre-upgrade
copy before applying append-only schema migrations transactionally. Preserve
that backup. On failure, stop and inspect the original and any backup; do not
remove operational state to make startup pass. A backup created before a failed
migration remains useful and is never overwritten by a retry. Select another
unused backup destination for a later attempt.

After success, start the upgraded binary with the same configuration. Migrating
the schema neither resolves uncertain source effects nor authorizes replay of
uploads. Recovery of quarantined file effects remains a separate, explicitly
validated operation under the original grant and scope.
