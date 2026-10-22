# Operator recovery of an uncertain file batch

A disconnected or interrupted upload can leave a durable held intent. Owner
expiry reports `recovery_required`; it does not prove that Notion has finished
an accepted effect. Keep the per-batch page and digest receipt and the operational
database. Do not replay the upload to repair retrieval state.

Use the same complete OAuth file-ingestion configuration and current binary.
Stop the server and every source owner or maintenance process using these
credentials and operational state. The operator command needs exclusive access
to the sealed grant store. A new or replacement grant cannot adopt an old intent.
Schema upgrades use the separate [offline migration command](operational-state-migration.md).

If a disconnect prevented delivery of the receipt, list held intents under the
current original grant and root policy. Listing is bounded to 64 held intents:

```sh
notion-knowledge-server --file-recovery list
```

Inspect one receipt using its exact lowercase page UUID and 64-character digest:

```sh
notion-knowledge-server --file-recovery status --page PAGE_UUID --digest MUTATION_DIGEST
```

The workspace and credential come from trusted configuration and the original
accepted intent. The command reports restricted intent/index status and does
not upload bytes or attach blocks. Missing, revoked, unrenewable or replaced
authority fails closed. Ordinary credential rotation under the same approved
grant can remain valid.

Before releasing a quarantined intent, establish that all relevant source
owners have stopped and that the provider has finished any accepted request.
Inspect and reconcile the explicit Notion target as necessary. A timeout or an
expired local lease alone cannot establish this. Then explicitly assert both
conditions:

```sh
notion-knowledge-server --file-recovery release --page PAGE_UUID --digest MUTATION_DIGEST --all-source-owners-stopped --provider-quiescent
```

This operator assertion releases only existing recovery work under the original
live grant and current physical root policy. It does not create a new upload,
approve a replacement grant, or claim source success. Restart the normal server
to let its grant-bound worker refresh the page and derived index. Index work can
still fail. After restarting, call the installed OAuth MCP tool
`knowledge_file_refresh` with `page_id` and `mutation_digest` from the receipt to
inspect derived-index status. Add `"retry": true` to retry failed indexing under
the original live grant. This tool never uploads or attaches files and cannot
release source quarantine. Preserve audit and operational state until the
outcome is resolved.
