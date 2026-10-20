# Grant-bound derived refresh (#449)

This contract supports durable refresh after a verified semantic write or a
future native file attachment. Shipping OAuth write composition remains #451;
OAuth file effects remain #446. The refresh processor itself has read authority
and cannot repeat a source mutation.

Admission requires current request-bound OAuth write permission. The queue stores
the affected page and mutation digest with non-secret grant ID, epoch, owner,
workspace and exact root/exclusion policy identity. It stores no source credential
or bearer token. Re-admission preserves the original authority and terminal state;
a changed grant, owner or scope cannot adopt existing work.

Existing integration rows retain explicit integration ownership during migration.
Integration workers claim only integration rows; OAuth workers claim only OAuth
rows. A shared workspace does not make those authorities interchangeable.

An accepted job uses live read credentials from its original grant. Credential
refresh must validate the expected binding after acquiring the refresh lock and
before making provider requests. Refreshing credentials for that same grant is
allowed; replacing the grant never transfers queued authority. Both source reads
and relation discovery use the same temporary OAuth client, with no integration
fallback. Current grant, deadline, cancellation and queue ownership are checked
again at the guarded commit boundary.

MCP token expiry, client disconnection and token-only logout do not remove the
stored Notion grant. Accepted durable work may continue under that grant. Grant
removal or replacement, changed identity or root policy, and expired credentials
that cannot be renewed prevent processing. A requester cannot use queue metadata
to create or select a new credential.

Explicit derived-work cancellation advances the claim generation and marks the
job failed. Expired worker leases can be reclaimed with a new generation; an old
claim cannot complete the replacement claim. A retry retains the original grant
binding and only retries derived indexing. Cancellation or failed indexing does
not roll back or repeat an already verified Notion mutation.

The authoritative source receipt and derived index status remain separate. Keep
successful source outcomes when refresh admission or processing fails, and expose
the derived failure rather than claiming the index is current. Operational state
and receipts must survive restart; see [index commits](index-commits.md) and
[state recovery](local-state-recovery.md).

Local SQLite, OAuth-provider and LanceDB fixtures are required evidence for this
contract. They do not establish real Notion writes or installed ChatGPT OAuth
write support.
