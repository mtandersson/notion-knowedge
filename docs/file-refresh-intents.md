# File-source ownership and derived refresh

`AuthorizedFileIntent` is a trusted server capability for the file workflow in
#477. This foundation does not install an upload tool. Its `begin` API takes the
approved HTTP authorization and operator-owned physical scope, rechecks the
original grant and root ancestry, generates its own non-secret operation digest,
and atomically writes an intent and held OAuth refresh job in operational state.
It never accepts credentials, authority, audit identity or file URLs from input.

The owner obtains its Notion client through `client()`. Each actual provider
attempt validates current request/grant authority, cancellation and the SQLite
owner fence after transport admission. A local attempt guard stays alive through
response receipt consumption. `settle` refuses to run while an attempt is locally
live. Mutation attempts leave sticky evidence: after a successful or uncertain
send, `settle(false)` cannot discard the work. Reads alone do not manufacture a
possible mutation. The ordinary Notion transport and semantic refresh contracts
retain their existing behavior.

A source lease is between 1 and 300 seconds and must be renewed before its exact
expiry. The accepted owner has an absolute one-hour deadline. Renewal cannot
revive an expired owner or cross this deadline. At most 64 held intents per
workspace are accepted; exhausted capacity denies new file admission rather than
losing recovery work. The caller must stop new source work if renewal fails.
Expired/stale owners cannot send, renew or change a held intent's settlement.
Repeated identical successful settlement is harmless; conflicting settlement is
rejected.

After source work returns, `settle(true)` releases read-only refresh for verified
or possible effects. A still-live owner with no mutation attempts may
`settle(false)`; that discarded intent is terminal and cannot be retried into an
index job. Grant or request authority lost before settlement leaves the accepted
intent held. Index eligibility is separate from any source success or partial
receipt; neither an upload receipt nor queue admission proves an attachment or
fresh local index.

Dropping a request or reopening the database does not establish that a remote
mutation has finished. Owner expiry therefore changes the inspection status from
`Held` to `RecoveryRequired` but never makes a held job claimable. Even a locally
live effect crossing expiry cannot be marked completed by the refresh worker.
Operational state and safe audit must be retained while recovery is unresolved.

Recovery requires a trusted operator to stop source owners in every relevant
process and establish provider quiescence. The server
`reconcile_quiescent(..., SourceQuiescence::OperatorConfirmed)` API additionally
checks there is no locally live attempt, validates current approval for the
original grant, and obtains fresh physical-root evidence under the same policy.
It only releases an already accepted, expired intent. There is no tool argument
or automatically inferred timeout that supplies this proof. This foundation
exposes the API; the installed workflow/recovery surface remains #477.

After release, the existing grant worker still validates the original grant,
workspace, owner, epoch and policy before source reads and guarded index commits.
Revocation, replacement or unavailable credentials do not switch to an
integration token. Refresh retries read and index only: they never replay an
upload or attachment. SQLite queue generation, claim leases and retry limits
continue to fence old workers.

The journal contains only workspace/digest keys, an opaque local owner, bounded
ownership times and held/released/discarded phase. Original non-secret grant and
page identity are in the associated queue record. No filename, URL, content,
file bytes or credential is stored. Preserve this database during model/index
rebuilds. Upgrade old operational state with the explicit backed-up
[offline migration command](operational-state-migration.md); read-only startup
continues to reject an older schema.
