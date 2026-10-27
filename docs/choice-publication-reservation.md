# Reserving a handed-off lexical choice

The offline choice API retains the original activation or runtime transition
owner after quarantine. A parsed journal or intent is descriptive metadata;
callers must supply the trusted current configuration and selected pinned
runtime/asset. Ordinary serving remains unavailable throughout reservation.

`choice::reserve_activation_publication` and
`RuntimeTransitionOperator::reserve_choice_publication` freshly verify the
active handoff, retained files, actual native source/candidate and complete
operational SQL under physical ownership and an exclusive SQL transaction.
They retain a private, bounded, no-clobber publication-start intent beside the
active journal before inserting the selected generation, scope and epoch into
the generation ledger. The intent binds the exact handoff digest, desired
authority and full pre-reservation SQL manifest.

Recovery uses actual SQL rather than assuming a journal phase proves commit:

- With no matching ledger row, the complete SQL must equal the intent preimage.
  The original freshness checks still apply before reservation.
- With a matching row, exactly its generation, scope and epoch must match.
  Omitting that one row must reproduce the complete preimage; physical scan
  budgets still charge the omitted row. Conflicts, duplicates, later epochs,
  changed schemas and any other operational changes refuse recovery.
- A hot rollback journal is recovered by SQLite only after acquiring the
  original physical owner. No application backup is restored into live SQL.

The retained choice backup identifies the immutable operational preimage after
reservation; it is not reported as the byte hash of the changed live database.
The generic live-file snapshot API retains its original meaning. Exact retry
rechecks the selected runtime, native rows/vectors, original evidence and SQL;
it does not allocate another authority or select a different choice. Missing
intent after reservation refuses instead of recreating evidence.

Reservation changes only the one generation-ledger row. It does not rename or
refence native directories, adopt operational work, publish serving SQL,
acknowledge receipts or replay Notion effects. Native installation and serving
publication remain the separate #572 and #573 requirements; #567 and its parent
acceptance stay open until the complete forward-recovery pipeline is verified.

Feature-enabled checks use `nix develop .#spike`. The non-ignored
`reservation_preimage` tests exercise exact SQL comparison and scan bounds.
The existing runtime-choice handoff tests additionally kill real subprocesses
at intent staging, rename/durability, SQL cache-flush before commit and after
commit, then retry in a fresh process. Bound cases require the normal
`NK_LEXICAL_RUNTIME` and pinned public `NK_LEXICAL_TEST_ARCHIVE`, as in CI.
