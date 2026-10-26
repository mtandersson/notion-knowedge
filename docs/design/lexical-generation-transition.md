# Offline lexical generation transition

Status: implementation in progress for #504. Activation-journal preparation is
available through `lexical-candidate prepare-activation`; activation,
reconciliation and rollback commands remain incomplete.
Pre-quarantine cancellation is available through `abort-prepared`, with explicit
retained-history retry. It cannot undo committed quarantine or publication.
The multilingual lexical expansion contract remains binding. This document
specifies activation ownership and recovery; it does not enable the runtime.

## Authority and prerequisites

Stop every server, webhook, reconciliation, file and mutation process using the
source state before activation or recovery. The operator must explicitly attest
to that shutdown. Index locks fence guarded reads and derived commits; they
cannot stop a previously running process from sending an unrelated Notion write.
These commands have no credentials, source clients or network capability.

Implementation prerequisite: production currently conflates physical commit
generation with `LifecycleScope.generation`. `production_scope` hashes the
latter, and planner admission requires it to equal both commit generation and
epoch. A candidate with an unchanged policy hash and fresh physical generation
cannot satisfy those checks. Separate physical fencing from the preserved
source-policy generation explicitly across production admission, prepared
evidence and reconciliation. Do not change root/exclusion policy or recompute
its scope hash merely to make a new index admissible. This compatibility work
must be verified before declaring #504 or parent #491 complete.

The source and candidate must pass the existing bounded candidate verifier with
the same workspace, table, root policy, canonical rows and exact vector bytes.
SQLite remains at its original path and inode. No operational database restore,
scope supersession, receipt deletion or source-effect replay occurs.

Acquire a stable exclusive lock on the existing operational state file, then
exclusive locks on both source and candidate directory inodes, then exclusive
SQLite ownership. Use nonblocking acquisition and reject live leases. Validate
all opened descriptors against captured identities and reject symlinks,
unexpected hardlinks, writable parents and path replacement. Keep ownership
through final validation and publication. Existing pinned readers must release
their shared lock before the transition can start.

## Durable record and phases

The library's `prepare_activation` builds a Prepared journal from a verified
sibling and a fresh activation-time SQLite backup. It validates artifact
locations before creating a backup, bounds backup bytes under SQL ownership,
and captures original state identity, exact binding bytes and canonical Ready
metadata. Under the transition owner it retains any previous completed history,
seals the source SQL manifest, and runs the read-only adoption preview. It does
not quarantine or activate. Errors retain artifacts for diagnosis; a valid
unchanged Prepared source remains admissible. The explicit operator command and
rollback-compatible artifact checks remain required before shipping activation.

A private, bounded, canonical JSON journal adjacent to SQLite records the state
inode/token, source and target directory identities, paths, complete binding
bytes and digests, source snapshot and ready-marker identities, asset digest,
retained backup identity, receipt adoption digest, and reserved generation/epoch
values. Atomic journal replacement fsyncs both the new file and parent. Validate
the journal itself without following links; paths alone never establish trust.
Retain completed journals as immutable transition history.
Before another transition, durably rotate the completed record into history
under the same stable ownership, preserving its backup and adoption references.
An error after rename or parent fsync may already have published a new record;
recovery rereads actual journal and SQL state rather than retrying an assumed
predecessor phase.

1. **Prepared.** Under ownership, reverify the entire candidate and its asset;
   create and verify a fresh activation-time SQLite backup. Preparation's older
   backup cannot prove the latest pending work. Check two fresh generation names
   and two increasing epochs for collisions and overflow. The immutable
   candidate's final epoch must be at least source epoch + 2; reserve source
   epoch + 1 for quarantine. Reject an earlier candidate and require preparation
   of a new one; never rewrite Ready metadata. Durably write the
   complete journal before any SQL mutation. Source admission is still valid.
2. **Quarantined.** In one SQL transaction reserve both generations in history
   and set the binding to the quarantine generation/epoch. Commit before any
   directory rename. No coordinator is issued for quarantine. This fences older
   runtimes that do not understand the journal. A crash between SQL commit and
   journal advancement is recognized from SQL, never inferred from the phase
   label alone.
3. **Source retained.** Rename the source to its recorded unused archive path
   and fsync the parent. Retain its inode and complete prior index/plan binding.
4. **Target installed.** Rename the candidate to the active path and fsync the
   parent. Install its operational anchor using the unchanged state inode/token.
   Reopen the installed physical table and validate all rows, vector bytes,
   native FTS configuration, raw/exact-ID probes, binding and pinned asset.
5. **Published.** In one SQL transaction adopt compatible derived authority and
   publish the target physical identity and final generation/epoch. Commit only
   after full installed verification. Record publication in the journal.
6. **Complete.** Use a private transition-owned validation path to reopen and
   perform the same physical, SQL, asset and implementation checks as runtime
   admission, without issuing serving or writer authority. Durably mark Complete;
   only then may ordinary admission open the generation. A different runtime
   must refuse admission rather than relabel the generation.

Normal admission rejects an unresolved transition, except an unchanged Prepared
source. Physical and SQL authority checks remain necessary even with a journal.
Every mutation rechecks the captured inode and generation; stale handles and
prepared workers cannot commit to the replacement.

## Pending work and history

Receipt operation IDs are globally unique. Adoption updates generation authority
without duplicating or replacing IDs, revisions, statuses, attempts, failures or
outcomes. Include successful receipts needed by idempotent replay, metadata
identity, relation and alias projection authority, and matching schedule
authority. Preserve proposal effects and operation links. Queue counters named
generation are independent queue fences and must not be rewritten.

Persist a bounded streaming digest/count inventory of adopted records and their
original authority, tied to the fresh verified SQLite backup. Retain the old
generation ledger. Historical reconciliation success remains historical evidence
and must not become a success for the new generation. Unfinished reconciliation
plans and prepared checkpoints contain serialized authority: translate validated
compatible derived work in the publication transaction, without acknowledging,
abandoning or dropping pending pages. Unknown plan versions, authority fields or
unsupported projection formats abort before quarantine; there is no deferred
repreparation escape path that could wedge the one-active-run constraint.
Never replay an already acknowledged Notion mutation or attachment.

Alias v1 effects include their original physical generation. Preserve those
opaque proposal hashes during adoption. Runtime comparison first requires
current receipt/marker authority, then reconstructs the complete fresh effect
while varying only an earlier generation under the identical policy. Current
hashes take the direct path. Historical comparison uses the offline-created
`(scope, epoch)` index, at most 1,024 earlier generations, bounded generation
strings and at most 64 MiB of hash input per comparison. Exceeding a bound fails
closed. Quarantine creates and validates the derived lookup index atomically;
ordinary serving never creates it. Changed aliases, configuration, namespace,
tombstone or cleanup semantics cannot match through generation compatibility.

In particular, `DurablePlan.authority` contains table/workspace/scope/generation/
epoch, and its operation IDs were originally hashed with that authority. Preserve
those IDs and the original plan in the retained backup/history; validate every
plan before translating its authority in the publication transaction. Preserve
the per-page prepared revision/action/hash/edit tuple and progress status. A
translated plan must compare future lifecycle evidence against the unchanged
source-policy generation, not the new physical epoch. Its captured native index
version must be mapped to the fully verified target snapshot only after equality
of all canonical rows has been established. Completed plans and success records
stay historical. Matching active schedules retain their run IDs, deadlines,
retry counts and statuses while adopting the fresh physical authority. No
abandon-and-restart shortcut is permitted.

The SQL adoption helper requires retained transition ownership, the exact
installed target record and quarantined binding, and the original operational
database connection. It uses a savepoint so an error cannot leave partial
receipt/projection/schedule changes. Defaults bound scanned rows across affected
tables to one million, a plan plus prepared work to 16 MiB, each projection
effect to 1 MiB, aggregate effects to 64 MiB and ownership keys to 64 MiB.
An indexed temporary ownership set avoids repeated receipt scans. Validate
complete known effect encodings and prepared-operation schemas before updates;
retain their bytes. This helper does not publish physical authority or replace
native-pair and adoption-manifest verification in the same outer transaction.

Before quarantine, `preview_publication` obtains fresh native verification and
compares the complete Prepared SQL state with the activation backup. It shares
the adoption validator for bounded projections, receipts, schedules, durable
plans and prepared work, then rolls back its temporary ownership set without
changing operational rows. A second complete comparison verifies preservation.
Unsupported work therefore fails while the old source remains admissible.
Preview is not durable authorization: publication repeats validation and checks
the resulting state and resource bounds in its actual transaction. Recovery
checks SQL authority before repeating a Prepared preview, because quarantine SQL
may already have committed before its journal advance.

Whole-database manifests read the main schema through a retained transaction;
TEMP tables cannot shadow their schemas, metadata or payload. Include persisted
schemas, internal sequence tables, typed values and observable row IDs. Preserve
duplicate multiplicity using sorted fixed-size row digests. Preflight schema,
column and row bytes and row/digest-buffer counts before payload materialization
or sorting. Exclude the derived history index only after validating its exact
main-schema profile. This raw snapshot is the basis for the backup comparison;
normalization must still check only the exact permitted transition deltas and
retain original document bytes for every untouched field and historical record.

Native transition verification runs without a serving coordinator under the
retained owner and its original SQLite transaction. It resolves the source and
target by inode before quarantine and after renames, checks the original source
anchor through its retained directory descriptor, and verifies both preparation
and activation backups. Reopen the complete canonical rows/vectors and target
native FTS profiles and ID probes, then recheck both native heads, bindings,
Ready bytes, pinned asset and journal. Returned evidence borrows the owner and
records its phase and target version. Publication must obtain fresh evidence in
its actual transaction; retaining a value does not establish permanent freshness.

## Explicit reconciliation

Reconciliation acquires the same ownership and compares actual SQL authority,
descriptor identities and verified bindings against the durable record. An
incomplete rename is not permission to guess which index is active. Missing,
corrupt, unexpected or replaced artifacts fail closed and remain available for
operator diagnosis.

Before quarantine, the unchanged old generation can continue. After quarantine
and before final publication, explicitly choose the fully verified old pair or
target pair and publish it using fresh fencing. Do not restore the SQL backup.
After final SQL publication, reconcile forward to Complete; never undo newer
acknowledgements because the final journal write was interrupted. Verify every
rename and every SQL commit crash window with a separate killed process and a
fresh recovery process.

The library's `reconcile_forward` coordinates the forward choice under retained
ownership. Each bounded step freshly verifies the native pair and inspects the
exact SQL authority tuple before selecting an operation. It distinguishes
Prepared source SQL from quarantine already committed before a journal advance,
and final SQL from an adoption that still needs publication. Impossible
phase/authority combinations fail closed. A Complete generation uses ordinary
runtime admission: later acknowledged updates need not match its original
activation backup. The killed-process fixture calls this coordinator directly.

## Rollback and asset updates

Rollback restores the previous plan and storage semantics over the latest
validated canonical rows and exact vectors. It does not rewind Notion state or
SQLite checkpoints to the archived snapshot. If source rows have changed, first
prepare a fresh candidate with the prior implementation and its pinned asset;
the archived physical index is usable directly only if its complete row snapshot
still matches. This interpretation preserves the complete index/plan pairing
while retaining acknowledged work.

Retain the previously trusted plan AND storage implementation identities.
Current code must not stamp an old plan with current storage identity or execute
an incompatible old bundle. Offline verification may check an opaque pinned
asset against the retained trusted digest; serving requires a compatible runtime
to validate its exact implementation and asset contract. An unmodified older
image that equates policy generation with physical generation cannot serve a
freshly fenced rollback. Before activation, require a rollback image/asset pair
that supports the authority separation and the prior lexical semantics. A
backported image has its own reviewed implementation and importer identities
and must produce a newly verified matching asset; it cannot relabel an old
bundle. Alternatively use a prior release already supporting this protocol.
Reject activation when no verified rollback-compatible pair is available.
Rollback, dictionary changes and planner changes all use fresh candidate
verification and the same durable transition protocol.

## Required evidence before shipping

Test real process interruption at every phase and between each durable SQL,
journal and rename operation; reconcile with fresh processes. Cover both choices
before publication and mandatory forward recovery afterward. Verify stale
handles/prepared work, path replacement, missing/corrupt artifacts, unchanged
operational inode, retained queued and completed work, resumed derived work,
rollback after later page updates, and reopened raw/exact-ID searches. No fixture
may weaken corpus judgments, enable graph ranking or claim production quality.

### Read-only capture while quarantined

`with_quarantined_source_inspection` retains the original transition owner and
actual exclusive SQL transaction, verifies the native pair, and requires the
real quarantine tuple and whole-SQL allowed-delta comparison. It locates the
original source by physical identity, including rename-before-journal windows.
The returned inspection borrows that transaction and cannot be deserialized or
escape its callback. Runtime-specific transitions use the selected executable's
native checks through the matching publication actor.

`canonical_transfer::export_quarantined` reuses the bounded canonical row/vector
format and private output protocol. Its descriptive evidence binds the actual
operation, source/configuration and operational snapshot. Capture does not
reopen a revoked serving coordinator, reserve fences, choose a pair or publish
SQL. Final SQL publication requires forward completion and refuses this capture
boundary. Verified source-choice preparation and durable choice publication
remain separate #551/#552 obligations under #519.
