# Executable rollback compatibility

This is the implementation contract for #523, under #516/#504/#491. It does
not establish compatibility for any image and does not enable activation.
Candidate rebuilding and rollback publication remain #524 and #525.

## Authority and retained evidence

The source generation is authoritative only while the original operational
SQLite identity, policy binding, physical directory identity and native source
snapshot are owned and verified by the existing offline protocol. Capture its
complete lexical binding without replacing any storage, planner, importer,
lockfile or asset identity. A canonical digest of that binding identifies the
retained evidence; it is not permission to execute an arbitrary runtime.

A legacy source without an expansion binding has no recorded implementation
identity. Absence must remain explicit. Do not synthesize a current expansion
binding or infer the previous executable from a table schema. Establish its
prior semantics using a retained, trusted runtime artifact and actual native
verification of the source. If that artifact cannot be established, compatibility
is unproven and activation must fail closed. Reading an older index through
`validate_offline_source` alone does not establish this evidence.

The operator supplies an explicitly trusted, pinned local executable (or an
immutable image reference through an explicit runner). A path, self-reported
version, claimed protocol flag or supplied SHA alone is insufficient. Hash and
retain the actual immutable executable/image identity and its exact asset pair;
verify their behavior against the owned source and a disposable probe. Do not
silently resolve image tags, search directories, fetch assets, or use a network
service as a compatibility oracle.

## Separate metadata from executable proof

Use distinct types for descriptive runtime metadata, retained source evidence
and a successfully verified runtime pair. Deserializing metadata must never
construct the verified type. The verifier produces that type only after all
checks below pass. Keep its construction private and bind it to the complete
input identities and snapshot; a stale proof cannot authorize another source,
asset, runtime, generation or policy.

The runtime descriptor must include its storage implementation and native
profiles, supported lexical semantics (including explicit legacy mode), exact
planner/executor/importer/lock identities for expansion, and the supported
policy/physical-fencing protocol. It must describe the code actually compiled
into the executable. A runtime must reject unsupported modes rather than
returning caller-supplied identities as its own.

Retaining an opaque old asset digest is allowed. Loading or executing that asset
requires the matching implementation. A compatible backport has its own
implementation/importer identities and needs a freshly produced, verified asset;
it cannot relabel an old bundle. Verification must establish the retained prior
semantics, rather than accept any current implementation with similar results
on a small fixture.

## Required executable verification

Run the pinned runtime against explicit local inputs with no credentials or
source-effect capability. The runtime must not open a normal serving listener or
modify the original operational database. Hold source ownership across evidence
capture, probe verification and the final input recheck.

1. Validate the retained bound or legacy source and the exact asset contract
   using the selected implementation. Reject incompatible storage, planner,
   importer, lock, embedding and native FTS contracts before execution.
2. Verify complete current canonical rows and exact vector bits. Prove the
   source's complete native profiles and raw/exact-ID behavior using the prior
   implementation, not merely the current reader's metadata acceptance.
3. In an isolated disposable fixture, retain one authoritative source policy but
   use a different physical generation and epoch. Exercise actual coordinator
   admission and a real native page commit with that separation. Reopen it in a
   fresh runtime process and verify the committed rows and ID probes.
4. Attempt admission and prepared work under the revoked physical generation;
   both must fail. An older runtime that equates policy generation and physical
   generation cannot pass this test by declaring a capability bit.
5. Recheck exact runtime, asset, source, state and snapshot identities before
   returning the verified pair. Replacement, corruption, interrupted execution,
   malformed/oversized output, nonzero exit or incomplete evidence fails closed.

Bound file reads, input/output sizes, row/vector verification and execution time
using explicit operator limits and existing candidate limits. Retain the owned
executable/artifact for the operation so hashing one path and executing a
replacement cannot succeed. Cleanup may remove only disposable probe artifacts;
never remove or restore source state on verification failure.

The executable probe establishes protocol behavior, not arbitrary binary trust.
The runtime artifact must already be trusted, just as the operator's installed
server is trusted. Self-attestation by an untrusted executable is not a security
boundary. The complete source/semantics identity checks remain mandatory even
when the generic fencing probe passes.

## Evidence and integration

Tests must cover actual process invocation and fresh-process reopen, compatible
bound and legacy modes, a genuinely coupled-generation runtime, wrong runtime
and asset identities, corrupt/missing/replaced files, malformed or excessive
output, timeouts and stale source evidence. Unit tests of descriptor equality
alone cannot establish executable compatibility.

#524 consumes the verified pair to rebuild prior semantics over the latest
acknowledged rows; an old archive is directly reusable only after full snapshot
comparison. #525 consumes the pair and candidate through fresh fencing and the
existing whole-SQL publication/recovery protocol. #517 exposes the complete
operator workflow. No activation command may become available without this
prerequisite, and no completed rollback claim follows from this document.

## Implemented probe boundary

The Linux `lexical-runtime` example currently exposes descriptive metadata and
an executable protocol probe. Build with `local-lancedb` and the `.#spike`
toolchain. These commands accept no serving configuration or credentials:

```sh
lexical-runtime describe legacy
lexical-runtime describe expansion EXACT_BUNDLE EXACT_SHA256
lexical-runtime probe-create NEW_PRIVATE_ROOT legacy
lexical-runtime probe-reopen EXISTING_PROBE_ROOT legacy
lexical-runtime probe-create NEW_PRIVATE_ROOT expansion EXACT_BUNDLE EXACT_SHA256
lexical-runtime probe-reopen EXISTING_PROBE_ROOT expansion EXACT_BUNDLE EXACT_SHA256
```

The new root's parent must be private and operator-owned. Creation refuses an
existing root. The runtime uses fixed fixture identities, separates policy from
physical generation, attempts stale prepared work, commits a native row and
reopens it. Expansion additionally runs the existing candidate/publication
protocol and performs a changed-row commit under the newly installed plan.
Reopen verifies every native profile and bounded canonical row/vector evidence
without startup repair or operational writes. Tests compare all file bytes
before and after reopen and refusal paths.

`PinnedRuntime` authenticates a trusted ELF executable into sealed memory before
execution, clears its environment/stdin, bounds time and combined output, and
terminates descendants before releasing the group leader's PID. The executable
limit is 1 GiB; use a bounded shipping artifact rather than a multi-gigabyte
native test binary with debug data. The selected binary and its dependencies
must already be trusted; pinning does not establish that trust.

## Ownership-scoped actual source verification

`IndexCommitCoordinator::with_owned_offline_inspection` retains the original
index lock, exclusive operational transaction and no-follow state descriptor.
Its read-only context validates SQLite's own VFS descriptor before queries and
checks the original token/binding tuple again after the callback. Existing
`with_offline_inspection` callers use the same boundary through a wrapper.
The context exposes bounded operational-byte hashing without reopening SQL or
exposing a connection. All source processes must already be stopped.

`rollback_runtime::verification::verify` borrows that context, an immutable
`PinnedRuntime`, and an optional opaque `PinnedAsset`. It captures actual native
source and SQL evidence, asks that exact runtime to inspect the source under its
own compiled storage/plan/asset contract, exercises real disposable fencing and
fresh-process reopen, then repeats the original source/state/artifact checks.
The host does not execute an old bundle with its current importer. Exact prior
identities must match; unsupported or differently identified backports are
refused rather than relabeled. A compatible artifact and its matching asset
must already be explicitly trusted and available.

The selected runtime's `inspect-source PRIVATE_REQUEST legacy|expansion ...`
entrypoint consumes bounded canonical owned configuration and opens no original
operational SQL. Requests omit SQL paths/tokens and source content. Reports are
descriptive, and parsing one never creates verified authority. They must match
the host's still-owned actual source; a generic fixture alone is insufficient.
Native source observation version 2 also binds a streaming bounded inventory of
relative paths, inodes, size, ownership, permissions and modification/change
timestamps. It checks that inventory before and after scanning, so a replaced
native file cannot reuse cached row evidence. Canonical row/vector digests and
existing candidate/transition record formats retain their previous meaning.

The resulting `VerifiedPriorRuntime` has a private constructor, no deserializer,
and a lifetime tied to its owning callback. It cannot escape that ownership or
be reconstructed from saved metadata. No serving or activation command is
introduced. The owned-source verifier was delivered by #529 (PR #531), followed
by the durable evidence primitive in #532 (PR #535) and the explicit
shutdown-acknowledged command and killed-process retry in #533 (PR #536).
Those latter two units implement #530. A serialized record still cannot
reconstruct the owned proof: every retry must freshly verify the actual source
and selected executable. Aggregate acceptance for #530/#528/#523 was reviewed
against the evidence below and closed after merge verification; candidate
rebuilding and rollback publication remain #524/#525.

The normal compiled legacy probe and actual owned-source verifier run explicitly
in CI through sealed execution. The bound normal-executable tests additionally
accept `NK_LEXICAL_TEST_ARCHIVE` pointing to the local pinned public archive;
they never download data or use Notion credentials.

### Retained descriptive evidence

`rollback_runtime::evidence::publish` accepts an explicit output path, the
still-owned offline context and a verified prior runtime. It reruns actual
source and executable verification before comparing or publishing evidence.
The bounded canonical record retains the complete prior binding (or legacy
absence), native physical inventory and row/vector observation, operational
identity/token fingerprint and bytes hash, and exact runtime/asset identities.
Parsing this metadata never creates `VerifiedPriorRuntime`.

Evidence must live outside the source index in an existing private directory.
An existing artifact is accepted only when its strict canonical metadata equals
the freshly verified observation; mismatched, partial, aliased or nonprivate
files remain untouched. New records use a private retained stage, file and
parent fsync, atomic no-replace rename and final parent fsync. Failed stages
remain diagnostics, and callers must name evidence explicitly rather than scan
for authority. All source processes must remain stopped.

This primitive is not itself an operator command or a cross-process retry
workflow; those behaviors were added separately in #533 (PR #536), as
documented below. Neither layer enables activation or rollback publication.
The completed aggregate acceptance review is recorded below.

### Explicit verification command

After stopping **all** source processes, build `lexical-verify` with
`cargo build -p notion-knowledge-retrieval --features local-lancedb --example lexical-verify`
in the documented Nix spike shell. Select the exact trusted runtime ELF and,
for bound sources, its exact matching immutable asset. No historical executable
or bundle becomes compatible merely because its digest was supplied.

Use an absolute private canonical configuration file (operator-owned regular
single-link file, mode 0600, at most 64 KiB). Its version-1 fields are
`index_directory`, `state_file`, `authority_json` (the exact canonical original
`CommitBinding` JSON from trusted configuration), `embedding`, `runtime`,
`runtime_sha256`, and `asset` (`null` for legacy, otherwise an object with
`path` and `sha256`). Unknown fields and noncanonical bytes are refused; all
paths select existing artifacts explicitly. Serialize the configuration through
`VerificationConfig` to preserve canonical field order. Never infer authority
from a directory scan.

```sh
lexical-verify /absolute/config.json --all-source-processes-stopped --evidence /private/evidence.json
lexical-verify /absolute/config.json --all-source-processes-stopped --retained /private/evidence.json
```

The first command requires an unused evidence path; the retry requires the
explicit retained file. Both freshly verify the original ownership, SQL, native
source and real executable/asset behavior before publication or identical reuse.
Refusal never rewrites stale evidence or restores a backup. Retain orphaned
`.runtime-evidence-*` stages as diagnostics after interruption; they grant no
authority and must never be selected automatically. If interruption preceded
publication, rerun with the explicitly chosen unused `--evidence` path. If the
published file exists, retry with `--retained`. New acknowledged source state
requires fresh verification and a new unused evidence path.

This command does not activate, serve, rebuild a candidate or publish rollback.

### Compatibility acceptance evidence (#530 / #528 / #523)

The following maps the original #530, #528 and #523 acceptance requirements
to their implemented boundaries and reported behavioral checks, delivered in
PRs #531, #535 and #536 (now merged). The mapping is an audit aid, not a
certificate for an arbitrary historical runtime/asset pair: every selected pair
still needs its own actual-source and executable verification.

The independent review of PR #536 confirmed this audit, including its follow-up
assertion for held file-effect leases. Its reviewed head `bcb62cf` passed
[required CI](https://github.com/mtandersson/notion-knowedge/actions/runs/40182130263)
and merged as `07b291d`. The merged tree was verified against the reviewed
implementation before #530, #528 and #523 were closed, with their native
blocking dependencies confirmed closed. Their closure records retain the
acceptance evidence and remaining boundaries.

Latest-row candidate rebuilding (#524), rollback publication (#525), and the
complete operator workflow and parent audits (#517) remain open and separately
gated. The broader #516/#504/#491 acceptance is not established by these units.

| Original requirement | Evidence boundary and behavioral check |
| --- | --- |
| Actual original authority, SQL identity/token and physical directory; complete prior binding or legacy absence; native profiles, current rows and exact vectors | `with_owned_offline_inspection`, `inspect_runtime_source`, and `verification::verify` retain original ownership and independently compare the selected runtime's complete actual-source observation before and after real probes. Both `verified_prior_runtime_*` normal-runtime tests check bound/legacy cases and unchanged original bytes. |
| Exact immutable runtime/asset pair and separate source-policy/physical fencing; retained prior semantics | `PinnedRuntime` executes a sealed exact-byte ELF; `PinnedAsset` retains and rechecks the selected asset. Descriptor/binding equality and actual native source/probe checks require matching storage/executor/importer/lock/asset identities. `probe-create` and fresh `probe-reopen` perform real native writes, reject revoked authority and exercise distinct physical generations. |
| Refuse incompatible or coupled generations, malformed/oversized reports and replacements | `refusal_and_freshness_cases` runs compiled refusing/coupled executables and real post-probe source/index/SQL/asset replacement cases. The coupled fixture forwards actual-source inspection before rejecting the distinct-generation probe. Transport tests cover output/deadline/process cleanup and hash mismatch. A replacement of the original ELF pathname cannot alter the already sealed selected executable. |
| Legacy compatibility without synthesizing a binding; no old-plan/current-storage stamp, incompatible bundle execution or backport relabeling | Explicit legacy mode uses `RuntimeDescriptor::current(None)` and requires actual binding absence. Bound mode requires complete prior binding equality. The host treats the selected asset as opaque; only the selected trusted runtime loads it. No backport or historical artifact is claimed compatible by these tests; any such pair must independently pass the same exact implementation/asset/source checks. |
| Reusable, non-deserializable ownership-bound proof; no nested SQL ownership | `VerifiedPriorRuntime` has no public constructor/deserializer. Compile-fail lifetime checks prevent escape from the retained owner. The actual-source subprocess never opens original operational SQL. Candidate rebuilding and eventual activation enforcement remain #524/#525. |
| Durable bounded private canonical evidence; strict readback, no clobber or inferred authority | `evidence::publish` freshly verifies before exact retry, uses retained staged files, file/parent fsync and atomic no-replace rename outside the source inventory. `RetainedEvidence::read` is descriptive only. Normal-runtime tests reject aliases, nonprivate/corrupt/oversized/noncanonical/unsupported nested-schema inputs without rewriting them. |
| Explicit shutdown-acknowledged operator, real interruption and fresh-process retry | `lexical-verify` requires explicit configuration and new/retained paths. Both `offline_verification_command_*` cases SIGKILL at staged-file fsync, rename-before-parent-fsync and completed publication, then retry with fresh normal command processes. Orphaned diagnostic stages remain unchanged and are never scanned for authority. |
| New acknowledged state, held leases/locks and original-store preservation | Command tests refuse missing acknowledgement/evidence, held index locks and active reconciliation leases. They commit a real newer native row and receipt through the production guard, refuse stale evidence, retain the latest source/SQL bytes and accept fresh evidence at a new explicit path. Original source/store byte inventories are compared after every crash retry and refusal. Existing ownership tests cover exclusive SQL and held file-effect leases. |
| Serving/activation and dependent acceptance remain gated | This command creates descriptive evidence only. #524 owns latest-row prior-semantics candidates; #525 owns fenced rollback publication/crash proof; #517 owns the complete operator workflow and parent audits. No graph enablement, source effects, credential use, corpus changes or quality/cutover claim is established here. |

### Canonical transfer for prior-runtime preparation

`chunks::canonical_transfer::export` consumes a retained original offline
inspection context and writes only an explicitly named unused private file
outside the native source. It reuses the native row/profile validator and
captures the original SQL identity/token fingerprint, SQL bytes hash, current
physical source observation and exact canonical row/vector digest. Source and
SQL must still match after the bounded stream is written and synced. All source
processes must remain stopped; partial files remain diagnostics.

Transfer version 1 starts with `NKROWS`, a zero byte and version byte 1, then
little-endian u32-length canonical JSON frames: one metadata header followed
by exactly the recorded number of rows. Each row contains the 16 version-1
canonical scalar string columns in schema order (including exact raw JSON
strings and nullable fields), followed by finite f32 vectors as u32 bit patterns.
No embedding or lexical execution occurs. The current source may be bound or
legacy independently of the eventual prior target plan.

`canonical_transfer::read` checks private regular-file identity, explicit limits,
complete file SHA/size, supported header/schema, bounded canonical frames,
shared row/ID/policy validation, exact row/vector digest and EOF. Its batch
callback may create derived diagnostics, but must discard any result if final
validation fails. `TransferRecord` is descriptive and deserializing it grants
no source, SQL or candidate authority; restarted reuse needs fresh ownership
and comparison. This unit does not build/select a candidate or publish rollback:
#539 owns the prior-runtime builder and #540 owns the integrated candidate and
archive-freshness proof. #524 remains open for its full acceptance.

`canonical_transfer::Reader` exposes the same validated transfer as an Arrow
`RecordBatchReader` for bounded native streaming ingestion. The callback API
uses this reader too. Consumers must exhaust the iterator and propagate any
error, including the final digest/EOF and file-identity checks; a partial read
never establishes complete validation or authority.

The transfer's current-source native backend, layout, profile and plan identities
remain descriptive and opaque to a prior consumer. Reader validation enforces
the known envelope/canonical version, embedding/text contract, logical policy,
physical generation/epoch and digest shapes without trying to open that native
backend or execute that plan. Actual native source/provenance verification still
belongs to the owning current host. This does not certify an unfamiliar backend
or historical pair. Candidate construction and integrated proof remain #539 and
#540; #524 remains open.
