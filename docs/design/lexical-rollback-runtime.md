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
introduced. This implementation remains subject to #529's complete verification
and independent review before delivery. Durable evidence, explicit operator
shutdown acknowledgement, cross-restart retry and aggregate acceptance remain
#530/#528/#523; candidate rebuilding/publication remain #524/#525.

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

This primitive does not provide the operator command or killed-process retry
workflow tracked by #533. Activation, rollback publication and aggregate
acceptance remain gated by their subsequent units.
