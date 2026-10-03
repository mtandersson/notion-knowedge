# Docker PR cache investigation (#185)

This investigation preserves the unfavorable source-edit/repeat results from
[#174](ci-performance-evidence.md), the earlier [Docker evidence](docker-cache-evidence.md),
and the immutable #151/#155 foundation. It does not establish a universal Rust
PR speedup or a cache-hit guarantee. #176, #155 and #151 retain their full scope.

## What the new controls establish

The production source build, first fresh repeat and next fresh run used the same
pinned Docker/Nix/Rust dependencies, platform, VERSION, Dockerfile/allowlist,
source bytes and file/directory modes, owners and sizes. Checkout timestamps
changed; Docker ignores mtime for COPY checksums. Every recorded final-image
revision matched the actual checkout. Fresh GitHub-hosted runners retained all
Rust, security, agent and final-image coverage. Only task-owned instrumentation,
real source-edit fixtures and docs probes differ from production.

The source producer exported a 13-record/30-layer index. Its first repeat reused
the compiler RUN and final executable COPY, but the export-only second solve
retained only 11 records/21 layers. The Cargo manifest COPY, crates COPY and
compiler keys remained as dependency links without their result layers. A later
fresh production run imported that exact available index and rebuilt 87 crates.
Thus an imported manifest and a successful warm final-image build do not prove
that the export preserves reusable intermediate compilation results.

The isolated custom-base control also lost intermediate result coverage on warm
export. Its following plain repeat still hit the compiler/executable keys. This
counterexample matters: a stripped graph does **not** necessarily produce a miss.
Lazy final-image lookup can avoid materializing compilation. Index size alone
cannot predict effective reuse. Adding cache-from to the export-only solve alone
also failed to retain complete results; its continuation is recorded as cancelled
and is not counted as a completed CI control.

The explicit builder-retention experiment also failed: run 37099520213
recompiled 87 crates despite importing a complete 11,196-byte index. A later
builder-only export reused compilation but reran the executable COPY
(37101109136). Separate builder/final exports with overlapping imports still
recompiled the builder on 37101839053. None of those ineffective designs ships.

### Full graph, ineffective manager selection

Run 37101839053 imported an available own final index (11,392 bytes) and own
builder index (3,883 bytes), both containing compilation results, alongside a
stripped main index. Its final solve reused compilation and executable COPY.
The builder solve queried the Cargo key in all three managers, but `Records`
consulted only empty local/main records, not either result-bearing own manager.
Subsequent crates inputs split between main and own managers and compilation
ran again: 87 actual `Compiling` events. The archived trace contains the complete
queries, manager identities, record lookups and index bodies, rather than an
inference from timing or graph size. Effective source bytes, pins and build
arguments remained fixed.

BuildKit v0.33.1's combined cache manager deduplicates query keys while retaining
the selected key's manager associations; those associations govern subsequent
record lookup. This explains the observed suppression of available results when
overlapping graphs are combined. It does not establish that all multiple-import
solves fail, and manager identity is not a simple textual-ID overwrite. The
isolated successful counterexample and earlier own-only/main-reintroduction
controls remain in the ledger.

### Reviewed treatment and fresh-run proof

CI now selects exactly one available authorized cache index per root: own first,
trusted main otherwise, and no remote cache on absence or lookup failure. It
exports both builder and runtime roots after real smoke succeeds. The engine is
pinned to the exact previously observed v0.33.1 image digest; changing engines
was not the treatment. No fixture or debug instrumentation ships in production.

| Hosted run | Actual final compilation | Final build seconds | Builder export seconds | Final export seconds | Workflow wall / summed step seconds |
| --- | ---: | ---: | ---: | ---: | ---: |
| 37101839053, failed overlapping-import design | 0 (builder export compiled 87) | 6 | 131 | 4 | 259 / 496 |
| 37102506265, changed used source, selected roots | 87 | 122 | 23 | 5 | 250 / 558 |
| 37102810771, first fresh repeat | 0 | 4 | 1 | 3 | 102 / 362 |
| 37103058113, second fresh repeat | 0 | 5 | 3 | 5 | 112 / 358 |

Both fresh repeats actually hit compilation **and executable COPY**. Traces
show nonempty compiler/executable records in the single final manager and a
nonempty compiler root in the single builder manager. All three solves emitted
zero compilation events. Final indices retained 13 records/30 layers/11,402
bytes through generations 16→17→18; builder indices retained 7 records/9
layers/3,896 bytes through generations 3→4→5. The used-source producer and both
repeats measured identical executable SHA-256
`4bfde7133c4208fa9345656d2d5b7321142fcb3939d9b0bc223d5142b76a2552`
(8,169,096 bytes), while image revision identity matched their distinct actual
checkouts `62cf8162`, `b16ab04d` and `82338460`. Earlier used-source edits changed
the measured executable hash. A source path selector is not a binary hash.
All Rust/security/agent jobs and full runtime interactions remained enabled.

Run 37103397187 independently changed a source comment, lockfile comment and
pinned builder image after the stable exports. Each variant compiled 87 crates,
reported current revision `e98eed5712fdf77675c41a4f4a877bd5cd5a9e25`, and passed
the complete final-image smoke. These probes imported but never exported caches.
Comment edits prove input invalidation, not dependency-resolution changes; the
used-source producer proves an actual executable change. Changed VERSION and
missing VERSION checks remain part of every smoke run. The pin probe used the
immutable Rust 1.98.0 Bookworm image, not a floating tag.

These are matched cache controls, not a claim that every PR speeds up. The
source producer, expensive failed interventions, cancelled 37099087908 and all
unfavorable original measurements remain archived. Dual exports have a real
cost, reported above, and availability races may still legitimately rebuild.
The captured task-owned experiment cache snapshot contains 101 entries totaling
2,933,653,227 bytes, including Rust CI caches; 81 Docker entries total
1,238,783,480 bytes. This includes retained failed graphs and multiple benchmark
refs, not steady-state production storage. Manifest layer descriptor sizes are
compressed declared blob sizes, not measured network-transfer totals. No direct
network meter was retained; the ledger reports those descriptors and actual
load/export step costs. They are not additive repository storage because blobs
overlap. Two logical roots per
active PR retain versioned indices under GitHub quota/LRU and seven-day unused
cache eviction. No unrelated or trusted-main cache is removed.

## Original #174 contradiction and evidence limits

Original source run [37049499558](https://github.com/mtandersson/notion-knowedge/actions/runs/37049499558)
and repeat [37050012705](https://github.com/mtandersson/notion-knowedge/actions/runs/37050012705)
retain all unfavorable timings and raw logs/API data. Actual checkouts `1a68afe`
and `0497db2` differ only in a docs probe; committed Docker/Rust inputs match.
The recovered original BuildKit records confirm Dockerfile frontend 1.27.0,
BuildKit 0.33.1, identical builder/runtime pins, linux/amd64, VERSION and compiler
operations. REVISION changes final-stage identity. The archived cache API shows
a warm base index of 8,116 bytes and a source PR index of 11,396 bytes.

The exact original imported index bodies and on-runner filesystem checksums were
not retained before earlier task-owned cache cleanup. Original BuildKit history
records contain LLB/provenance/HTTP traces, but not those missing cache bodies.
Their absence prevents uniquely attributing that particular historical miss to
this mechanism or excluding an availability/input-metadata difference. The
production defect investigated here is supported by new available index bodies,
actual effective-input records and fresh runs; retrospective certainty about the
original event is not claimed. The old result remains evidence, not a relabeled hit.

## Reproduction and raw evidence

Task-owned draft PRs [#187](https://github.com/mtandersson/notion-knowedge/pull/187)
(production numbered scopes) and [#188](https://github.com/mtandersson/notion-knowedge/pull/188)
(isolated custom base) retain immutable harness commits. They never merge.
The local probe reads only authorized cache refs/permissions from runtime token
claims, hashes/stats committed context files and directories, and performs
read-only cache index lookups. It never prints token or signed-download values.
Index records include actual matched keys, body hashes, records and layer graphs.
BuildKit debug logs supplement those observations.

Collect completed run/attempt API data and sanitized logs with:

```sh
python3 scripts/collect-docker-cache-evidence.py \
  --output /tmp/docker-evidence --cache-ref refs/pull/187/merge RUN_ID...
python3 scripts/benchmark-ci.py collect --output /tmp/docker-originals RUN_ID...
python3 scripts/benchmark-ci.py summarize --input /tmp/docker-originals \
  --output /tmp/docker-timings.json RUN_ID...
```

Recalculate all observations, including invalidation controls, with:

```sh
python3 scripts/calculate-docker-cache-evidence.py \
  --input docs/docker-cache-causal-raw \
  --output /tmp/docker-cache-causal-evidence.json
```

The calculator reconstructs every recorded index body and verifies its exact
original byte count and SHA-256 before reporting result coverage. The archive
manifest hashes immutable run/attempt API data and sanitized logs. The generated
[ledger](docker-cache-causal-evidence.json) retains every control and failure.

Original debug logs remain hosted at immutable run/attempt links. Durable copies
remove signed URL queries and record original and sanitized SHA-256 hashes; cache
keys, scopes, manifests and compiler evidence remain inspectable. Six original
#174 `.dockerbuild` artifacts were retrieved through GitHub's artifact connector
when this environment's CLI storage redirect returned HTTP 403. Their extracted
OCI history/provenance/trace files, original artifact IDs/hashes and redaction
counts are retained as JSON archives. Original digest filenames are identities;
sanitation does not produce a new valid OCI artifact.

No unrelated cache is deleted, no cold state is fabricated, and no credentials,
Cargo artifacts, weaker smoke checks or relaxed scan coverage manufacture reuse.
