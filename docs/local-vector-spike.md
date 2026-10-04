# Local embedding / embedded LanceDB spike (#212)

Investigation checkpoint on 2026-10-04, Linux x86_64 host `north` (NixOS,
kernel 6.18.47), CPU execution, 30 GiB RAM with approximately 7 GiB available
at start and swap enabled. No credentials, live Notion content, remote embedding
API, fake vectors or fake database are used.

**The synthetic feasibility path succeeded with LanceDB 0.26.2.** Real local
Qwen vectors were persisted and searched after reopening in a new process.
The current-release 0.39.0 stack remains incompatible with the pinned compiler. See the
[reproduction commands](../crates/retrieval/spikes/qwen-lance/README.md).

## Investigated stack and reproducible compiler finding

The repository's flake lock remains the toolchain authority. The actual pinned
shell reports Cargo 1.98.0, rustc 1.98.1 (48a229cea), LLVM 21.1.8. A dedicated
`nix develop .#spike` shell adds the actual native `protoc` prerequisite found
when compiling Lance encoding, plus timing/download tools. Normal production
shells and the server composition are unaffected.

Initial attempted stack: fastembed 7.1.0 / Candle 0.11.0, LanceDB 0.39.0 /
Lance 12.0.0 / Arrow 58.4.0. LanceDB's default embedded feature build first
failed with `Error::Http` missing in `job.rs` (the enum variant is gated by
`remote`, while its uses are unconditional). Enabling the compile-time remote
feature resolved that check, with local-only database connection retained.
Executable code generation then failed in `lance-linalg`:

```text
intrinsic signature mismatch for llvm.x86.avx512.vpdpbusd.512:
expected <16 x i32> (<16 x i32>, <16 x i32>, <16 x i32>)
found <16 x i32> (<16 x i32>, <64 x i8>, <64 x i8>)
```

This is reproducible without LanceDB or model assets:

```sh
nix develop .#spike --command rustc --edition=2024 \
  crates/retrieval/spikes/qwen-lance/compiler-reproducer.rs \
  -o /tmp/nk-avx512-reproducer
```

Both default optimization and `-C opt-level=3` fail with the same diagnostic.
The reproducer only compiles an intrinsic; it does not execute AVX512 on the host.
Changing Nix's `llvmPackages` passthrough alone leaves the compiler output path
unchanged and is not a fix. Overriding its actual `llvmShared` build/host/target
inputs to LLVM 23 requires rebuilding rustc and its wrapper (dry-run observed
910.4 MiB additional fetch, 1.9 GiB unpacked). That compiler rebuild was not
started within this bounded spike. The recommendation for current-release
production adoption is to resolve the Rust/LLVM pairing and replay the compiler
reproducer plus actual embedding/index smoke before selecting the production
stack.

Compatibility experiment: published embedded LanceDB 0.26.2 / Lance 2.0.0 /
Arrow 57.3.1 on the original pinned compiler. The earlier Lance SIMD source
contains no offending `vpdpbusd` intrinsic. This is still the real Rust embedded
LanceDB SDK, with no database/service substitution. The two-text create/reopen/query proof succeeded; the version is experimental,
not a production recommendation.

## Security observations

Fresh unsuppressed RustSec audit covers both the root lock and experimental lock
via `scripts/check-dependencies.sh`. No advisory exceptions were added. The
0.26.2 lock returns status zero but reports three allowed informational warnings:
`paste` 1.0.15 unmaintained (RUSTSEC-2024-0436), and `lru` 0.12.5 unsound
(RUSTSEC-2026-0253 / RUSTSEC-2026-0002). Status zero does not mean these warnings
are resolved.

The actual dependency chain is `lru <- tantivy 0.24.2 <- lance/lance-index 2.0.0
<- lancedb 0.26.2`. [The pop panic-safety advisory](https://rustsec.org/advisories/RUSTSEC-2026-0253.html)
is fixed at >=0.18.2 and needs a panicking key Drop plus unwinding/catch_unwind;
[the mutable-iterator advisory](https://rustsec.org/advisories/RUSTSEC-2026-0002.html)
is fixed at >=0.16.3 and concerns alias invalidation. A semver-compatible 0.12
lock update cannot obtain either fix. Tantivy's store reader uses
`LruCache<usize, Block>`; the fixture path does not create or query a Tantivy
full-text index and does not invoke its stored-document reader. The dependency
remains compiled and unsound; this is an explicit residual risk of the isolated
synthetic feasibility experiment. Do not adopt this pin in production without
remediation and renewed review. This spike does not weaken #37/#39, evaluation
or security release requirements.

## Verification and observed resources

The actual Nix commands in the reproduction README were executed with the
unoptimized dev profile (`debug = 0`), two bounded compile jobs, and CPU-only
F32 inference on an AMD Ryzen 7 7800X3D (8 cores / 16 threads). Runtime thread
limits were not supplied in the command. These are single observations under
ordinary host load, not a benchmark or a production latency promise. Assets
were already downloaded; “cold start” means a new process/model instance, not
cold filesystem cache. The command did not drop OS page caches.

| Observation | Create process | New query process |
| --- | ---: | ---: |
| Asset verification + model load | 34,472 ms | 34,563 ms |
| Document batch embedding (2 texts) | 16,471 ms | — |
| Query embedding | — | 7,529 ms |
| Exact vector scan/stream collection | — | 43 ms |
| Whole executable wall time (GNU time) | 51.19 s | 42.32 s |
| Peak process RSS (GNU time, KiB) | 3,523,560 | 3,526,132 |
| Process exit / swap count | 0 / 0 | 0 / 0 |

The first command persisted two vectors and exited. The second command started
another executable process, validated schema-v1 sidecar identity, reloaded the
same SHA256-verified local model, opened the persisted `chunks` table, validated
its embedded identity and actual FixedSizeList<Float32,1024> schema, embedded
the Swedish query with the retrieval instruction, and returned:

```text
chunk_id=fixture-backups cosine_similarity=0.7235765
chunk_id=fixture-tomatoes cosine_similarity=0.07236338
```

The expected backup fixture ranked first and both scores were finite. Persisted
state was about 32 KiB; external assets about 1.2 GiB. The table also preserves
the complete canonical `IndexedChunk` JSON for each synthetic row. There is no
ANN index or lexical/hybrid query. The provider implements the actual core port
and application output-validation boundary; no fixture vectors are hard-coded.

The successful compatibility executable build took 4m56s (dev/unoptimized).
That is compiler wall time, not model peak memory. The measured model load
includes SHA256 verification and the safe owned-buffer F32 conversion. Production
#39 can investigate bounded workers, optimized kernels, mapped immutable assets
and other precision/device options while preserving model-space identity.

Verification executed:

- Three model-free Rust specifications passed (missing assets before runtime,
  same-dimension model-space mismatch, canonical synthetic chunk roundtrip).
- `check-failures.py` ran the actual binary and confirmed missing assets fail
  clearly without creating an index; altered asset hashes fail without index
  creation; incompatible persisted metadata fails with explicit rebuild guidance
  before model loading, even with the same 1024 dimensions.
- The fresh root + nested lock audits completed with the unsuppressed warnings
  described above. All five security gate behavior tests passed, including a
  vulnerable nested workspace after a clean root lock.

Run the model-free checks explicitly (ordinary CI audits both locks but does
not download assets or compile/run this isolated experimental model workspace):

```sh
nix develop .#spike --command cargo test --locked \
  --manifest-path crates/retrieval/spikes/qwen-lance/Cargo.toml -j 2
nix develop .#spike --command python3 \
  crates/retrieval/spikes/qwen-lance/check-failures.py \
  crates/retrieval/spikes/qwen-lance/target/debug/qwen-lance-spike
nix develop .#security --command ./scripts/check-dependencies.sh
```

## Decision and downstream reconciliation

Proceed with the bounded synthetic/one-page spike using this explicitly
experimental compatibility path. Local CPU Qwen inference and real embedded
LanceDB persistence/reopen/search are feasible on the observed host. This does
not resolve production #37/#39: current-release Rust/LLVM compatibility and
unsound transitive dependencies require remediation before adopting a production
pin; canonical schema/migrations, operational resource limits, cancellation,
model lifecycle and quality gates retain their full criteria. The experiment
can inform the next spike step (#213), but neither #211 nor the complete
Notion-to-ChatGPT flow has been demonstrated by these synthetic fixtures.
