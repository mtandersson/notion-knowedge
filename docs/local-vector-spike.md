# Local embedding / embedded LanceDB spike (#212)

Investigation checkpoint on 2026-10-04, Linux x86_64 host `north` (NixOS,
kernel 6.18.47), CPU execution, 30 GiB RAM and swap enabled. No credentials,
live Notion content, remote embedding API, fake vectors or fake database are used.

**The real create/reopen/query proof succeeds with LanceDB 0.39.0.**
The experiment now targets **LanceDB 0.39.0 / Lance 12.0.0 / Arrow 58.4.0**,
the [current published Rust release](https://docs.rs/crate/lancedb/0.39.0)
checked on 2026-10-04. The older 0.26.2 compatibility pin from PR #217 has been
removed. The schema-v1 vector identity,
canonical chunk records and real CPU Qwen provider remain the same. See the
[reproduction commands](../crates/retrieval/spikes/qwen-lance/README.md).

## Matched Rust/LLVM toolchain

The original distro shell reports Cargo 1.98.0, rustc 1.98.1 (48a229cea),
LLVM 21.1.8. Executable code generation with Lance 12.0.0 failed:

```text
intrinsic signature mismatch for llvm.x86.avx512.vpdpbusd.512:
expected <16 x i32> (<16 x i32>, <16 x i32>, <16 x i32>)
found <16 x i32> (<16 x i32>, <64 x i8>, <64 x i8>)
```

The isolated `nix develop .#spike` shell now uses the upstream Rust 1.98.1
compiler with its bundled LLVM 22.1.8, Cargo 1.98.1 and matching Clippy/rustfmt.
The rust-overlay revision and downloaded component hashes are pinned by the
flake lock. The root, format and security shells retain their existing packages.
This fixes the actual compiler/LLVM pairing rather than changing only an LLVM
passthrough or downgrading LanceDB. No local/global Rust installation is needed.

Both compiler-only checks succeed in the spike shell:

```sh
nix develop .#spike --command rustc --edition=2024 \
  crates/retrieval/spikes/qwen-lance/compiler-reproducer.rs \
  -o /tmp/nk-avx512-reproducer
nix develop .#spike --command rustc --edition=2024 -C opt-level=3 \
  crates/retrieval/spikes/qwen-lance/compiler-reproducer.rs \
  -o /tmp/nk-avx512-reproducer-opt
```

The reproducer does not execute AVX512 instructions or require a model.
LanceDB 0.39.0 also needs the compile-time `remote` feature because its `job.rs`
uses `Error::Http` unconditionally while the variant is feature-gated. The
experiment resolves its index argument to a local absolute filesystem path
before opening the embedded database, even with that feature compiled in. Table
creation now supplies the Arrow `RecordBatch` directly to the
0.39.0 `Scannable` API, preserving its embedding schema metadata. The shell
retains protobuf (`protoc`), pkg-config and OpenSSL.

The changed spike shell evaluates on x86_64-linux, aarch64-linux and
aarch64-darwin; execution was verified only on x86_64-linux. Full flake display
still fails on the existing x86_64-darwin default shell because the pinned
nixpkgs revision dropped that platform. This pre-existing platform problem is
outside the LanceDB upgrade.

## Security observations

Fresh unsuppressed RustSec audit covers both the root and experimental lockfiles
via the unchanged `scripts/check-dependencies.sh`. No advisory exceptions were
added. The updated experimental lock no longer contains the old `lru` 0.12.5
or Tantivy 0.24.2 dependencies, removing the two prior unsoundness warnings
(RUSTSEC-2026-0253 / RUSTSEC-2026-0002).

The audit returns zero with one remaining informational warning: `paste` 1.0.15
is unmaintained (RUSTSEC-2024-0436). `cargo tree -i paste` shows multiple paths
through Candle, tokenizers, image kernels and Lance; updating LanceDB alone does
not remove it. Status zero does not resolve that warning. Production #37/#39
must remediate outstanding dependency warnings and retain all schema, evaluation,
operational and security release requirements before adoption.

## Latest-stack verification and observed resources

The final spike shell passed both compiler-only checks, all three Rust
specifications, the executable build, Clippy with `-D warnings`, and isolated
formatting. Root formatting and all five security-gate tests also passed.
The actual executable confirmed missing assets and altered asset hashes fail
without creating an index, and incompatible same-dimension persisted metadata
fails before model loading with explicit rebuild guidance.

Two separate executable processes used the existing SHA256-verified public
Qwen assets. Create wrote two actual embeddings and canonical chunk records;
query reloaded the model and reopened the table, validating the sidecar identity,
embedded schema metadata and FixedSizeList<Float32,1024> vector column. The
expected backup fixture ranked first with finite scores:

```text
chunk_id=fixture-backups cosine_similarity=0.7235765
chunk_id=fixture-tomatoes cosine_similarity=0.07236338
```

| Observation (LanceDB 0.39.0) | Create process | New query process |
| --- | ---: | ---: |
| Asset verification + model load | 34,761 ms | 34,691 ms |
| Document batch embedding (2 texts) | 15,275 ms | — |
| Query embedding | — | 7,360 ms |
| Exact vector scan/stream collection | — | 9 ms |
| Whole executable wall time (GNU time) | 50.24 s | 42.25 s |
| Peak process RSS (GNU time, KiB) | 3,548,972 | 3,549,416 |
| Process exit / swap count | 0 / 0 | 0 / 0 |

These are single observations on the same Ryzen 7 7800X3D, with an unoptimized
`debug = 0` dev profile and two compile jobs, CPU F32 inference, no explicit
runtime thread limits, downloaded assets and warm filesystem caches. They do
not establish production latency or compare release performance. The compiler
build time is separate from model runtime and memory.

Ordinary CI continues to audit both locks without compiling this isolated model
workspace or downloading assets. Run its model-free checks and the real smoke
explicitly using the reproduction README. No production requirement was waived.

## Historical compatibility-run resources (LanceDB 0.26.2)

The original compatibility-run commands from PR #217 were executed with the
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

The original compatibility executable build took 4m56s (dev/unoptimized).
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
  recorded in PR #217. All five security gate behavior tests passed, including a
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

Use the pinned matched upstream toolchain and current LanceDB stack for the
synthetic spike and subsequent experiments. The 0.26.2 compatibility fallback
is no longer the selected dependency. Production #37/#39 keep their full
criteria, including remaining dependency remediation, canonical schema and
migrations, operational resource limits, cancellation, model lifecycle and
quality gates. These synthetic fixtures do not demonstrate the complete
Notion-to-ChatGPT flow or close parent #211.
