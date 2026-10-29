# CI test performance

Notion is authoritative and retrieval is a disposable cache owned by one service
process. Routine tests cover authorized synchronization, idempotent updates,
reset/resync, search admission, provenance and protected external-effect records.
The obsolete candidate/handoff/rollback matrix and its dedicated fixtures were
removed in #577/#578. Real Notion mutations and deployment measurements remain
separate operator acceptance.

## Measured cleanup baseline

Source head `35efa2f3b882e012b7f9641e6b40da76b47d443c`, CI run
[40732238247](https://github.com/mtandersson/notion-knowedge/actions/runs/40732238247),
passed before merging #578. Durations below distinguish compilation from test
execution; they are observations from this run, not portable machine guarantees.

| Command or suite | Compilation | Execution / separate step |
| --- | --- | --- |
| Retrieval with `local-lancedb`, main `cargo test` | 9m59s | About 53s across its test binaries and doc tests |
| Retrieval library in that command | Included above | 89 passed, two ignored subprocess helpers; 9.94s |
| Persisted compression compatibility | Included in its own harness | Separate step 1m53s, including setup/build work |
| Server with `webhook-worker`, main `cargo test` | 10m08s | About 76s across its binaries and doc tests |
| Authoritative reconciliation command suite | Included above | 26 passed; 26.06s |
| Webhook/reader integration suite | Included above | 28 passed, one ignored subprocess helper; 12.22s |
| Production-evaluation storage/authorization example | Additional 7m02s | Three passed; 4.86s |

The ignored helpers are invoked deliberately by their owning tests. They do not
represent deferred product acceptance. #583 subsequently adds one ordinary sync
repair case, so its merged server count differs from this pinned baseline.

The preceding #577 run
[40719531116](https://github.com/mtandersson/notion-knowedge/actions/runs/40719531116)
had 126 passing retrieval-library tests and 43 ignored cases, taking 70.19s
against the same isolated LanceDB feature. After source cleanup the library has
89 passing tests and two ignored helpers, taking 9.94s. Compilation changed from
10m07s to 9m59s; the source removal primarily reduced unnecessary test execution.

Two explicit over-60-second warnings named
`candidate::killed_transition_recovers_every_durable_phase_in_a_fresh_process`
and `candidate::source_capture::killed_transition_source_capture_uses_actual_sql_authority_after_restart`.
They specified durable offline candidate phases and retained source handoff,
whose operator examples were not shipping server callers. #578 removed that
framework and its fixtures. The replacement is the normal authoritative
reconciliation/cache-reset suite above, including retry, scope/provenance and
preservation of external-effect records. Ordinary directory/lease and
external-write safety checks remain covered.

The old recovery lane in run 40646750809 exhausted its 60-minute job limit.
That does not isolate an individual test cost. Its retired subprocess/archive
matrix is superseded by bounded reset/resync acceptance, not by a larger timeout.

## Native execution pipeline

Four logical test suites remain. Workspace and Qwen continue to compile/run
independently, without a dependency on the native build or release artifact.
The native build job compiles server and retrieval tests plus the production
evaluation example in one Cargo invocation. It uses the shipping server feature
set (`webhook-worker`, enabling local Qwen and LanceDB). Retrieval's ordinary
native behavior does not depend on the Qwen feature; the existing separate
Clippy step still checks standalone LanceDB compilation. The separate Qwen
suite retains provider-only and Qwen/e5 example coverage.

The build job also runs Rust doc tests and the existing original/patched
compression fixture. Those checks intentionally need a compiler and stay in the
build job. The two native execution jobs download only their suite's compressed
executable archive. They restore no Cargo cache and run no Cargo command.
`nix develop .#test-runtime` supplies Python, OpenSSL and C++ runtime libraries,
without a Rust toolchain, protobuf compiler or C compiler.

`native-test-artifacts.py` consumes Cargo's executable metadata, never a scan of
the build cache. Archives include the ordinary server binary required by
`CARGO_BIN_EXE` in integration tests. They retain checkout-relative executable
paths for subprocess helpers and embedded fixture paths. Builder and runners
check out the same workflow SHA at the same workspace path; the manifest checks
that revision/path, the suite and each executable digest before execution.
Only regular executable files are unpacked under `target/debug`; archives do not
contain compiler fingerprints, incremental scratch or dependency build caches.
They expire after one day. Execution timing reports are retained separately.

Each executable has a 180-second ceiling; each suite has a 240-second execution
budget. These leave margin above the pinned baseline's approximately 53/76-second
suite executions. The ten-minute execution-job limit also covers artifact
transfer, Nix runtime setup and integrity checks. The native build retains the
previous 30-minute compilation limit. These are initial budgets to validate with
the new pipeline, not a measured claim that the new pipeline has already passed.
A failure, timeout, missing artifact or skipped selected native job blocks the
unchanged final CI gate. No native test is silently disabled.

## Delivery measurements

Each run emits per-binary execution timing reports and a compiled executable
footprint. The accepted head, job/report identifiers and before/after comparison
are recorded in the [delivery PR](https://github.com/mtandersson/notion-knowedge/pull/589)
before #579 closes. Those results must prove real native execution in the Nix
runtime and practical budgets; packaging fixtures and this baseline alone do
not establish a speedup or complete delivery.

## Executable archive compression

Native executable archives use Zstandard level 3 through the pinned Nix Python
3.14 runtime. The existing `line-tables-only` Rust debug profile is unchanged:
executables retain filename/line backtraces, symbols and executable contents.
No stripping or rebuilding occurs when packaging or extracting an archive.
The [Cargo profile contract](https://doc.rust-lang.org/cargo/reference/profiles.html#debug)
describes the retained diagnostics, and [Python's archive documentation](https://docs.python.org/3.14/library/tarfile.html)
describes its Zstandard support. Integrity contracts exercise both codecs in the
compiler-free Nix shell before the native build proceeds.

Footprints report each suite's executable bytes, archive bytes, codec and
`pack_seconds` (hashing plus archive creation). Runner reports retain the
execution-only `seconds` field and add `unpack_seconds` (decompression, extraction
and digest verification) and `total_seconds` (unpack plus execution and harness
overhead). GitHub step timestamps supply upload and download durations. These
measurements do not include compiler setup in test-execution time.

The default for PR and main CI is Zstandard. A trusted manual run can select
the prior gzip level-1 baseline without changing the source or test inventory:

```sh
gh workflow run ci.yml --ref <measurement-branch> -f native-artifact-codec=gzip
gh workflow run ci.yml --ref <measurement-branch> -f native-artifact-codec=zstd
```

The codec does not affect the Cargo snapshot key or compiler settings. Both
manual runs use the same required CI jobs and native suites; the input does not
skip checks or add test lanes. Artifact paths retain their suite and source
identity, safe extraction and individual file digests. Retention and runtime
budgets are unchanged. Measured comparison evidence is recorded in
[PR #592](https://github.com/mtandersson/notion-knowedge/pull/592); until both hosted
runs finish, this is a candidate compression change, not a measured speedup.
