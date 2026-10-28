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

The old recovery lane in run40646750809 exhausted its 60-minute job limit.
That does not isolate an individual test cost. Its retired subprocess/archive
matrix is superseded by bounded reset/resync acceptance, not by a larger timeout.

## Remaining #579 work

Share useful native-test compilation and distribute executable test artifacts;
runners should not restore a build cache or install a compiler just to execute
those binaries. Keep the independent workspace and Qwen suites able to start
without waiting for the native build. Preserve compression compatibility,
production-evaluation authorization checks, security checks and the final CI
gate. Record the implemented command split, after measurements and practical
budgets once the resulting pipeline has run.
