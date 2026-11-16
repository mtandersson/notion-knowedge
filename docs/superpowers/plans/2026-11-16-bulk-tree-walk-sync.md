# Bulk Tree-Walk Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Full sync of ~3,000 pages in tens of minutes rather than hours, and incremental sync in seconds. Scope comes from walking the tree down from the root. The page inventory is a local resource. The sync code and CI both get smaller.

**Architecture:** Approach A, rework the pipeline in place. One walk from the root reads each page's Markdown once. The walk finds child pages in that Markdown and finds rows through data-source queries. It records every reached page in a disposable `page_inventory` SQLite table and streams the content it already holds into the existing prefetch/embed/commit consumer. Membership is "reached in this walk", so the per-page ancestor walk, its reverse bracket, the before/after double read and the commit-time recheck are all deleted. Incremental runs use search, sorted by `last_edited_time`, only to find changed pages faster. Scope is never decided by search.

**Tech Stack:** Rust 2024 workspace (`notion-knowledge-{core,notion,retrieval,mcp,server}`), tokio, reqwest, rusqlite (bundled), LanceDB, local Qwen via the `local-qwen` feature, Nix dev shells, GitHub Actions.

**Spec:** `docs/design/bulk-listing-sync.md`

## Global Constraints

- 1 CPU, 4 GiB RAM, swap 0. Do not raise limits. Run one local model while Qwen is the backend.
- Integration auth is read-only and there is a single root. Multi-root scope, graph mode and write-back to Notion are out of scope.
- Keep these: per-page atomic commits with receipts, leases and ownership fences, the #649 page deadlines and settlement, the #654/#661 bounded prefetch (64 MiB budget, `PrefetchBudget`, `PREFETCH_WORKERS`), the #657/#660 stale retry, the #659 arena cap, and the shared limiter `INTERVAL = 334 ms` (`crates/notion/src/transport.rs:28`).
- Do not change retrieval ranking, chunking or vector identity. No re-embed is required.
- Do not add a recovery ledger, a multi-step repair state machine or retained cache generations. If state is doubtful, run a full resync.
- No silent partial syncs. Every inventory page ends as `indexed`, `skipped(<typed reason>)` or `failed`, and those totals equal the inventory size.
- Never index a page that the current walk did not reach from the root. A page that has left the root, been trashed or lost permission must be gone after the next walk.
- Logs and diagnostics carry counts and ids only, never titles or contents.
- The lifecycle evidence or digest format may change. Existing caches need one full resync, and the inventory table is disposable.
- Search is only an incremental accelerator. Full runs never call `POST /v1/search`.
- Each slice deletes the code it supersedes and records CI wall time before and after in its PR body. The baseline is main run 43786090925: 35 min.
- Use Conventional Commits, one issue per slice and PR, and start each PR as a draft (see `AGENTS.md`).
- Do not touch `notion-knowledge.service` or `/var/lib/notion-knowledge-*`. Do not run the real Qwen model in tests.

## Review Focus

1. **A child page that is visible only through `unknown_block_ids` or a truncated Markdown body.** Expected: the walk fetches the truncated blocks, still reaches the child, and records a 404 as `skipped(inaccessible)`. It does not silently drop the child. The test is in Task 1.2.
2. **A cycle or repeated id, such as a page linked as a child twice or a database row that is also a child-page tag.** Expected: the page is reached once, keeps the first parent, the walk ends, and the totals count it once. The test is in Task 1.2.
3. **A data-source query cursor that repeats or goes missing mid-walk.** Expected: the walk fails as incomplete. It does not delete inventory entries that it did not reach in that run. The test is in Task 1.3.
4. **A crash after a page commit and before the batched FTS/vector optimize.** Expected: after restart the page's rows are found by both lexical and vector search. The test is in Task 5.1.
5. **Search returns a page whose parent is not in the inventory.** Expected: the page is not indexed, the run counts it as `drift_unplaced`, and the next full walk settles it. The test is in Task 3.1.

---

## File Structure

| File | Responsibility | Slice |
|---|---|---|
| `crates/core/src/inventory.rs` (create) | Inventory value types: `InventoryEntry`, `InventoryParent`, `InventoryOutcome`, `InventoryDiff`, `InventoryCounts` | 1 |
| `crates/notion/src/walk.rs` (create) | The tree walk from the root: Markdown child extraction, data-source row queries, pruning by exclusions, streaming content | 1 |
| `crates/notion/src/crawl.rs` (delete) | The old blocks/children crawler with per-node retrieves and ancestor checks | 1 |
| `crates/retrieval/migrations/0020_page_inventory.sql` (create) | The `page_inventory` table | 1 |
| `crates/retrieval/src/sync_state.rs` (modify) | Inventory store methods; `LATEST_SCHEMA_VERSION = 20` | 1 |
| `crates/retrieval/src/planner.rs` (modify) | Plans from a `TreeWalk` and the inventory diff | 1–2 |
| `crates/notion/src/prepare.rs` (modify) | A single read with no `RootScopeGate` and no before/after check | 2 |
| `crates/notion/src/content.rs` (modify) | `read_markdown` without `fetch_page` | 2 |
| `crates/notion/src/relation_proposal.rs` (modify) | Removes the lifecycle revalidation wrappers | 2 |
| `crates/retrieval/src/refresh.rs` (modify) | Refreshes from walked content and drops the owned revalidation | 2 |
| `crates/retrieval/src/chunks/guarded.rs` (modify) | Drops the precommit source check from sync commits (2) and moves optimize out of the commit (5) | 2, 5 |
| `crates/server/src/index_runtime.rs` (modify) | One `sync_from_walk` path for initial, resync and reconciliation | 2 |
| `crates/notion/src/search.rs` (create) | `search_changed_since` | 3 |
| `crates/retrieval/src/drift.rs` (create) | Drift classification from an inventory diff | 3 |
| `crates/server/src/diagnostics.rs` (modify) | Inventory counts and the out-of-sync list endpoint | 3 |
| `crates/core/src/root_scope.rs`, `crates/notion/src/lifecycle.rs` (modify, mostly delete) | The MCP gate resolves membership from the inventory parent map | 4 |
| `crates/retrieval/src/maintenance.rs` (create) | `IndexMaintenance`: optimize every N pages or T seconds and at the end of a sync, plus compaction | 5 |
| `crates/retrieval/src/embedder.rs` (create) | `load_configured_embedder`, the one factory | 6 |
| `crates/notion-fake/` (create, dev-only crate) | The shared scripted HTTP Notion fake that replaces the two copied `HttpNotion` structs | 1 |
| `.github/workflows/ci.yml`, `.github/workflows/container.yml` (create), `scripts/ci-jobs.py`, `scripts/test-ci-jobs.py` | The required gate under 10 min | 7 |
| `crates/retrieval/tests/it/main.rs`, `crates/server/tests/it/main.rs` (create) | One integration test binary per crate | 7 |

---

## Slice 1: Tree walk and page inventory (replaces the crawler)

**Depends on:** none. **Issue scope:** walk the tree from the root, persist the inventory, and swap the planner and initial-sync discovery to the walk. Per-page preparation still calls the old lifecycle check in this slice, so the swap is safe on its own. There is deliberately no feature flag, because a flag would add code. The spec's comparison with the old lifecycle results is done in tests only.

**Deletes:**
- `crates/notion/src/crawl.rs` in full (1,746 lines, with its 25 tests). That removes `crawl_roots`, `crawl_with_exclusions`, `crawl_with_pages`, `crawl_observed`, `crawl_node`, `source_owner_in_scope` and `root_ancestor_exclusion`.
- `ScopedDiscovery` and `DiscoveryReport.pages` in `crates/core/src/discovery.rs`. Keep `SkipReason`, `SkippedContent` and `ExclusionRules`.
- The duplicated `HttpNotion`/`Model`/`ExtraPage`/`RequestPause` fakes, about 290 lines each, in `crates/retrieval/tests/authoritative_refresh.rs` and `crates/server/tests/reconciliation_command.rs`. They move into `crates/notion-fake`.

### Task 1.1: Inventory types and table

**Files:**
- Create: `crates/core/src/inventory.rs` (add `pub mod inventory;` to `crates/core/src/lib.rs`)
- Create: `crates/retrieval/migrations/0020_page_inventory.sql`
- Modify: `crates/retrieval/src/sync_state.rs:18` (`LATEST_SCHEMA_VERSION = 20`, append `(20, "page_inventory", include_str!(...))`)
- Test: `crates/retrieval/tests/sync_state.rs`

**Interfaces:**
- Produces:
  ```rust
  pub enum InventoryParent { Root, Page(String), DataSource { data_source_id: String, database_id: String } }
  pub struct InventoryEntry { pub page_id: String, pub parent: InventoryParent, pub root_id: String,
      pub last_edited_time: String, pub in_trash: bool }
  pub enum InventoryOutcome { Indexed { revision: String }, Skipped(SkipReason), Failed(FailureClass) }
  pub struct InventoryDiff { pub added: Vec<String>, pub changed: Vec<String>, pub moved: Vec<String>,
      pub removed: Vec<String>, pub trashed: Vec<String>, pub unchanged: usize }
  pub struct InventoryCounts { pub total: u64, pub indexed: u64, pub skipped: u64, pub failed: u64, pub pending: u64 }
  impl SqliteSyncStateStore {
      pub fn replace_inventory(&self, run_id: &str, entries: &[InventoryEntry]) -> Result<InventoryDiff, SyncStateError>;
      pub fn upsert_inventory_entry(&self, run_id: &str, entry: &InventoryEntry) -> Result<InventoryDiff, SyncStateError>;
      pub fn record_inventory_outcome(&self, page_id: &str, outcome: &InventoryOutcome, now_unix: i64) -> Result<(), SyncStateError>;
      pub fn inventory_entry(&self, page_id: &str) -> Result<Option<InventoryEntry>, SyncStateError>;
      pub fn inventory_parent_chain(&self, page_id: &str) -> Result<Option<Vec<String>>, SyncStateError>; // ends at root, None if not placed
      pub fn inventory_counts(&self) -> Result<InventoryCounts, SyncStateError>;
      pub fn out_of_sync_pages(&self, limit: usize) -> Result<Vec<(String, InventoryOutcome)>, SyncStateError>;
  }
  ```
- Schema: `page_inventory(page_id TEXT PRIMARY KEY, parent_kind TEXT NOT NULL CHECK(parent_kind IN('root','page','data_source')), parent_id TEXT, database_id TEXT, root_id TEXT NOT NULL, last_edited_time TEXT NOT NULL, in_trash INTEGER NOT NULL CHECK(in_trash IN(0,1)), walk_run TEXT NOT NULL, indexed_revision TEXT, last_fetched_at INTEGER, outcome TEXT CHECK(outcome IN('indexed','skipped','failed')), outcome_detail TEXT)`. The table has no title or content columns.

- [ ] **Step 1: Write the failing tests** in `crates/retrieval/tests/sync_state.rs`:
  - `inventory_replace_classifies_added_changed_moved_removed_trashed`: seed {A root, B parent A}, then replace with {A edited, B parent root, C new, D trashed}. Assert `diff.added == ["C"]`, `changed == ["A"]`, `moved == ["B"]`, `trashed == ["D"]`, and `removed` equal to the id seeded and not re-walked.
  - `inventory_counts_add_up_to_total`: after outcomes are recorded for 3 of 4 entries, assert `indexed + skipped + failed + pending == total` and `pending == 1`.
  - `inventory_parent_chain_ends_at_root_or_is_none`: for chain C→B→A→root, assert `Some(["B","A"])`. An unknown parent gives `None`. A parent cycle gives `None` and the test still terminates.
  - `dropping_inventory_table_forces_full_walk`: after `DROP TABLE page_inventory` and reopen, assert `inventory_counts().total == 0` and that migration 20 recreates the table.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-retrieval --test sync_state inventory_`. Expected: FAIL, because `inventory` is not defined.
- [ ] **Step 3: Implement** the types, the migration and the store methods. `replace_inventory` runs in one transaction. It deletes rows whose `walk_run != run_id` only when called. Callers pass only complete walks (see 1.3).
- [ ] **Step 4: Run the tests again.** Expected: PASS. Also run `cargo test -p notion-knowledge-retrieval --test sync_state` to cover the existing migration tests.
- [ ] **Step 5: Commit** with `feat(sync): add disposable page inventory table`.

### Task 1.2: Walk from the root through Markdown child references

**Files:**
- Create: `crates/notion/src/walk.rs` (`pub mod walk;` in `crates/notion/src/lib.rs`)
- Modify: `crates/notion/src/content.rs` (factor out `async fn markdown_body(&self, id: &PageId) -> Result<MarkdownBody, BackendError>` so the existing `read_content` and the walk share one parser)
- Test: unit tests in `crates/notion/src/walk.rs`, using `with_fast_fixture_transport()` like `prepare.rs`

**Interfaces:**
- Consumes: `InventoryEntry`, `InventoryParent` (1.1), `LifecycleScope`, `ExclusionRules::exclusion`, `SkipReason`, `PageContent`.
- Produces:
  ```rust
  pub struct WalkedPage { pub entry: InventoryEntry, pub content: Option<PageContent> } // content None for skipped/trashed
  pub struct TreeWalk { pub run_id: String, pub entries: Vec<InventoryEntry>, pub skipped: Vec<SkippedContent>, pub complete: bool, pub calls: u64 }
  pub(crate) enum ChildRef { Page(String), Database(String) }
  pub(crate) fn child_references(markdown: &str) -> Vec<ChildRef>; // `<page url=…>` and `<database url=…>` tags, ids via pages::uuid
  impl NotionClient {
      pub async fn walk_tree(&self, scope: &LifecycleScope, sink: tokio::sync::mpsc::Sender<WalkedPage>) -> Result<TreeWalk, BackendError>;
  }
  ```
- Per non-row page: one `GET pages/{id}` for metadata and one `GET pages/{id}/markdown`. Follow-ups happen only for `unknown_block_ids`. Rows get their metadata from the query result. Pages with children cost no extra listing call. Database rows cost about 1 + 1/100 calls each.
- Pruning: `scope.exclusions.exclusion(id, SourceType::…)` drops the subtree. `descendants_of` keeps the page and skips its children. The walk reaches each page once (a `BTreeSet` of ids), the first parent wins, and it fails closed at `MAX_NODES = 100_000`.
- Concurrency: `FETCH_CONCURRENCY = 8` through `buffer_unordered`, with every request going through the shared limiter.

- [ ] **Step 1: Write the failing tests**:
  - `child_references_extracts_page_and_database_tags_but_not_mentions`: given Markdown with `<page url="https://www.notion.so/…A">x</page>`, `<database url=…B>`, and an inline `<mention-page url=…C>`, assert `[Page(A), Database(B)]`.
  - `walk_reaches_nested_pages_and_rows_with_expected_call_count`: for root → page P → database D with 2 rows, assert that `entries` has ids {root, P, row1, row2}, the parents are Root/Page(root)/DataSource, `complete`, and `calls == 2*2 + 1 (databases/D) + 1 (data_sources/…/query)`.
  - `walk_follows_unknown_block_ids_and_types_404_as_inaccessible` (Review Focus 1): the root Markdown has `unknown_block_ids: [X]`, and X holds a child tag for Q, where Q returns 404. Assert that Q is in `skipped` with `SkipReason::Inaccessible` and that `complete == true`.
  - `walk_reaches_repeated_child_once_with_first_parent` (Review Focus 2): P is tagged under both root and A, and A tags root back. Assert that P is in `entries` once with `parent == Page(root)` and that the walk terminates.
  - `walk_prunes_excluded_subtree_without_reading_its_content`: an excluded page E has a child F. Assert that no request was made for `pages/E/markdown` or for F, and that `skipped` holds E with `ExcludedPage`.
  - `walk_never_follows_links_outside_root`: a Markdown link to an outside page O produces no request for O.
  - `walk_matches_old_lifecycle_membership_on_fixture_tree` (the spec's comparison, test-only): for one fixture tree, assert that the set of walk entries equals the set of pages whose `resolve_lifecycle` status is `Allowed`. Slice 4 deletes this test together with `resolve_lifecycle`.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-notion walk::`. Expected: FAIL, unresolved module `walk`.
- [ ] **Step 3: Implement** `walk_tree` and `child_references`. Send each `WalkedPage` to `sink` as soon as its content is read, so prefetch overlaps the walk the way it does with `crawl_with_pages` today.
- [ ] **Step 4: Run the tests again.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(notion): walk the root tree through markdown child references`.

### Task 1.3: Planner and initial sync consume the walk; delete the crawler

**Files:**
- Modify: `crates/retrieval/src/planner.rs:280-330`. Replace `plan()`, `plan_discovered(DiscoveryReport)` and `plan_report(Option<DiscoveryReport>)` with one `plan_walk`.
- Modify: `crates/retrieval/src/runner.rs:262-279`. Collapse `dry_run`/`dry_run_discovered` into `dry_run(walk: &TreeWalk)`.
- Modify: `crates/server/src/index_runtime.rs:409-646` (`stream_initial_pages`). Use `walk_tree` instead of `crawl_with_pages` and persist with `replace_inventory` when `walk.complete`.
- Delete: `crates/notion/src/crawl.rs`, `ScopedDiscovery` and its implementations.
- Create: `crates/notion-fake/{Cargo.toml,src/lib.rs}` (a `publish = false` workspace member and dev-dependency of retrieval and server), holding `FakeNotion` with a `Model` tree.
- Modify: `crates/retrieval/tests/authoritative_refresh.rs` and `crates/server/tests/reconciliation_command.rs` to use `notion_fake::FakeNotion`.

**Interfaces:**
- Consumes: `NotionClient::walk_tree`, `TreeWalk`, `SqliteSyncStateStore::replace_inventory`.
- Produces:
  ```rust
  impl AuthoritativeReconciliationPlanner { pub async fn plan_walk(&self, walk: &TreeWalk) -> Result<ReconciliationPlan, PlanError>; }
  impl AuthoritativeReconciliationRunner { pub async fn dry_run(&self, walk: &TreeWalk) -> Result<ReconciliationPlan, RunnerError>; }
  // notion-fake
  pub struct FakeNotion { pub base_url: String, /* … */ }
  impl FakeNotion { pub async fn start(model: Model) -> Self; pub fn set(&self, f: impl FnOnce(&mut Model)); pub fn requests(&self) -> Vec<String>; }
  pub struct Model { pub pages: BTreeMap<String, FakePage>, pub databases: BTreeMap<String, Vec<String>> /* rows */ }
  pub struct FakePage { pub parent: String, pub title: String, pub revision: String, pub markdown: String, pub in_trash: bool, pub status: u16 }
  ```
- Rule: `plan.traversal_complete = walk.complete`. Only complete walks may plan deletions. This keeps the guarantee in `planner_absence_access_failures_and_races_never_authorize_deletion`.

- [ ] **Step 1: Port the fakes, then write the failing tests**:
  - Keep and port: `planner_absence_access_failures_and_races_never_authorize_deletion`, `intentional_synced_references_preserve_physical_inventory_completeness`, and `reconciliation_stale_archive_move_restore_and_incomplete_traversal_apply_only_safe_differences`.
  - New `incomplete_walk_with_repeated_cursor_plans_no_deletes` (Review Focus 3) in `authoritative_refresh.rs`: the data-source query returns the same `next_cursor` twice. Assert `plan.planned_delete() == 0`, `!plan.traversal_complete`, and that inventory rows for unreached pages are still present.
  - New `initial_sync_records_inventory_totals_equal_outcomes` in `reconciliation_command.rs`: for a tree of 5 pages where 1 returns 404, assert after the resync that `inventory_counts()` gives `total == 5`, `indexed == 4` and `skipped == 1`.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-retrieval --test authoritative_refresh` and `cargo test -p notion-knowledge-server --test reconciliation_command`. Expected: FAIL to compile until `plan_walk` exists.
- [ ] **Step 3: Implement** `plan_walk`, wire `walk_tree` into `stream_initial_pages` and the reconciliation command, and delete `crawl.rs`. Delete these tests that only exercise the crawler: everything under `crawl.rs` `mod tests`, and the `blocks/{ROOT}/children` routes in both fakes.
- [ ] **Step 4: Run** `nix develop --command cargo test --workspace --locked` and `cargo clippy --workspace --all-targets --locked -- -D warnings`. Expected: PASS.
- [ ] **Step 5: Commit** with `refactor(sync)!: discover scope by walking the root tree` and a footer `BREAKING CHANGE: existing caches need one full resync`. Record the CI time in the PR.

---

## Slice 2: One read per page, no lifecycle walk and no commit-time recheck

**Depends on:** Slice 1. **Issue scope:** sync preparation uses the walked content and inventory parent. The before/after double read, `RootScopeGate` in sync, `revalidate_lifecycle` in sync, and the precommit source check for sync commits are all removed. The two parallel stream paths (initial and reconciliation) become one `sync_from_walk`. #651 is re-scoped to this slice.

**Deletes:**
- The `RootScopeGate::authorize`/`revalidate` calls in `prepare_scoped_page` (`prepare.rs:105-215`), the `after` re-read, `source_changed()`/`SOURCE_CHANGED`, and the `ScopedPageSnapshot.evidence` field.
- `prepare.rs` tests that only test the bracket: `metadata_or_markdown_race_is_conflict_not_snapshot`, `changed_physical_ancestry_during_final_check_is_conflict`, `relation_commit_rejects_source_physical_move_after_preparation`, `owned_relation_revalidation_rejects_source_and_target_changes_without_timestamp_help`, `relation_preparation_rejects_policy_replacement_during_authoritative_reads`, and the `chain()` fixtures with their 4-request lifecycle prefix.
- `relation_proposal.rs`: `RecordingLifecycle` and its `PageLifecycle` impl, plus the `revalidate_lifecycle` calls at lines 274, 328, 348 and 382.
- `refresh.rs`: `owned_revalidation` (line 1600), `source_revalidation_failure`, the `RootScopeGate::new` check in `new`, and the `source_changed` flag plumbing in `prepare_source`.
- `chunks/guarded.rs`: the `precommit_source_check` closure parameter of `commit_page_with_authority` for sync callers. If no non-sync caller remains, delete `commit_page_with_authority` itself and keep `commit_page`.
- `planner.rs:555` `revalidate_lifecycle` and `metadata_identity(page, evidence)`, which now uses `last_edited_time` + parent instead.
- `index_runtime.rs`: `prefetch_initial_page`'s `gate.authorize` step, the `Prefetched::Inline` re-preparation branch, and the separate reconciliation `apply_next` loop at around lines 843-900. Both collapse into `sync_from_walk`.
- These `authoritative_refresh.rs` tests: `changed_source_during_embedding_fails_commit_without_ack_or_index_mutation`, `lifecycle_refresh_rejects_same_root_ancestry_change_at_commit`, and `reconciliation_binding_change_during_owned_precommit_rejects_effect_and_preserves_resume`. Each one asserts the removed recheck. Keep the binding fence half as a lease or fence test if no other test covers it.

### Task 2.1: Single-read preparation

**Files:**
- Modify: `crates/notion/src/content.rs` (add `read_markdown`, and change `read_content` to call `fetch_page` only when the caller has no metadata)
- Modify: `crates/notion/src/prepare.rs:105`
- Test: `crates/notion/src/prepare.rs` `mod tests`

**Interfaces:**
- Consumes: `WalkedPage` (1.2) and `InventoryEntry`.
- Produces:
  ```rust
  impl NotionClient {
      pub async fn read_markdown(&self, page: &PageId) -> Result<String, BackendError>;
      pub fn prepare_walked_page(&self, walked: &WalkedPage, scope: &LifecycleScope,
          previous: &[IndexedChunk], config: ChunkConfig) -> Result<ScopedPageSnapshot, BackendError>; // no network
  }
  pub struct ScopedPageSnapshot { pub document: IndexedDocument, pub chunks: Vec<IndexedChunk>,
      pub relation_names: BTreeMap<String, String>, pub(crate) source: PageContent } // `evidence` removed
  ```
- `IndexedMetadata.source.database_id` and `data_source_id` come from `InventoryParent::DataSource`.
- Only properties marked truncated (relations or people over 25 items) trigger `fetch_page` property pagination.

- [ ] **Step 1: Write the failing tests**:
  - `prepare_walked_page_makes_no_requests`: the fixture has an empty route list, and the test asserts `requests == []` and that `snapshot.document.metadata.last_edited_time` equals the walked entry's value.
  - Port `selects_only_target_and_preserves_canonical_metadata_links_and_stable_ids` and `changed_section_keeps_unmodified_chunk_identity` to `prepare_walked_page`, keeping their assertions.
  - `row_page_metadata_carries_database_and_data_source_ids`: for `InventoryParent::DataSource{..}`, assert that both ids appear in `metadata.source`.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-notion prepare::`. Expected: FAIL.
- [ ] **Step 3: Implement** the change. Remove `prepare_scoped_page` once its last caller moves in 2.2.
- [ ] **Step 4: Run the tests again.** Expected: PASS.
- [ ] **Step 5: Commit** with `perf(notion): prepare pages from the walked read`.

### Task 2.2: One sync path from the walk to commit, without a recheck

**Files:**
- Modify: `crates/retrieval/src/refresh.rs` (`prefetch_page` becomes `prepare_walked`, `refresh_prefetched` is kept, and `apply_lifecycle` loses negative-evidence revalidation)
- Modify: `crates/retrieval/src/chunks/guarded.rs:784-905`
- Modify: `crates/notion/src/relation_proposal.rs`
- Modify: `crates/server/src/index_runtime.rs:409-900`
- Test: `crates/retrieval/tests/authoritative_refresh.rs`, `crates/server/tests/reconciliation_command.rs`

**Interfaces:**
- Consumes: `prepare_walked_page`, `walk_tree`, and `record_inventory_outcome`.
- Produces:
  ```rust
  impl AuthoritativePageRefresh {
      pub fn prepare_walked(&self, walked: &WalkedPage, operation_id: &str) -> Result<PrefetchedPage, RefreshError>;
  }
  impl IndexRuntime {
      async fn sync_from_walk(&self, settings: &WorkerSettings, lease: &mut Lease, mode: SyncMode) -> Result<SyncTotals, &'static str>;
  }
  pub enum SyncMode { Full, Incremental { since: String } } // Incremental is wired in Slice 3
  pub struct SyncTotals { pub inventory: u64, pub indexed: u64, pub unchanged: u64, pub skipped: u64, pub failed: u64, pub removed: u64 }
  ```
- Pages whose `last_edited_time` and content hash match the receipt are counted `unchanged` without embedding.
- After a complete walk, removals are pages in `InventoryDiff.removed ∪ trashed` that are present in the index.

- [ ] **Step 1: Write the failing tests**:
  - `full_sync_makes_two_calls_per_plain_page_and_one_per_row` (`reconciliation_command.rs`): for 10 plain pages and a database with 20 rows, assert `fake.requests().len() <= 2*11 + 20 + 2` and that the totals add up to `SyncTotals.inventory == 31`.
  - `page_moved_out_between_read_and_commit_is_removed_on_next_walk` (records the owner's decision §3): the fake moves P outside the root after its Markdown is served. Assert that P is indexed after run 1 and absent from the index and inventory after run 2.
  - `trashed_page_is_removed_after_one_walk` and `revoked_subtree_is_removed_after_one_walk` (403 on a subtree root): assert that the subtree's index rows are gone and that `skipped` has `Inaccessible`.
  - `unchanged_rerun_embeds_nothing`: run twice. On the second run, assert that the `CountingProvider` batch count did not grow and `SyncTotals.unchanged == inventory`.
  - Keep `lifecycle_move_between_allowed_roots_preserves_ids_and_vectors`, `concurrent_canonical_refresh_cannot_resurrect_authoritatively_removed_page` and all the lease, fence, receipt and #649 deadline tests unchanged.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-retrieval --test authoritative_refresh` and `cargo test -p notion-knowledge-server --test reconciliation_command`. Expected: the new tests FAIL.
- [ ] **Step 3: Implement** the change and delete everything in this slice's Deletes list.
- [ ] **Step 4: Run** `nix develop --command cargo test --workspace --locked` and clippy. Expected: PASS. Also run `grep -rn "revalidate_lifecycle\|prepare_scoped_page" crates/retrieval crates/server crates/notion/src/prepare.rs crates/notion/src/relation_proposal.rs`, which should print nothing.
- [ ] **Step 5: Commit** with `perf(sync)!: one read per page and no commit-time lifecycle recheck`. Record the CI time.

---

## Slice 3: Incremental sync, drift reconciliation, diagnostics and the callback path

**Depends on:** Slice 2. **Issue scope:** `SyncMode::Incremental` uses search to find candidates, places them through the inventory parent map, and refreshes them through the same consumer. The scheduled and startup reconciliation runs a full walk and reports drift counts. A diagnostics endpoint exposes the counts and the out-of-sync list. The webhook worker handles one page through the same path. The webhook receiver itself is out of scope.

**Deletes:**
- The scheduler's own plan/apply loop in `crates/server/src/scheduled_reconciliation.rs` that duplicates the runtime path. It now calls `sync_from_walk(SyncMode::Full)`.
- The webhook-worker path that calls `apply_lifecycle` with a live lifecycle resolve (`webhook_worker.rs`, `grant_refresh.rs`). It is replaced by `refresh_one(page)` using the parent map.
- Scheduled tests that only cover the removed loop. Keep the deadline, restart, cancellation and backoff tests from `actual_scheduled_http_*` and port them to the new entrypoint.

### Task 3.1: Search accelerator and single-page refresh

**Files:**
- Create: `crates/notion/src/search.rs`
- Create: `crates/retrieval/src/drift.rs`
- Modify: `crates/server/src/index_runtime.rs`, `crates/server/src/webhook_worker.rs`
- Test: `crates/server/tests/reconciliation_command.rs`, `crates/server/tests/webhook_worker.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct SearchHit { pub page_id: String, pub parent: InventoryParent, pub last_edited_time: String, pub in_trash: bool }
  impl NotionClient { pub async fn search_changed_since(&self, since: &str) -> Result<Vec<SearchHit>, BackendError>; }
  // POST /v1/search, filter object=page, sort last_edited_time ascending, page_size 100; re-read the last page's tail (minute-granular timestamps)
  pub enum Placement { Placed(Vec<String>), Unplaced }
  pub fn place(store: &SqliteSyncStateStore, hit: &SearchHit) -> Result<Placement, SyncStateError>;
  impl IndexRuntime { pub async fn refresh_one(&self, page: &PageId, lease: &mut Lease) -> Result<SyncTotals, &'static str>; }
  pub struct DriftCounts { pub added: u64, pub changed: u64, pub moved: u64, pub removed: u64, pub trashed: u64, pub unplaced: u64 }
  ```
- Incremental: a hit whose parent chain ends at the root is fetched (one retrieve and one Markdown read), committed, and upserted. An `Unplaced` hit is counted and never indexed (Review Focus 5). A page that is in the inventory but trashed in search is confirmed by one `GET pages/{id}` before removal (search lag guard).
- `refresh_one` does one `GET pages/{id}` and checks `place()`. If placed, it refreshes and upserts. If unplaced, it records `drift.unplaced` and does nothing else.

- [ ] **Step 1: Write the failing tests**:
  - `incremental_with_no_changes_only_calls_search`: assert that all of `fake.requests()` start with `POST /v1/search` and that no index write happened.
  - `incremental_refreshes_only_changed_page`: after editing page B, assert that the only page reads are `pages/B` and `pages/B/markdown`.
  - `search_hit_with_unknown_parent_is_counted_not_indexed` (Review Focus 5): assert `drift.unplaced == 1` and that the page is not in the index.
  - `search_tail_is_reread_for_same_minute_edits`: two pages share a `last_edited_time` across the page boundary, and both are refreshed.
  - `trashed_in_search_is_confirmed_before_removal`: search says trashed, but `GET pages/X` returns not trashed, so X stays indexed.
  - `webhook_single_page_event_refreshes_through_parent_map` in `webhook_worker.rs`: assert exactly 2 Notion requests for the event.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-server --test reconciliation_command incremental_` and `--test webhook_worker`. Expected: FAIL.
- [ ] **Step 3: Implement** the change.
- [ ] **Step 4: Run the tests again.** Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sync): incremental sync via search and single-page refresh`.

### Task 3.2: Scheduled drift walk and diagnostics

**Files:**
- Modify: `crates/server/src/scheduled_reconciliation.rs`, `crates/server/src/diagnostics.rs`, `crates/core/src/logging.rs` (`SyncCount` gains drift kinds), and `crates/mcp/src/sync_status.rs` (counts only)
- Test: `crates/server/tests/sync_status.rs`, `crates/server/tests/reconciliation_command.rs`

**Interfaces:**
- Consumes: `sync_from_walk(SyncMode::Full)`, `InventoryDiff`, `inventory_counts`, `out_of_sync_pages`.
- Produces: the diagnostics JSON `{"inventory":{"total","indexed","skipped","failed","pending"},"drift":{"added","changed","moved","removed","trashed","unplaced"},"out_of_sync":[{"page_id","outcome"}]}`, with no titles. Startup runs `Incremental` when an inventory exists and `Full` otherwise. The scheduled cycle runs `Full`.

- [ ] **Step 1: Write the failing tests**:
  - `drift_walk_detects_move_trash_revoke_and_add_in_one_pass`: apply all four fake mutations, run one scheduled cycle, and assert `drift == {added:1, moved:1, trashed:1, removed:≥1}` and that the index matches the inventory afterwards.
  - `diagnostics_reports_counts_without_titles`: assert that the response body does not contain any fixture title string and that `inventory.total` equals the sum of the outcome counts.
  - Port `actual_scheduled_http_preserves_deadline_restart_and_unchanged_next_cycle` and `actual_scheduled_http_shutdown_drains_live_owned_effect_before_release`.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-server --test sync_status --test reconciliation_command`. Expected: FAIL.
- [ ] **Step 3: Implement** the change and delete the old scheduler loop.
- [ ] **Step 4: Run** the workspace tests and clippy. Expected: PASS.
- [ ] **Step 5: Commit** with `feat(sync): periodic drift walk and inventory diagnostics`. Record the CI time.

---

## Slice 4: Retire the live ancestor walk (MCP root gate uses the inventory)

**Depends on:** Slice 3. **Issue scope:** `RootScopeGate` (`crates/core/src/root_scope.rs`), which `crates/mcp/src/{lib.rs,mutations.rs,related.rs}` use for reads, related lookups and writes, resolves membership through `inventory_parent_chain` instead of `PageLifecycle`. With no callers left, `resolve_lifecycle` and its reverse bracket are deleted. **Owner review point:** this applies decision (b), staleness bounded by reconciliation, to the MCP write gate too. If the owner wants live checks before writes, keep a slimmed forward-only `resolve_lifecycle` for `mutations.rs` and drop this slice's write half.

**Deletes:**
- `crates/notion/src/lifecycle.rs`: `resolve_lifecycle`, the reverse bracket, `revalidate_lifecycle`, `impl PageLifecycle for NotionClient`, and all 9 tests (about 990 lines). Keep only `parent()`/`parse()` if `search.rs` and `walk.rs` reuse them, moved into `walk.rs`.
- `crates/core/src/lifecycle.rs`: the `PageLifecycle` trait, `LifecycleEvidence.ancestry` and `LifecycleNode` (`LifecycleScope` stays).
- The `PageLifecycle` fakes in `crates/mcp/tests/mutations.rs:252-310` and `crates/retrieval/examples/production_support/mod.rs:255-300`.
- `walk_matches_old_lifecycle_membership_on_fixture_tree` (1.2).

### Task 4.1: Gate from the parent map

**Files:**
- Modify: `crates/core/src/root_scope.rs`, `crates/mcp/src/lib.rs:353-360`, `crates/mcp/src/mutations.rs:24-47,140`, `crates/mcp/src/related.rs:93-117`, `crates/server/src/retrieval_runtime.rs` (wiring)
- Test: `crates/mcp/tests/mutations.rs`

**Interfaces:**
- Produces:
  ```rust
  pub trait Membership: Send + Sync { fn parent_chain(&self, page: &str) -> Result<Option<Vec<String>>, ScopeError>; }
  impl Membership for SqliteSyncStateStore { /* inventory_parent_chain */ }
  impl RootScopeGate { pub fn new(membership: Arc<dyn Membership>, scope: LifecycleScope) -> Result<Self, ScopeError>;
                       pub fn authorize(&self, page: &PageId) -> Result<ScopePermit, ScopeError>; } // sync, no network
  ```
  `revalidate` is removed. Callers at `mcp/src/lib.rs:656,753,886,923,974` and `related.rs:105` drop the call.

- [ ] **Step 1: Write the failing tests**: `gate_denies_page_absent_from_inventory`, `gate_denies_page_under_excluded_descendant`, and `gate_allows_page_with_chain_to_root_without_network`. The last one asserts that the fake Notion source receives 0 requests.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-mcp --test mutations gate_`. Expected: FAIL.
- [ ] **Step 3: Implement** the change and delete everything in this slice's Deletes list.
- [ ] **Step 4: Run** `nix develop --command cargo test --workspace --locked`, clippy, and `grep -rn "PageLifecycle\|resolve_lifecycle" crates`, which should print nothing. Expected: PASS.
- [ ] **Step 5: Commit** with `refactor(scope)!: authorize from the page inventory and delete ancestor walks`. Record the CI time.

---

## Slice 5: Batched index maintenance (#663) and compaction (#664)

**Depends on:** none. It touches only `chunks/guarded.rs` commit internals, so it can run in parallel with Slices 1–3. Rebase it onto Slice 2 if it lands second. **Issue scope:** per-page merges stay atomic. FTS and vector optimize run every N pages or T seconds, at the end of a sync, and on startup if they are pending. Compaction and pruning run at the end of a sync.

**Deletes:** the inline `ensure_fts_index` + `optimize_fts_index` + vector `optimize` block in `commit_page_with_authority` (`guarded.rs:871-903`), and any per-commit index-maintenance assertions in `crates/retrieval/tests/index_commits.rs` that require optimize inside the commit.

### Task 5.1: `IndexMaintenance`

**Files:**
- Create: `crates/retrieval/src/maintenance.rs`
- Modify: `crates/retrieval/src/chunks/guarded.rs`, `crates/server/src/index_runtime.rs` (call it from `sync_from_walk`, or from the current stream loop if Slice 2 has not landed)
- Test: `crates/retrieval/tests/index_commits.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct MaintenancePolicy { pub every_pages: usize, pub every: Duration } // defaults: 50 pages, 60 s
  pub struct IndexMaintenance { /* guard, policy, counters */ }
  impl IndexMaintenance {
      pub fn new(guard: GuardedChunkTable, policy: MaintenancePolicy) -> Self;
      pub async fn after_commit(&mut self) -> Result<(), ChunkTableError>;   // optimizes when a threshold is hit
      pub async fn finish(&mut self) -> Result<(), ChunkTableError>;         // optimize + compact + prune old versions
  }
  impl GuardedChunkTable { pub async fn optimize_indexes(&self) -> Result<(), ChunkTableError>;
                           pub async fn compact_and_prune(&self) -> Result<(), ChunkTableError>; }
  ```
  Both run under the same writer lease as commits.

- [ ] **Step 1: Write the failing tests**:
  - `unoptimized_committed_rows_are_lexically_and_vector_searchable` (Review Focus 4): commit 3 pages with `every_pages = 100`, drop `IndexMaintenance` without calling `finish` to simulate a crash, reopen, and assert that a lexical query and a vector query each return all 3 pages.
  - `maintenance_optimizes_after_threshold_and_at_finish`: with `every_pages = 2`, after 5 commits and `finish`, assert that the optimize count is 3, using a counter exposed under `#[cfg(test)]`.
  - `compaction_reduces_table_versions_without_losing_rows`: assert that `table_version` metadata shrinks and the row count is unchanged.
- [ ] **Step 2: Run** `nix develop --command cargo test -p notion-knowledge-retrieval --test index_commits maintenance_ unoptimized_ compaction_`. Expected: FAIL.
- [ ] **Step 3: Implement** the change.
- [ ] **Step 4: Run** the workspace tests and clippy. Expected: PASS.
- [ ] **Step 5: Commit** with `perf(index): batch FTS/vector optimize and compact at sync end`. Record the CI time.

---

## Slice 6: One embedder factory

**Depends on:** none. **Issue scope:** `EmbeddingProvider` (`crates/core/src/embedding.rs:121`) is already the spec's interface, so no new trait is needed. This slice routes every provider construction through one factory, keeping the vector identity checks (`EmbeddingMetadata::ensure_compatible`), so a later external backend changes one function. It adds no new backend.

**Deletes:** the five duplicated `QwenProvider::load(QwenConfig::cpu(...))` blocks, with their error mapping, in `crates/server/src/{webhook_worker.rs:261,retrieval_runtime.rs:144,retrieval_runtime.rs:298,oauth_writes.rs:191,index_command.rs:86}`.

### Task 6.1: `load_configured_embedder`

**Files:**
- Create: `crates/retrieval/src/embedder.rs`
- Modify: the five call sites above
- Test: unit tests in `crates/retrieval/src/embedder.rs`, using a fake provider and no real Qwen

**Interfaces:**
- Produces:
  ```rust
  pub enum EmbedderConfig { LocalQwen { assets: PathBuf } }
  pub async fn load_configured_embedder(config: &EmbedderConfig, expected: Option<&EmbeddingMetadata>)
      -> Result<Arc<dyn EmbeddingProvider>, EmbeddingError>; // rejects identity mismatch before returning
  ```

- [ ] **Step 1: Write the failing test** `factory_rejects_identity_mismatch_before_use`. It uses a test-only constructor that injects a stub, and asserts `Err(EmbeddingError::…Incompatible)` and that the stub's `embed_batch` was never called.
- [ ] **Step 2: Run** `nix develop .#spike --command cargo test -p notion-knowledge-retrieval --features local-qwen embedder::`. Expected: FAIL.
- [ ] **Step 3: Implement** the factory and replace the five call sites.
- [ ] **Step 4: Run** the workspace tests and clippy with `--features local-qwen`. Expected: PASS.
- [ ] **Step 5: Commit** with `refactor(embedding): route all providers through one factory`.

---

## Slice 7: Required CI gate under 10 minutes

**Depends on:** none. It can start now, and its test-binary consolidation is easier after Slice 1 deletes the crawler tests. **Baseline:** 35 min, with container smoke on main at 34 min, the native test compile at 16 min, Clippy at 10 min, and unit tests at 2–4 min. **Target:** the required `CI gate` finishes in under 10 min on a warm cache. Full coverage still runs, but outside the required path.

**Deletes:**
- The `check` job (`ci.yml:183-223`). `cargo clippy --all-targets` already type-checks every target, so remove it from `JOBS` in `scripts/ci-jobs.py:10` and from the `gate.needs` list.
- `--example production-eval` from the PR native compile (`ci.yml:325`). It builds on `main` and on schedule only.
- The `container` and `container-main` jobs in `ci.yml`. They move to `container.yml`.
- The separate test binaries: 22 files under `crates/retrieval/tests/*.rs` and 17 under `crates/server/tests/*.rs` become one `tests/it/main.rs` per crate, with `mod` files. Each binary links LanceDB, so linking 2 binaries instead of 39 removes most of the 16-minute native link time.
- Test cases deleted by Slices 1–4. Record the test count before and after in each PR.

### Task 7.1: Split required and optional workflows

**Files:**
- Modify: `.github/workflows/ci.yml`, `scripts/ci-jobs.py`, `scripts/test-ci-jobs.py`
- Create: `.github/workflows/container.yml`

**Decisions:**
- **Required (`CI gate` needs):** `changes`, `agent-layout`, `fmt`, `clippy` (default features only: `cargo clippy --workspace --all-targets --locked -- -D warnings`), `test (workspace)`, `dependencies`, and `secrets`. These jobs run in parallel, and each is expected to take under 8 min with a warm cache.
- **Optional on PRs** (they still run and report, but they are not in `gate.needs`): `clippy-features`, which takes the four `nix develop .#spike` clippy steps at `ci.yml:159-166` out of the required Clippy job; `test (local-qwen)`; `native-build` and `native-test`; and `build` (release).
- **`container.yml`:** triggers are `push` to `main`, `schedule: cron "0 3 * * *"`, `workflow_dispatch`, and `pull_request` with `paths: [Dockerfile, docker-compose*.yml, scripts/*container*, scripts/smoke-*.py, nix/**, flake.lock]`. Move `*container_smoke_steps` there unchanged, and keep `container-main` as the GHCR cache publisher on `main`.
- **Merge rule** (document it in `CONTRIBUTING.md` or the `issue-to-merge` skill): before merging a PR whose `changes` selection includes native or container inputs, the optional jobs must be green. The required gate stays under 10 min. `scripts/ci-jobs.py select` still decides which optional jobs a PR needs.
- **Caching:** keep `cargo-cache-restore` and `cargo-cache-save` as they are. Save caches only from `main` and keep keys per job. Give `clippy-features` its own cache key, `job: clippy-features`, so the default Clippy cache stays small. Set `CARGO_INCREMENTAL=0` at workflow `env`. Set `CARGO_PROFILE_TEST_DEBUG: line-tables-only` and `CARGO_PROFILE_DEV_DEBUG: line-tables-only` at workflow level, not only for `test`, and include both in the cache key. This is the existing snapshot rule at `ci.yml:244-246`.

- [ ] **Step 1: Write the failing tests** in `scripts/test-ci-jobs.py`:
  - `test_required_gate_excludes_optional_feature_and_container_jobs`: parse `ci.yml` and assert that `set(gate.needs) == {changes, agent-layout, fmt, clippy, test, dependencies, secrets}`.
  - `test_check_job_removed_and_clippy_covers_all_targets`: assert that `check` is not in `jobs` and that the Clippy step contains `--all-targets`.
  - `test_container_workflow_runs_on_main_schedule_and_container_paths`.
  - Update `test_alternate_main_registry_job_satisfies_gate_and_both_are_required` and `test_registry_export_is_main_only_and_required_smoke_is_not_optional` to the new `container.yml` home. Delete these assertions where they claim container smoke is required.
- [ ] **Step 2: Run** `python3 scripts/test-ci-jobs.py`. Expected: FAIL.
- [ ] **Step 3: Edit** `ci.yml`, `ci-jobs.py` and `container.yml` as decided above.
- [ ] **Step 4: Run** `python3 scripts/test-ci-jobs.py` and `python3 scripts/verify-ci-final-audit.py`. Expected: PASS. Push, and read the timings with `gh run view --json jobs`. Expected: the gate job completes in under 10 min after a main cache exists.
- [ ] **Step 5: Commit** with `ci: keep required gate to default-feature checks under 10 minutes`.

### Task 7.2: One integration test binary per crate

**Files:**
- Create: `crates/retrieval/tests/it/main.rs`, `crates/server/tests/it/main.rs`
- Move: `crates/{retrieval,server}/tests/*.rs` to `tests/it/<name>.rs`. Shared helpers (`common`, `support`, `fixtures`) become modules.
- Modify: `crates/{retrieval,server}/Cargo.toml` (`autotests = false`, `[[test]] name = "it" path = "tests/it/main.rs"`), and `scripts/native-test-artifacts.py` (suite manifest expects 1 binary per crate)

- [ ] **Step 1: Record** the test count with `cargo test --workspace -- --list 2>/dev/null | grep -c ': test$'`, saved as N.
- [ ] **Step 2: Move the files.** Fix any duplicate top-level helper names by giving each file its own module, since none conflict once they are modules.
- [ ] **Step 3: Run** `nix develop --command cargo test --workspace --locked` and `python3 scripts/test-native-test-artifacts.py`. Expected: PASS, and the count from Step 1 still equals N.
- [ ] **Step 4: Commit** with `test: build one integration binary per crate`. Record the native-build job time before and after in the PR.

---

## Slice 8: Release measurement and full-root acceptance (#653, #662, #455)

**Depends on:** Slices 2, 3 and 5. **Issue scope:** measurement only. Make no code changes unless the measurement fails.

**Deletes:** any stale measurement notes or docs that describe the ancestor walk or the double read. Search `docs/` for `resolve_lifecycle`, `reverse`, `bracket` and `crawl`.

### Task 8.1: Measure and accept

- [ ] **Step 1:** Run a release build (`nix develop --command cargo build --workspace --release --locked`) on a staging data directory, not on `notion-knowledge.service` or `/var/lib/notion-knowledge-*`. Run a full sync of the production root with the #662 per-phase logging.
- [ ] **Step 2: Record** calls per page, pages per hour by phase, total wall time and peak RSS. Expected: totals equal the inventory size, peak RSS is under 4 GiB, and calls per page are at most 2.1. See the self-review note on the 1.1–1.3 target.
- [ ] **Step 3:** Run an incremental sync with no changes. Expected: only `POST /v1/search` calls, finishing in under 1 min.
- [ ] **Step 4:** Run the #455 retrieval acceptance probe. Expected: PASS.
- [ ] **Step 5:** Post the results on #455 and close the slice.

---

## Self-Review

1. **Spec coverage:**

   | Spec section | Covered by |
   |---|---|
   | §1 walk | 1.2, 1.3 |
   | §1b search | 3.1 |
   | §2 one read | 2.1 |
   | §3 no recheck | 2.2 |
   | §4 inventory and drift | 1.1, 3.2 |
   | §5 callback | 3.1 `refresh_one` |
   | §6 batching | 5.1 |
   | §7 embedder | 6.1 |
   | Simpler | the Deletes block in every slice |
   | CI | 7.1, 7.2 |
   | Measurement | 8.1 |
   | Drift tests | 2.2, 3.2 |
   | Crash-before-optimize | 5.1 |

   - **Gap stated honestly (spec §2 budget):** for pages that are not database rows, Notion's Markdown endpoint does not return `last_edited_time` or title-property metadata. A full walk therefore costs about 2 calls per plain page, while rows cost about 1. That puts 3,000 pages at roughly 20–35 min depending on the row share, not the spec's 1.1–1.3 calls. The only way to reach 1.1 calls is to take metadata from search in full runs, which the owner ruled out. Slice 8 reports the measured mix. The coordinator should raise this with the owner rather than change it silently.
2. **Step scan:** each code step names a signature and file, and each test step names a test with assertions. The fake's internals and the SQL bodies are left to the builder, as the spec allows.
3. **Type consistency:**
   - `InventoryEntry`, `InventoryParent`, `TreeWalk` and `WalkedPage` are used identically in 1.1, 1.2, 1.3, 2.x, 3.x and 4.1.
   - `SyncMode`, `SyncTotals` and `sync_from_walk` are defined in 2.2 and consumed in 3.x.
   - `RootScopeGate::new` changes signature only in Slice 4.
4. **Review Focus:** all five items have owning tests (1.2 ×2, 1.3, 5.1, 3.1).
5. **Proportion:** the plan holds signatures and test assertions only. Its code blocks are interface declarations, not function bodies.
