# Releasing notion-knowledge

This is the **manual, reviewed release procedure** for issue #113. No GitHub
Release is created merely by merging to `main`, and tagging does not currently
trigger an artifact-publishing workflow. Release operators must verify and
publish the actual tag build.

## Version and compatibility policy

- The root `[workspace.package].version` in `Cargo.toml` is the one source of
  release identity. All Rust workspace crates inherit it. MCP `serverInfo`,
  `notion-knowledge-server --version`, startup diagnostics, HTTP health JSON,
  and Docker image identity already derive from that same Cargo version.
- Use SemVer `MAJOR.MINOR.PATCH`, with Git tags `vMAJOR.MINOR.PATCH`. While
  pre-1.0, incompatible changes increase **MINOR**, new compatible behavior
  increases MINOR, and fixes increase PATCH. After 1.0, incompatible changes
  increase MAJOR. Do not reuse or move a published version/tag.
- **MCP tool compatibility:** changes to names, required inputs, output
  structure/types, meaning, authorization, or error semantics may break agent
  clients and must be explicitly described. Additive optional changes are
  normally compatible but must still be noted. Review both `knowledge_search`
  and `knowledge_get` schemas (and any new tools) against the previous tag.
- **Index and operational state:** application SemVer does **not** itself migrate
  LanceDB, embeddings, fingerprints, or SQLite state. Release notes must say
  whether an index rebuild, embedding-model/vector-space rotation, SQLite
  migration/backup, or no migration is required. The *Notion source* is
  authoritative for content, but the operational SQLite journal/queue cannot
  be treated as an expendable retrieval cache. Use a verified backup and
  reversible migration plan before changing persistent state.

## Prepare the release on a reviewed PR

1. Start from an up-to-date `main`; check existing Git tags and releases.
   Resolve in-flight PRs and any migration prerequisites before choosing a version.
2. Bump `Cargo.toml` workspace version for a **new** release and regenerate
   `Cargo.lock` through Cargo. Confirm all workspace packages still inherit it.
3. Transfer the relevant `[Unreleased]` notes to an exact
   `## [X.Y.Z] - YYYY-MM-DD` section in `CHANGELOG.md`. Always fill
   **MCP tool-schema changes** and **Index/state migrations**, explicitly
   saying `None` where appropriate. Do not silently omit either. Retain
   `## [Unreleased]` for subsequent changes. A `TBD` section is allowed
   for planning, but must be dated before publication.
4. Review source/schema diffs and storage compatibility against the **previous
   released tag**. Record operator actions, backup/rebuild needs, changes
   to file/tool schema, and any intentional breaking behavior. Do not claim
   a workflow is available just because its API or CLI is staged.
5. Validate the version and notes:
   ```sh
   python3 scripts/check-release.py --tag "vX.Y.Z"
   python3 scripts/test-release.py
   cargo fmt --all -- --check
   cargo clippy --workspace --all-targets --locked -- -D warnings
   cargo test --workspace --locked
   cargo build --workspace --release --locked
   ./target/release/notion-knowledge-server --version
   ```
   The printed version must be the version being released. Run optional
   feature tests and container smoke tests as defined in `docs/ci.md`
   whenever those adapters are part of the deployment.
6. Merge only after the PR's CI gate succeeds; build/test from the **exact
   merged commit**. Validate the final release tag check again. Create an
   annotated `vX.Y.Z` tag on that commit and push it:
   ```sh
   git tag -a vX.Y.Z -m "notion-knowledge vX.Y.Z"
   git push origin vX.Y.Z
   ```
   Make a GitHub Release (draft first) targeting **that exact tag**, using
   its matching changelog section as the release notes. Explicitly include
   tool-schema changes and state migration/rebuild steps. Attach artifacts
   built from the tag only after verifying `--version` and checksums.
   Do not publish an unrelated/expired CI artifact as a release binary.

## Rollback

Stop affected writers before changing a deployment or restoring state.
Rolling back the **binary** is not necessarily safe after a persistent-state
migration. Follow the release-specific backward-compatibility statement:
either restore a consistent pre-upgrade SQLite backup and rebuild the derived
index, or apply an explicitly tested forward-compatible rollback. Never
silently delete the SQLite journal/queue to make an old binary start.
Keep Notion content authoritative and verify freshness, search and retry
processing after recovery. If any required migration is not reversible,
mark the release **no automatic rollback** and rehearse recovery before cutover.

## Automated guard

`scripts/check-release.py` validates the root SemVer identity, draft/release
entry and the two mandatory disclosure sections. In tag mode it also rejects
wrong tags and `TBD` releases. `scripts/test-release.py` exercises those
guards without credentials; CI runs both on every PR. This guard does not
infer or approve schema safety; review and smoke testing remain mandatory.
