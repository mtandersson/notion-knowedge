//! Metadata-only physical ancestry resolution through the shared Notion transport.
use crate::{
    NotionClient,
    pages::{PAGE_VERSION, inactive, page_id, uuid},
};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, BackendFuture, PageId},
    discovery::{ExclusionRules, SkipReason, SourceType},
    lifecycle::{
        LifecycleEvidence, LifecycleKind, LifecycleNode, LifecycleScope, LifecycleStatus,
        PageLifecycle, PhysicalParent,
    },
};
use reqwest::{Method, header::AUTHORIZATION};
use serde_json::Value;
use std::collections::BTreeSet;

/// Total physical nodes including the selected page; no silent truncation.
pub const MAX_LIFECYCLE_NODES: usize = 256;
fn error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.lifecycle",
        retry_after: None,
        committed_page_id: None,
    }
}
fn identifier(value: &Value) -> Result<String, BackendError> {
    value
        .as_str()
        .and_then(uuid)
        .ok_or_else(|| error(BackendErrorKind::Internal))
}
fn normalize(scope: &LifecycleScope) -> Result<LifecycleScope, BackendError> {
    if scope.roots.is_empty()
        || scope.roots.len() > MAX_LIFECYCLE_NODES
        || scope.exclusions.page_ids.len() + scope.exclusions.descendants_of.len() > 100_000
    {
        return Err(error(BackendErrorKind::InvalidInput));
    }
    Ok(LifecycleScope {
        workspace_id: uuid(&scope.workspace_id)
            .ok_or_else(|| error(BackendErrorKind::InvalidInput))?,
        generation: scope.generation,
        roots: scope
            .roots
            .iter()
            .map(|p| page_id(&p.0).map(|p| p.0))
            .collect::<Result<BTreeSet<_>, _>>()?
            .into_iter()
            .map(PageId)
            .collect(),
        exclusions: ExclusionRules {
            page_ids: scope
                .exclusions
                .page_ids
                .iter()
                .map(|v| page_id(v).map(|p| p.0))
                .collect::<Result<_, _>>()?,
            descendants_of: scope
                .exclusions
                .descendants_of
                .iter()
                .map(|v| page_id(v).map(|p| p.0))
                .collect::<Result<_, _>>()?,
            source_types: scope.exclusions.source_types.clone(),
        },
    })
}
fn parent(value: &Value) -> Result<PhysicalParent, BackendError> {
    let p = value
        .get("parent")
        .and_then(Value::as_object)
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    let t = p
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    // Known alternative parent fields must not contradict the discriminator.
    // A data-source parent may additionally carry its containing database UUID.
    for field in [
        "workspace",
        "page_id",
        "block_id",
        "database_id",
        "data_source_id",
        "agent_id",
    ] {
        if field != t && p.contains_key(field) {
            if t == "data_source_id" && field == "database_id" {
                identifier(&p[field])?;
            } else {
                return Err(error(BackendErrorKind::Internal));
            }
        }
    }
    let kind = match t {
        "workspace" => {
            if p.get("workspace").and_then(Value::as_bool) != Some(true) {
                return Err(error(BackendErrorKind::Internal));
            }
            return Ok(PhysicalParent::Workspace);
        }
        "page_id" => LifecycleKind::Page,
        "block_id" => LifecycleKind::Block,
        "database_id" => LifecycleKind::Database,
        "data_source_id" => LifecycleKind::DataSource,
        _ => return Err(error(BackendErrorKind::UnsupportedContent)),
    };
    Ok(PhysicalParent::Object {
        kind,
        id: identifier(&p[t])?,
    })
}
fn parse(value: &Value, kind: LifecycleKind, id: &str) -> Result<LifecycleNode, BackendError> {
    let object = match kind {
        LifecycleKind::Page => "page",
        LifecycleKind::Block => "block",
        LifecycleKind::Database => "database",
        LifecycleKind::DataSource => "data_source",
    };
    if value["object"] != object || identifier(&value["id"])? != id {
        return Err(error(BackendErrorKind::Internal));
    }
    if kind == LifecycleKind::Block && value["type"].as_str().is_none_or(str::is_empty) {
        return Err(error(BackendErrorKind::Internal));
    }
    if kind == LifecycleKind::Block
        && value["type"] == "synced_block"
        && value["synced_block"].get("synced_from").is_none()
    {
        return Err(error(BackendErrorKind::Internal));
    }
    // A partial block/page must not be mistaken for complete ancestry metadata.
    let revision = value["last_edited_time"]
        .as_str()
        .ok_or_else(|| error(BackendErrorKind::Internal))?;
    let revision = chrono::DateTime::parse_from_rfc3339(revision)
        .map_err(|_| error(BackendErrorKind::Internal))?
        .to_rfc3339();
    if kind == LifecycleKind::Block
        && value["type"] == "synced_block"
        && !value["synced_block"]["synced_from"].is_null()
    {
        return Err(error(BackendErrorKind::UnsupportedContent));
    }
    let physical_parent = parent(value)?;
    let valid_relationship = match (&kind, &physical_parent) {
        (LifecycleKind::Page, _) => true,
        (LifecycleKind::Database, PhysicalParent::Workspace) => true,
        (LifecycleKind::Database, PhysicalParent::Object { kind, .. }) => matches!(
            kind,
            LifecycleKind::Page | LifecycleKind::Block | LifecycleKind::DataSource
        ),
        (LifecycleKind::DataSource, PhysicalParent::Object { kind, .. }) => {
            matches!(kind, LifecycleKind::Database | LifecycleKind::DataSource)
        }
        (LifecycleKind::Block, PhysicalParent::Object { kind, .. }) => matches!(
            kind,
            LifecycleKind::Page | LifecycleKind::Block | LifecycleKind::DataSource
        ),
        _ => false,
    };
    if !valid_relationship {
        return Err(error(BackendErrorKind::UnsupportedContent));
    }
    Ok(LifecycleNode {
        kind,
        id: id.to_owned(),
        revision,
        inactive: inactive(value).ok_or_else(|| error(BackendErrorKind::Internal))?,
        parent: physical_parent,
    })
}
impl NotionClient {
    async fn lifecycle_node(
        &self,
        kind: LifecycleKind,
        id: &str,
    ) -> Result<LifecycleNode, BackendError> {
        let endpoint = match kind {
            LifecycleKind::Page => "pages",
            LifecycleKind::Block => "blocks",
            LifecycleKind::Database => "databases",
            LifecycleKind::DataSource => "data_sources",
        };
        let request = self
            .http
            .request(Method::GET, format!("{}/{endpoint}/{id}", self.api_root))
            .header(AUTHORIZATION, self.authorization.clone())
            .header("Notion-Version", PAGE_VERSION);
        let mut response = self.send(request, true, "notion.lifecycle").await?;
        if response.status().as_u16() != 200 {
            return Err(error(match response.status().as_u16() {
                400 => BackendErrorKind::InvalidInput,
                401 => BackendErrorKind::Unauthenticated,
                403 => BackendErrorKind::PermissionDenied,
                404 => BackendErrorKind::NotFound,
                409 => BackendErrorKind::Conflict,
                429 => BackendErrorKind::RateLimited,
                500..=599 => BackendErrorKind::Unavailable,
                _ => BackendErrorKind::Internal,
            }));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| error(BackendErrorKind::Unavailable))?
        {
            if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
                return Err(error(BackendErrorKind::Internal));
            }
            bytes.extend_from_slice(&chunk);
        }
        parse(
            &serde_json::from_slice(&bytes).map_err(|_| error(BackendErrorKind::Internal))?,
            kind,
            id,
        )
    }
    pub async fn resolve_lifecycle(
        &self,
        page: &PageId,
        scope: &LifecycleScope,
    ) -> Result<LifecycleEvidence, BackendError> {
        let scope = normalize(scope)?;
        let mut current = (LifecycleKind::Page, page_id(&page.0)?.0);
        let mut ancestry = Vec::new();
        let mut seen = BTreeSet::new();
        loop {
            if ancestry.len() == MAX_LIFECYCLE_NODES || !seen.insert(current.clone()) {
                return Err(error(BackendErrorKind::UnsupportedContent));
            }
            let node = self.lifecycle_node(current.0, &current.1).await?;
            let stop = matches!(node.parent, PhysicalParent::Workspace)
                || (ancestry.is_empty() && node.inactive);
            if !ancestry.is_empty() && node.inactive {
                return Err(error(BackendErrorKind::Conflict));
            }
            if let PhysicalParent::Object { kind, id } = &node.parent {
                current = (*kind, id.clone());
            }
            ancestry.push(node);
            if stop {
                break;
            }
        }
        // Bracket the chain with a reverse metadata pass. Changes noticed during
        // traversal fail; no finite number of reads creates an atomic snapshot.
        for node in ancestry.iter().rev() {
            if self.lifecycle_node(node.kind, &node.id).await? != *node {
                return Err(error(BackendErrorKind::Conflict));
            }
        }
        let status = if ancestry[0].inactive {
            LifecycleStatus::Inactive
        } else {
            let exclusion = ancestry.iter().enumerate().find_map(|(index, node)| {
                let source = match node.kind {
                    LifecycleKind::Page => Some(SourceType::Page),
                    LifecycleKind::Database => Some(SourceType::Database),
                    LifecycleKind::DataSource => Some(SourceType::DataSource),
                    LifecycleKind::Block => None,
                };
                source
                    .and_then(|s| scope.exclusions.exclusion(&node.id, s))
                    .or_else(|| {
                        (index > 0
                            && node.kind == LifecycleKind::Page
                            && scope.exclusions.descendants_of.contains(&node.id))
                        .then_some(SkipReason::ExcludedDescendants)
                    })
                    .map(|reason| LifecycleStatus::Excluded {
                        id: node.id.clone(),
                        reason,
                    })
            });
            if let Some(excluded) = exclusion {
                excluded
            } else {
                let roots: Vec<_> = scope
                    .roots
                    .iter()
                    .filter(|root| {
                        ancestry
                            .iter()
                            .any(|n| n.kind == LifecycleKind::Page && n.id == root.0)
                    })
                    .map(|r| r.0.clone())
                    .collect();
                if roots.is_empty() {
                    LifecycleStatus::OutsideScope
                } else {
                    LifecycleStatus::Allowed { roots }
                }
            }
        };
        Ok(LifecycleEvidence {
            scope,
            status,
            ancestry,
        })
    }
}
impl PageLifecycle for NotionClient {
    fn lifecycle<'a>(
        &'a self,
        page: &'a PageId,
        scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, LifecycleEvidence> {
        Box::pin(self.resolve_lifecycle(page, scope))
    }
    fn revalidate_lifecycle<'a>(
        &'a self,
        evidence: &'a LifecycleEvidence,
        current_scope: &'a LifecycleScope,
    ) -> BackendFuture<'a, ()> {
        Box::pin(async move {
            if normalize(current_scope)? != evidence.scope {
                return Err(error(BackendErrorKind::Conflict));
            }
            let page = evidence
                .ancestry
                .first()
                .filter(|n| n.kind == LifecycleKind::Page)
                .ok_or_else(|| error(BackendErrorKind::InvalidInput))?;
            let fresh = self
                .resolve_lifecycle(&PageId(page.id.clone()), current_scope)
                .await?;
            if fresh != *evidence {
                return Err(error(BackendErrorKind::Conflict));
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    const PAGE: &str = "11111111-1111-1111-1111-111111111111";
    const ROOT: &str = "22222222-2222-2222-2222-222222222222";
    const OTHER: &str = "33333333-3333-3333-3333-333333333333";
    const SOURCE: &str = "44444444-4444-4444-4444-444444444444";
    const DB: &str = "55555555-5555-5555-5555-555555555555";
    const BLOCK: &str = "66666666-6666-6666-6666-666666666666";
    fn scope() -> LifecycleScope {
        LifecycleScope {
            workspace_id: OTHER.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        }
    }
    fn node(object: &str, id: &str, parent: Value) -> Value {
        json!({"object":object,"id":id,"type":"paragraph","last_edited_time":"2026-10-09T01:00:00Z","in_trash":false,"parent":parent,"private_content":"must not be returned"})
    }
    fn p(kind: &str, id: &str) -> Value {
        json!({"type":kind,kind:id})
    }
    fn workspace() -> Value {
        json!({"type":"workspace","workspace":true})
    }
    fn chain(parent: &str) -> Vec<(String, u16, Value)> {
        let selected = node("page", PAGE, p("page_id", parent));
        let root = node("page", parent, workspace());
        vec![
            (format!("pages/{PAGE}"), 200, selected.clone()),
            (format!("pages/{parent}"), 200, root.clone()),
            (format!("pages/{parent}"), 200, root),
            (format!("pages/{PAGE}"), 200, selected),
        ]
    }
    async fn fixture(
        responses: Vec<(String, u16, Value)>,
    ) -> (NotionClient, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("test-credential")
            .unwrap()
            .without_retries();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for (path, status, value) in responses {
                let (mut stream, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut buf = [0; 1024];
                    let n = stream.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if bytes.windows(4).any(|v| v == b"\r\n\r\n") {
                        break;
                    }
                }
                let headers = String::from_utf8(bytes).unwrap();
                assert!(
                    headers.starts_with(&format!("GET /v1/{path} HTTP/1.1\r\n")),
                    "{headers}"
                );
                assert!(headers.contains("notion-version: 2026-03-11"));
                assert!(headers.contains("authorization: Bearer test-credential"));
                let body = value.to_string();
                stream.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
        });
        (client, server)
    }
    #[tokio::test]
    async fn moves_restore_and_inactive_are_authoritative_metadata_only() {
        for (parent, allowed) in [(ROOT, true), (OTHER, false)] {
            let (client, server) = fixture(chain(parent)).await;
            let port: &dyn PageLifecycle = &client;
            let evidence = port
                .lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap();
            assert_eq!(
                evidence.status,
                if allowed {
                    LifecycleStatus::Allowed {
                        roots: vec![ROOT.into()],
                    }
                } else {
                    LifecycleStatus::OutsideScope
                }
            );
            assert_eq!(evidence.ancestry.len(), 2);
            assert!(!format!("{evidence:?}").contains("must not be returned"));
            assert_eq!(client.request_metrics().attempts, 4);
            server.await.unwrap();
        }
        for field in ["in_trash", "is_archived", "archived"] {
            let mut value = node("page", PAGE, p("page_id", OTHER));
            value[field] = json!(true);
            let (client, server) = fixture(vec![
                (format!("pages/{PAGE}"), 200, value.clone()),
                (format!("pages/{PAGE}"), 200, value),
            ])
            .await;
            let evidence = client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap();
            assert_eq!(evidence.status, LifecycleStatus::Inactive);
            assert_eq!(evidence.ancestry.len(), 1); // No request to inaccessible parent.
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn physical_source_chain_and_overlapping_roots_preserve_exclusion_precedence() {
        let nodes = [
            (
                format!("pages/{PAGE}"),
                node("page", PAGE, p("data_source_id", SOURCE)),
            ),
            (
                format!("data_sources/{SOURCE}"),
                node("data_source", SOURCE, p("database_id", DB)),
            ),
            (
                format!("databases/{DB}"),
                node("database", DB, p("block_id", BLOCK)),
            ),
            (
                format!("blocks/{BLOCK}"),
                node("block", BLOCK, p("page_id", ROOT)),
            ),
            (
                format!("pages/{ROOT}"),
                node("page", ROOT, p("page_id", OTHER)),
            ),
            (format!("pages/{OTHER}"), node("page", OTHER, workspace())),
        ];
        for excluded in [
            None,
            Some(SourceType::Page),
            Some(SourceType::Database),
            Some(SourceType::DataSource),
        ] {
            let responses = nodes
                .iter()
                .chain(nodes.iter().rev())
                .map(|(p, v)| (p.clone(), 200, v.clone()))
                .collect();
            let (client, server) = fixture(responses).await;
            let mut config = scope();
            config.roots = vec![
                PageId(ROOT.replace('-', "").to_uppercase()),
                PageId(OTHER.into()),
                PageId(ROOT.into()),
            ];
            if let Some(kind) = excluded {
                config.exclusions.source_types.insert(kind);
            }
            let evidence = client
                .resolve_lifecycle(&PageId(PAGE.into()), &config)
                .await
                .unwrap();
            if excluded.is_some() {
                assert!(matches!(
                    evidence.status,
                    LifecycleStatus::Excluded {
                        reason: SkipReason::ExcludedSourceType,
                        ..
                    }
                ));
            } else {
                assert_eq!(
                    evidence.status,
                    LifecycleStatus::Allowed {
                        roots: vec![ROOT.into(), OTHER.into()]
                    }
                );
            }
            assert_eq!(evidence.ancestry.len(), 6);
            server.await.unwrap();
        }
        for descendants in [false, true] {
            let responses = nodes
                .iter()
                .chain(nodes.iter().rev())
                .map(|(p, v)| (p.clone(), 200, v.clone()))
                .collect();
            let (client, server) = fixture(responses).await;
            let mut config = scope();
            if descendants {
                config.exclusions.descendants_of.insert(OTHER.into());
            } else {
                config.exclusions.page_ids.insert(OTHER.into());
            }
            assert_eq!(
                client
                    .resolve_lifecycle(&PageId(PAGE.into()), &config)
                    .await
                    .unwrap()
                    .status,
                LifecycleStatus::Excluded {
                    id: OTHER.into(),
                    reason: if descendants {
                        SkipReason::ExcludedDescendants
                    } else {
                        SkipReason::ExcludedPage
                    }
                }
            );
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn access_failures_partial_metadata_and_cycles_never_authorize_removal() {
        for status in [401, 403, 404, 429, 503] {
            for ancestor in [false, true] {
                let mut responses = vec![];
                if ancestor {
                    responses.push((
                        format!("pages/{PAGE}"),
                        200,
                        node("page", PAGE, p("page_id", ROOT)),
                    ));
                }
                responses.push((
                    format!("pages/{}", if ancestor { ROOT } else { PAGE }),
                    status,
                    json!({"private":"upstream-secret"}),
                ));
                let (client, server) = fixture(responses).await;
                let failure = client
                    .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                    .await
                    .unwrap_err();
                assert!(!format!("{failure:?}").contains("upstream-secret"));
                server.await.unwrap();
            }
        }
        let valid = node("page", PAGE, workspace());
        let mut cases = vec![];
        for key in ["object", "id", "parent", "last_edited_time", "in_trash"] {
            let mut v = valid.clone();
            v.as_object_mut().unwrap().remove(key);
            cases.push(v);
        }
        for parent in [
            json!({"type":"workspace","workspace":false}),
            json!({"type":"agent_id","agent_id":ROOT}),
            json!({"type":"page_id","page_id":"bad"}),
        ] {
            let mut v = valid.clone();
            v["parent"] = parent;
            cases.push(v);
        }
        let mut v = valid.clone();
        v["in_trash"] = json!("false");
        cases.push(v);
        let mut v = valid;
        v["last_edited_time"] = json!("yesterday");
        cases.push(v);
        for value in cases {
            let (client, server) = fixture(vec![(format!("pages/{PAGE}"), 200, value)]).await;
            assert!(
                client
                    .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                    .await
                    .is_err()
            );
            server.await.unwrap();
        }
        let (client, server) = fixture(vec![
            (
                format!("pages/{PAGE}"),
                200,
                node("page", PAGE, p("page_id", ROOT)),
            ),
            (
                format!("pages/{ROOT}"),
                200,
                node("page", ROOT, p("page_id", PAGE)),
            ),
        ])
        .await;
        assert_eq!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::UnsupportedContent
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn source_revision_move_and_scope_generation_changes_reject_revalidation() {
        let mut responses = chain(ROOT);
        responses.extend(chain(OTHER));
        let (client, server) = fixture(responses).await;
        let config = scope();
        let evidence = client
            .resolve_lifecycle(&PageId(PAGE.into()), &config)
            .await
            .unwrap();
        let mut changed = config.clone();
        changed.generation += 1;
        assert_eq!(
            client
                .revalidate_lifecycle(&evidence, &changed)
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Conflict
        );
        assert_eq!(client.request_metrics().attempts, 4); // Scope rejection before I/O.
        assert_eq!(
            client
                .revalidate_lifecycle(&evidence, &config)
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Conflict
        );
        server.await.unwrap();
        let mut responses = chain(ROOT);
        responses[3].2["last_edited_time"] = json!("2026-10-09T02:00:00Z");
        let (client, server) = fixture(responses).await;
        assert_eq!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Conflict
        );
        server.await.unwrap();
        let mut responses = chain(ROOT);
        responses.extend(chain(ROOT));
        let (client, server) = fixture(responses).await;
        let evidence = client
            .resolve_lifecycle(&PageId(PAGE.into()), &scope())
            .await
            .unwrap();
        client
            .revalidate_lifecycle(&evidence, &scope())
            .await
            .unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn inactive_ancestor_is_distinct_from_affirmatively_inactive_selected_page() {
        let mut ancestor = node("page", ROOT, workspace());
        ancestor["in_trash"] = json!(true);
        let (client, server) = fixture(vec![
            (
                format!("pages/{PAGE}"),
                200,
                node("page", PAGE, p("page_id", ROOT)),
            ),
            (format!("pages/{ROOT}"), 200, ancestor),
        ])
        .await;
        assert_eq!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Conflict
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn depth_limit_fails_without_partial_authority_or_extra_requests() {
        let mut responses = Vec::new();
        for index in 0..MAX_LIFECYCLE_NODES {
            let id = if index == 0 {
                PAGE.to_owned()
            } else {
                format!("{:08x}-7777-7777-7777-777777777777", index)
            };
            let next = format!("{:08x}-7777-7777-7777-777777777777", index + 1);
            responses.push((
                format!("pages/{id}"),
                200,
                node("page", &id, p("page_id", &next)),
            ));
        }
        let (client, server) = fixture(responses).await;
        assert_eq!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::UnsupportedContent
        );
        assert_eq!(
            client.request_metrics().attempts,
            MAX_LIFECYCLE_NODES as u64
        );
        server.await.unwrap();
    }
    #[tokio::test]
    async fn supported_wiki_parent_chains_legacy_status_and_descendants_only_rules() {
        // Database -> data source -> database is a physical wiki relationship;
        // ordinary blocks can have data-source parents, too.
        let nodes = [
            (
                format!("pages/{PAGE}"),
                node("page", PAGE, p("block_id", BLOCK)),
            ),
            (
                format!("blocks/{BLOCK}"),
                node("block", BLOCK, p("data_source_id", SOURCE)),
            ),
            (
                format!("data_sources/{SOURCE}"),
                node("data_source", SOURCE, p("data_source_id", OTHER)),
            ),
            (
                format!("data_sources/{OTHER}"),
                node("data_source", OTHER, p("database_id", DB)),
            ),
            (
                format!("databases/{DB}"),
                node("database", DB, p("page_id", ROOT)),
            ),
            (format!("pages/{ROOT}"), node("page", ROOT, workspace())),
        ];
        let responses = nodes
            .iter()
            .chain(nodes.iter().rev())
            .map(|(p, v)| {
                let mut v = v.clone();
                v.as_object_mut().unwrap().remove("in_trash");
                v["archived"] = json!(false);
                (p.clone(), 200, v)
            })
            .collect();
        let (client, server) = fixture(responses).await;
        assert!(matches!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap()
                .status,
            LifecycleStatus::Allowed { .. }
        ));
        server.await.unwrap();
        let value = node("page", ROOT, workspace());
        let (client, server) = fixture(vec![
            (format!("pages/{ROOT}"), 200, value.clone()),
            (format!("pages/{ROOT}"), 200, value),
        ])
        .await;
        let mut config = scope();
        config.exclusions.descendants_of.insert(ROOT.into());
        assert!(matches!(
            client
                .resolve_lifecycle(&PageId(ROOT.into()), &config)
                .await
                .unwrap()
                .status,
            LifecycleStatus::Allowed { .. }
        ));
        server.await.unwrap();
    }
    #[tokio::test]
    async fn shared_retry_body_bounds_and_scope_binding_use_the_real_adapter() {
        let mut responses = vec![(
            format!("pages/{PAGE}"),
            503,
            json!({"private":"unavailable"}),
        )];
        responses.extend(chain(ROOT));
        let (mut client, server) = fixture(responses).await;
        client.transport = Default::default();
        assert!(matches!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap()
                .status,
            LifecycleStatus::Allowed { .. }
        ));
        assert_eq!(client.request_metrics().retries, 1);
        server.await.unwrap();
        let mut large = node("page", PAGE, workspace());
        large["private_content"] = json!("x".repeat(2 * 1024 * 1024));
        let (client, server) = fixture(vec![(format!("pages/{PAGE}"), 200, large)]).await;
        assert_eq!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Internal
        );
        server.await.unwrap();
        let (client, server) = fixture(chain(ROOT)).await;
        let evidence = client
            .resolve_lifecycle(&PageId(PAGE.into()), &scope())
            .await
            .unwrap();
        for variant in 0..4 {
            let mut changed = scope();
            match variant {
                0 => changed.workspace_id = ROOT.into(),
                1 => changed.roots = vec![PageId(OTHER.into())],
                2 => {
                    changed.exclusions.page_ids.insert(PAGE.into());
                }
                _ => changed.generation += 1,
            }
            assert_eq!(
                client
                    .revalidate_lifecycle(&evidence, &changed)
                    .await
                    .unwrap_err()
                    .kind,
                BackendErrorKind::Conflict
            );
        }
        let forged = LifecycleEvidence {
            ancestry: vec![],
            ..evidence.clone()
        };
        assert_eq!(
            client
                .revalidate_lifecycle(&forged, &scope())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::InvalidInput
        );
        assert_eq!(client.request_metrics().attempts, 4);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn wiki_database_parent_and_malformed_block_edges_are_checked_authoritatively() {
        let nodes = [
            (
                format!("pages/{PAGE}"),
                node("page", PAGE, p("database_id", DB)),
            ),
            (
                format!("databases/{DB}"),
                node("database", DB, p("data_source_id", SOURCE)),
            ),
            (
                format!("data_sources/{SOURCE}"),
                node("data_source", SOURCE, p("database_id", OTHER)),
            ),
            (
                format!("databases/{OTHER}"),
                node("database", OTHER, p("page_id", ROOT)),
            ),
            (format!("pages/{ROOT}"), node("page", ROOT, workspace())),
        ];
        let (client, server) = fixture(
            nodes
                .iter()
                .chain(nodes.iter().rev())
                .map(|(p, v)| (p.clone(), 200, v.clone()))
                .collect(),
        )
        .await;
        assert!(matches!(
            client
                .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                .await
                .unwrap()
                .status,
            LifecycleStatus::Allowed { .. }
        ));
        server.await.unwrap();
        let valid = node("block", BLOCK, p("page_id", ROOT));
        let mut cases = Vec::new();
        let mut v = valid.clone();
        v.as_object_mut().unwrap().remove("type");
        cases.push(v);
        let mut v = valid.clone();
        v["type"] = json!("synced_block");
        cases.push(v.clone());
        v["synced_block"] = json!({"synced_from":{"type":"block_id","block_id":OTHER}});
        cases.push(v);
        let mut v = valid.clone();
        v["parent"] = workspace();
        cases.push(v);
        let mut v = valid;
        v["parent"]["workspace"] = json!(true);
        cases.push(v);
        for block in cases {
            let (client, server) = fixture(vec![
                (
                    format!("pages/{PAGE}"),
                    200,
                    node("page", PAGE, p("block_id", BLOCK)),
                ),
                (format!("blocks/{BLOCK}"), 200, block),
            ])
            .await;
            assert!(
                client
                    .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                    .await
                    .is_err()
            );
            server.await.unwrap();
        }
        let (client, server) = fixture(Vec::new()).await;
        for variant in 0..4 {
            let mut invalid = scope();
            match variant {
                0 => invalid.workspace_id = "bad".into(),
                1 => invalid.roots.clear(),
                2 => {
                    invalid.exclusions.page_ids.insert("bad".into());
                }
                _ => invalid.roots = vec![PageId(ROOT.into()); MAX_LIFECYCLE_NODES + 1],
            }
            assert!(
                client
                    .resolve_lifecycle(&PageId(PAGE.into()), &invalid)
                    .await
                    .is_err()
            );
        }
        assert_eq!(client.request_metrics().attempts, 0);
        server.await.unwrap();
    }
    #[tokio::test]
    async fn changed_ancestor_revision_and_parent_fail_the_reverse_observation_pass() {
        for field in ["parent", "last_edited_time"] {
            let mut responses = chain(ROOT);
            responses.truncate(3);
            responses[2].2[field] = if field == "parent" {
                p("page_id", OTHER)
            } else {
                json!("2026-10-09T02:00:00Z")
            };
            let (client, server) = fixture(responses).await;
            assert_eq!(
                client
                    .resolve_lifecycle(&PageId(PAGE.into()), &scope())
                    .await
                    .unwrap_err()
                    .kind,
                BackendErrorKind::Conflict
            );
            server.await.unwrap();
        }
    }
}
