//! Native Notion image attachment scoped to an operator-approved physical page.
//!
//! No MCP tool is activated here. A confirmed upload ID is not authorization to
//! mutate an arbitrary page. The caller must provide trusted root policy.
use std::sync::Arc;

use crate::{
    NotionClient,
    file_upload::{UploadStatus, UploadedFile},
    pages,
};
use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind, PageId},
    lifecycle::LifecycleScope,
    root_scope::{RootScopeGate, ScopeError},
};
use serde_json::{Value, json};

const ATTACH: &str = "notion.image.attach";
const VERIFY: &str = "notion.image.verify";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedImage {
    pub page_id: PageId,
    pub block_id: String,
    pub upload_id: String,
    /// True only when a fresh authoritative block GET confirms the image.
    pub verified: bool,
}

fn error(operation: &'static str, kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation,
        kind,
        retry_after: None,
        committed_page_id: None,
    }
}

fn scope_error(value: ScopeError) -> BackendError {
    error(
        ATTACH,
        match value {
            ScopeError::Denied => BackendErrorKind::PermissionDenied,
            ScopeError::Changed => BackendErrorKind::Conflict,
            ScopeError::Unverifiable => BackendErrorKind::Unavailable,
            ScopeError::InvalidConfiguration => BackendErrorKind::InvalidInput,
        },
    )
}

fn caption(value: Option<&str>) -> Result<Vec<Value>, BackendError> {
    let Some(text) = value.map(str::trim).filter(|text| !text.is_empty()) else {
        return Ok(Vec::new());
    };
    if text.chars().count() > 2000 || text.chars().any(|ch| {
        ch.is_control() || matches!(ch, '\u{200e}' | '\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}')
    }) {
        return Err(error(ATTACH, BackendErrorKind::InvalidInput));
    }
    Ok(vec![json!({"type":"text", "text":{"content":text}})])
}

fn valid_upload(upload: &UploadedFile) -> bool {
    upload.status == UploadStatus::Uploaded
        && pages::uuid(&upload.id).as_deref() == Some(upload.id.as_str())
        && upload.size_bytes > 0
        && matches!(
            upload.mime_type.as_str(),
            "image/png" | "image/jpeg" | "image/gif" | "image/webp"
        )
        && !upload.filename.is_empty()
}

fn block_id(value: &Value, parent: &PageId) -> Option<String> {
    let id = pages::uuid(value["id"].as_str()?)?;
    if value["object"] != "block"
        || value["type"] != "image"
        || value["in_trash"] != false
        || value["archived"] == true
        || value["parent"]["type"] != "page_id"
        || pages::uuid(value["parent"]["page_id"].as_str()?).as_deref() != Some(parent.0.as_str())
    {
        return None;
    }
    Some(id)
}

fn readback_ok(
    value: &Value,
    parent: &PageId,
    block: &str,
    upload: &str,
    caption: &[Value],
) -> bool {
    if block_id(value, parent).as_deref() != Some(block) {
        return false;
    }
    let image = &value["image"];
    // Notion may resolve uploaded IDs to temporary signed file objects on read.
    let source_ok = match image["type"].as_str() {
        Some("file_upload") => {
            image["file_upload"]["id"]
                .as_str()
                .and_then(pages::uuid)
                .as_deref()
                == Some(upload)
        }
        Some("file") => image["file"]["url"]
            .as_str()
            .is_some_and(|url| url.starts_with("https://")),
        _ => false,
    };
    if !source_ok {
        return false;
    }
    let expected = caption.first().and_then(|c| c["text"]["content"].as_str());
    let actual = image["caption"].as_array();
    match (expected, actual) {
        (None, Some(items)) => items.is_empty(),
        (Some(want), Some(items)) if items.len() == 1 => {
            items[0]["type"] == "text" && items[0]["plain_text"].as_str() == Some(want)
        }
        _ => false,
    }
}

impl NotionClient {
    /// Append one native image to an explicit page and confirm it with a block GET.
    ///
    /// Only server-owned `scope` is permitted; never derive it from user input.
    /// Target is a PAGE UUID, not an arbitrary block, database or uploaded URL.
    /// The physical ancestry is checked twice immediately before the mutation.
    /// No mutation replay is attempted after a transport error or ambiguous reply.
    pub async fn attach_uploaded_image(
        &self,
        target: &PageId,
        upload: &UploadedFile,
        text: Option<&str>,
        scope: &LifecycleScope,
    ) -> Result<AttachedImage, BackendError> {
        if pages::uuid(&target.0).as_deref() != Some(target.0.as_str()) || !valid_upload(upload) {
            return Err(error(ATTACH, BackendErrorKind::InvalidInput));
        }
        let caption = caption(text)?;
        let gate =
            RootScopeGate::new(Arc::new(self.clone()), scope.clone()).map_err(scope_error)?;
        let permit = gate.authorize(target).await.map_err(scope_error)?;
        // Pre-mutation revalidation deliberately repeats fresh parent/source
        // ancestry; a stale index root or forged target cannot authorize it.
        gate.revalidate(&permit, scope).await.map_err(scope_error)?;

        let added = self
            .write(
                reqwest::Method::PATCH,
                &format!("/blocks/{}/children", target.0),
                json!({"children":[{
                    "object":"block",
                    "type":"image",
                    "image":{
                        "type":"file_upload",
                        "file_upload":{"id":upload.id},
                        "caption":caption
                    }
                }]}),
                ATTACH,
            )
            .await?;
        // A successful HTTP status is not a confirmed attachment: inspect the
        // created block receipt, then read the actual block from Notion.
        let results = added["results"]
            .as_array()
            .filter(|r| added["object"] == "list" && r.len() == 1)
            .ok_or_else(|| error(ATTACH, BackendErrorKind::Conflict))?;
        let block = block_id(&results[0], target)
            .ok_or_else(|| error(ATTACH, BackendErrorKind::Conflict))?;

        self.verify_attached_block(&block, VERIFY, |value| {
            readback_ok(value, target, &block, &upload.id, &caption)
        })
        .await?;
        Ok(AttachedImage {
            page_id: target.clone(),
            block_id: block,
            upload_id: upload.id.clone(),
            verified: true,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notion_knowledge_core::discovery::ExclusionRules;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PAGE: &str = "11111111-1111-1111-1111-111111111111";
    const ROOT: &str = "22222222-2222-2222-2222-222222222222";
    const OUTSIDE: &str = "33333333-3333-3333-3333-333333333333";
    const WORKSPACE: &str = "44444444-4444-4444-4444-444444444444";
    const UPLOAD: &str = "55555555-5555-5555-5555-555555555555";
    const BLOCK: &str = "66666666-6666-6666-6666-666666666666";

    fn policy() -> LifecycleScope {
        LifecycleScope {
            workspace_id: WORKSPACE.into(),
            generation: 1,
            roots: vec![PageId(ROOT.into())],
            exclusions: ExclusionRules::default(),
        }
    }
    fn upload(mime: &str) -> UploadedFile {
        UploadedFile {
            id: UPLOAD.into(),
            status: UploadStatus::Uploaded,
            filename: "puffin.png".into(),
            mime_type: mime.into(),
            size_bytes: 16,
        }
    }
    fn node(id: &str, parent: Option<&str>) -> Value {
        json!({"object":"page", "id":id, "last_edited_time":"2026-10-11T09:00:00Z",
        "in_trash":false,
        "parent":match parent {
            Some(id) => json!({"type":"page_id","page_id":id}),
            None => json!({"type":"workspace","workspace":true})
        }})
    }
    fn chain(parent: &str) -> Vec<(String, u16, Value)> {
        let page = node(PAGE, Some(parent));
        let root = node(parent, None);
        vec![
            (format!("GET /v1/pages/{PAGE}"), 200, page.clone()),
            (format!("GET /v1/pages/{parent}"), 200, root.clone()),
            (format!("GET /v1/pages/{parent}"), 200, root),
            (format!("GET /v1/pages/{PAGE}"), 200, page),
        ]
    }
    fn block(kind: &str, parent: &str, body: Value) -> Value {
        json!({"object":"block","id":BLOCK,"type":kind,"in_trash":false,
            "parent":{"type":"page_id","page_id":parent},"image":body})
    }
    fn append() -> (String, u16, Value) {
        (
            "PATCH /v1/blocks/11111111-1111-1111-1111-111111111111/children".into(),
            200,
            json!({"object":"list","results":[block("image",PAGE,json!({}))]}),
        )
    }
    fn readback(parent: &str) -> (String, u16, Value) {
        (
            format!("GET /v1/blocks/{BLOCK}"),
            200,
            block(
                "image",
                parent,
                json!({
                    "type":"file",
                    "file":{"url":"https://files.notion.so/temporary-image"},
                    "caption":[{"type":"text","plain_text":"Hans visar upp sina fjädrar"}]
                }),
            ),
        )
    }
    async fn fixture(
        responses: Vec<(String, u16, Value)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<(String, Value)>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("fixture-secret")
            .unwrap()
            .without_retries();
        client.api_root = format!("http://{}/v1", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let mut requests = Vec::new();
            for (path, status, response) in responses {
                let (mut socket, _) =
                    tokio::time::timeout(std::time::Duration::from_secs(10), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                let mut bytes = Vec::new();
                let (end, length) = loop {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        let heads = String::from_utf8_lossy(&bytes[..pos]).to_lowercase();
                        assert!(heads.starts_with(&path.to_lowercase()), "expected {path}");
                        assert!(heads.contains("authorization: bearer fixture-secret"));
                        let length = heads
                            .lines()
                            .find_map(|line| {
                                line.strip_prefix("content-length: ")
                                    .and_then(|s| s.parse::<usize>().ok())
                            })
                            .unwrap_or(0);
                        break (pos + 4, length);
                    }
                };
                while bytes.len() < end + length {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buf[..n]);
                }
                let body = if length == 0 {
                    Value::Null
                } else {
                    serde_json::from_slice(&bytes[end..end + length]).unwrap()
                };
                requests.push((path, body));
                let json = response.to_string();
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json}",json.len()).as_bytes()).await.unwrap();
            }
            requests
        });
        (client, handle)
    }

    #[tokio::test]
    async fn authorized_native_image_append_and_block_readback() {
        let mut steps = chain(ROOT);
        steps.extend(chain(ROOT));
        steps.push(append());
        steps.push(readback(PAGE));
        let (client, server) = fixture(steps).await;
        let result = client
            .attach_uploaded_image(
                &PageId(PAGE.into()),
                &upload("image/png"),
                Some(" Hans visar upp sina fjädrar "),
                &policy(),
            )
            .await
            .unwrap();
        assert_eq!(result.page_id.0, PAGE);
        assert_eq!(result.block_id, BLOCK);
        assert_eq!(result.upload_id, UPLOAD);
        assert!(result.verified);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 10);
        assert_eq!(
            requests[8].1,
            json!({"children":[{
                "object":"block","type":"image",
                "image":{"type":"file_upload","file_upload":{"id":UPLOAD},
                    "caption":[{"type":"text","text":{"content":"Hans visar upp sina fjädrar"}}]}
            }]})
        );
        assert_eq!(requests[9].0, format!("GET /v1/blocks/{BLOCK}"));
    }

    #[tokio::test]
    async fn outside_root_never_attaches() {
        let (client, server) = fixture(chain(OUTSIDE)).await;
        let err = client
            .attach_uploaded_image(&PageId(PAGE.into()), &upload("image/png"), None, &policy())
            .await
            .unwrap_err();
        assert_eq!(err.kind, BackendErrorKind::PermissionDenied);
        assert_eq!(server.await.unwrap().len(), 4);
    }

    #[tokio::test]
    async fn invalid_target_upload_or_caption_never_enters_network() {
        let client = NotionClient::integration("fixture-secret").unwrap();
        for (target, uploaded, text) in [
            (PageId("../notion".into()), upload("image/png"), None),
            (PageId(PAGE.into()), upload("application/pdf"), None),
            (
                PageId(PAGE.into()),
                upload("image/png"),
                Some("\u{202e}bad"),
            ),
        ] {
            let err = client
                .attach_uploaded_image(&target, &uploaded, text, &policy())
                .await
                .unwrap_err();
            assert_eq!(err.kind, BackendErrorKind::InvalidInput);
        }
    }

    #[tokio::test]
    async fn stale_lifecycle_denies_append() {
        let mut steps = chain(ROOT);
        let mut changed = chain(ROOT);
        changed[0].2["last_edited_time"] = json!("2026-10-11T10:00:00Z");
        changed[3].2["last_edited_time"] = json!("2026-10-11T10:00:00Z");
        steps.extend(changed);
        let (client, server) = fixture(steps).await;
        assert_eq!(
            client
                .attach_uploaded_image(&PageId(PAGE.into()), &upload("image/png"), None, &policy())
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Conflict
        );
        assert_eq!(server.await.unwrap().len(), 8);
    }

    #[tokio::test]
    async fn bad_receipt_or_missing_readback_never_claims_attachment() {
        for bad_receipt in [true, false] {
            let mut steps = chain(ROOT);
            steps.extend(chain(ROOT));
            if bad_receipt {
                steps.push((
                    append().0,
                    200,
                    json!({"object":"list","results":[
                    block("file",PAGE,json!({}))]}),
                ));
            } else {
                steps.push(append());
                steps.push(readback(OUTSIDE));
            }
            let (client, server) = fixture(steps).await;
            let err = client
                .attach_uploaded_image(
                    &PageId(PAGE.into()),
                    &upload("image/png"),
                    Some("Hans visar upp sina fjädrar"),
                    &policy(),
                )
                .await
                .unwrap_err();
            assert_eq!(err.kind, BackendErrorKind::Conflict);
            assert_eq!(err.operation, if bad_receipt { ATTACH } else { VERIFY });
            server.await.unwrap();
        }
    }
    #[tokio::test]
    async fn empty_caption_and_exact_upload_readback_are_supported() {
        let mut steps = chain(ROOT);
        steps.extend(chain(ROOT));
        steps.push(append());
        steps.push((
            format!("GET /v1/blocks/{BLOCK}"),
            200,
            block(
                "image",
                PAGE,
                json!({
                    "type":"file_upload",
                    "file_upload":{"id":UPLOAD},
                    "caption":[]
                }),
            ),
        ));
        let (client, server) = fixture(steps).await;
        let attached = client
            .attach_uploaded_image(&PageId(PAGE.into()), &upload("image/webp"), None, &policy())
            .await
            .unwrap();
        assert!(attached.verified);
        let requests = server.await.unwrap();
        assert_eq!(requests[8].1["children"][0]["image"]["caption"], json!([]));
    }

    #[test]
    fn readback_rejects_missing_active_status_and_wrong_upload() {
        let mut returned = block(
            "image",
            PAGE,
            json!({"type":"file_upload","file_upload":{"id":OUTSIDE},"caption":[]}),
        );
        assert!(!readback_ok(
            &returned,
            &PageId(PAGE.into()),
            BLOCK,
            UPLOAD,
            &[]
        ));
        returned["image"]["file_upload"]["id"] = json!(UPLOAD);
        returned.as_object_mut().unwrap().remove("in_trash");
        assert!(!readback_ok(
            &returned,
            &PageId(PAGE.into()),
            BLOCK,
            UPLOAD,
            &[]
        ));
    }
    #[tokio::test]
    async fn image_block_eventually_appears_after_initial_not_found() {
        let mut steps = chain(ROOT);
        steps.extend(chain(ROOT));
        steps.push(append());
        steps.push((
            format!("GET /v1/blocks/{BLOCK}"),
            404,
            json!({"code":"not_found"}),
        ));
        steps.push(readback(PAGE));
        let (client, server) = fixture(steps).await;
        let attached = client
            .attach_uploaded_image(
                &PageId(PAGE.into()),
                &upload("image/png"),
                Some("Hans visar upp sina fjädrar"),
                &policy(),
            )
            .await
            .unwrap();
        assert!(attached.verified);
        assert_eq!(server.await.unwrap().len(), 11);
    }

    #[tokio::test]
    async fn image_block_never_appears_returns_error_without_second_append() {
        let mut steps = chain(ROOT);
        steps.extend(chain(ROOT));
        steps.push(append());
        for _ in 0..3 {
            steps.push((
                format!("GET /v1/blocks/{BLOCK}"),
                404,
                json!({"code":"not_found"}),
            ));
        }
        let (client, server) = fixture(steps).await;
        let failure = client
            .attach_uploaded_image(&PageId(PAGE.into()), &upload("image/png"), None, &policy())
            .await
            .unwrap_err();
        assert_eq!(failure.kind, BackendErrorKind::Conflict);
        assert_eq!(failure.operation, VERIFY);
        let requests = server.await.unwrap();
        assert_eq!(requests.len(), 12);
        assert_eq!(
            requests
                .iter()
                .filter(|(method, _)| method.starts_with("PATCH"))
                .count(),
            1
        );
    }
}
