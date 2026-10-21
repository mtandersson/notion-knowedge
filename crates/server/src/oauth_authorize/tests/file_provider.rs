//! Native upload/block receipts from a local fake provider, never a ranker.
use super::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Mutex};
#[derive(Clone)]
struct Upload {
    name: String,
    total: usize,
    sent: usize,
}
pub(super) struct Files {
    uploads: Mutex<BTreeMap<String, Upload>>,
    pub blocks: Mutex<BTreeMap<String, Value>>,
    pub created: AtomicUsize,
    pub sent: AtomicUsize,
    pub completed: AtomicUsize,
    pub attached: AtomicUsize,
    pub read_back: AtomicUsize,
    pub fail_send: AtomicUsize,
    pub pause_attach: AtomicBool,
    pub entered: tokio::sync::Notify,
    pub release: tokio::sync::Semaphore,
}
impl Default for Files {
    fn default() -> Self {
        Self {
            uploads: Default::default(),
            blocks: Default::default(),
            created: 0.into(),
            sent: 0.into(),
            completed: 0.into(),
            attached: 0.into(),
            read_back: 0.into(),
            fail_send: 0.into(),
            pause_attach: false.into(),
            entered: Default::default(),
            release: tokio::sync::Semaphore::new(0),
        }
    }
}
fn allowed(headers: &HeaderMap) {
    assert!(
        matches!(
            headers["authorization"].to_str().unwrap(),
            "Bearer provider-access-secret" | "Bearer rotated-access-secret"
        ),
        "only original approved OAuth provider credentials are accepted"
    );
}
fn receipt(id: &str, upload: &Upload, uploaded: bool) -> Value {
    json!({"object":"file_upload","id":id,"status":if uploaded{"uploaded"}else{"pending"},"number_of_parts":{"total":upload.total,"sent":upload.sent}})
}
pub(super) fn routes(
    router: Router,
    files: Arc<Files>,
    document: Arc<Mutex<Option<(String, u64)>>>,
) -> Router {
    let create = files.clone();
    let send = files.clone();
    let complete = files.clone();
    let status = files.clone();
    let attach = files.clone();
    let readback = files;
    router
        .route(
            "/v1/file_uploads",
            post(
                move |headers: HeaderMap, axum::Json(body): axum::Json<Value>| {
                    let files = create.clone();
                    async move {
                        allowed(&headers);
                        let ordinal = files.created.fetch_add(1, Ordering::SeqCst) + 1;
                        let id = format!("eeeeeeee-eeee-eeee-eeee-{ordinal:012x}");
                        let upload = Upload {
                            name: body["filename"].as_str().unwrap().into(),
                            total: body["number_of_parts"].as_u64().unwrap_or(1) as usize,
                            sent: 0,
                        };
                        let value = receipt(&id, &upload, false);
                        files.uploads.lock().unwrap().insert(id, upload);
                        axum::Json(value)
                    }
                },
            ),
        )
        .route(
            "/v1/file_uploads/{id}/send",
            post(
                move |axum::extract::Path(id): axum::extract::Path<String>,
                      headers: HeaderMap,
                      bytes: axum::body::Bytes| {
                    let files = send.clone();
                    async move {
                        allowed(&headers);
                        assert!(!bytes.is_empty());
                        let ordinal = files.sent.fetch_add(1, Ordering::SeqCst) + 1;
                        if files.fail_send.load(Ordering::SeqCst) == ordinal {
                            return (StatusCode::SERVICE_UNAVAILABLE, axum::Json(json!({})));
                        }
                        let mut uploads = files.uploads.lock().unwrap();
                        let upload = uploads.get_mut(&id).unwrap();
                        upload.sent += 1;
                        (
                            StatusCode::OK,
                            axum::Json(receipt(&id, upload, upload.total == 1)),
                        )
                    }
                },
            ),
        )
        .route(
            "/v1/file_uploads/{id}/complete",
            post(
                move |axum::extract::Path(id): axum::extract::Path<String>, headers: HeaderMap| {
                    let files = complete.clone();
                    async move {
                        allowed(&headers);
                        files.completed.fetch_add(1, Ordering::SeqCst);
                        let uploads = files.uploads.lock().unwrap();
                        let upload = uploads.get(&id).unwrap();
                        assert_eq!(upload.sent, upload.total);
                        axum::Json(receipt(&id, upload, true))
                    }
                },
            ),
        )
        .route(
            "/v1/file_uploads/{id}",
            axum::routing::get(
                move |axum::extract::Path(id): axum::extract::Path<String>, headers: HeaderMap| {
                    let files = status.clone();
                    async move {
                        allowed(&headers);
                        let uploads = files.uploads.lock().unwrap();
                        axum::Json(receipt(&id, uploads.get(&id).unwrap(), true))
                    }
                },
            ),
        )
        .route(
            "/v1/blocks/{id}/children",
            axum::routing::patch(
                move |axum::extract::Path(page): axum::extract::Path<String>,
                      headers: HeaderMap,
                      axum::Json(body): axum::Json<Value>| {
                    let files = attach.clone();
                    let document = document.clone();
                    async move {
                        allowed(&headers);
                        assert_eq!(body["children"].as_array().unwrap().len(), 1);
                        let child = &body["children"][0];
                        let kind = child["type"].as_str().unwrap();
                        assert!(matches!(kind, "image" | "file"));
                        let upload_id = child[kind]["file_upload"]["id"].as_str().unwrap();
                        let upload = files
                            .uploads
                            .lock()
                            .unwrap()
                            .get(upload_id)
                            .unwrap()
                            .clone();
                        assert_eq!(upload.sent, upload.total);
                        let ordinal = files.attached.fetch_add(1, Ordering::SeqCst) + 1;
                        if files.pause_attach.swap(false, Ordering::SeqCst) {
                            files.entered.notify_one();
                            let _hold = files.release.acquire().await.unwrap();
                        }
                        let id = format!("ffffffff-ffff-ffff-ffff-{ordinal:012x}");
                        let mut value = child.clone();
                        value["id"] = json!(id);
                        value["object"] = json!("block");
                        value["in_trash"] = json!(false);
                        value["parent"] = json!({"type":"page_id","page_id":page});
                        value[kind]["name"] = json!(upload.name);
                        for item in value[kind]["caption"].as_array_mut().unwrap() {
                            item["plain_text"] = item["text"]["content"].clone();
                        }
                        files.blocks.lock().unwrap().insert(id, value.clone());
                        if let Some((text, revision)) = document.lock().unwrap().as_mut() {
                            text.push_str(&format!(
                                "\n[{}](https://files.notion.so/{})",
                                upload.name, upload_id
                            ));
                            *revision += 1;
                        }
                        axum::Json(json!({"object":"list","results":[value]}))
                    }
                },
            ),
        )
        .route(
            "/v1/blocks/{id}",
            axum::routing::get(
                move |axum::extract::Path(id): axum::extract::Path<String>, headers: HeaderMap| {
                    let files = readback.clone();
                    async move {
                        allowed(&headers);
                        files.read_back.fetch_add(1, Ordering::SeqCst);
                        let blocks = files.blocks.lock().unwrap();
                        axum::Json(blocks.get(&id).unwrap().clone())
                    }
                },
            ),
        )
        .layer(axum::extract::DefaultBodyLimit::max(110 * 1024 * 1024))
}
