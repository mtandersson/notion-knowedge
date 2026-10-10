//! Behavioral contract for the fresh revision preflight (#301).
use std::collections::BTreeMap;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

use notion_knowledge_core::backend::{
    BackendError, BackendErrorKind, BackendFuture, NotionRead, Page, PageContent, PageId,
};
use notion_knowledge_core::revision::{
    ExpectedRevision, RevisionError, RevisionMetadata, check_revision, markdown_sha256,
};

const ID: &str = "12345678-1234-1234-1234-123456789abc";
const T0: &str = "2026-10-03T12:30:00.000Z";
const T1: &str = "2026-10-03T12:31:00Z";

struct Reader {
    current: Mutex<PageContent>,
    metadata_reads: AtomicUsize,
    content_reads: AtomicUsize,
    failure: Option<BackendErrorKind>,
}

fn fixture() -> Reader {
    Reader {
        current: Mutex::new(PageContent {
            page: Page {
                id: PageId(ID.into()),
                url: format!("https://www.notion.so/{ID}"),
                title: "Private title".into(),
                last_edited_time: T0.into(),
                archived: false,
                properties: BTreeMap::new(),
            },
            markdown: "Private original content".into(),
        }),
        metadata_reads: AtomicUsize::new(0),
        content_reads: AtomicUsize::new(0),
        failure: None,
    }
}

fn upstream_error(kind: BackendErrorKind) -> BackendError {
    BackendError {
        operation: "mock.fetch",
        kind,
        retry_after: None,
        committed_page_id: None,
    }
}

impl NotionRead for Reader {
    fn fetch_page<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, Page> {
        Box::pin(async move {
            self.metadata_reads.fetch_add(1, Ordering::Relaxed);
            if let Some(kind) = self.failure {
                return Err(upstream_error(kind));
            }
            Ok(self.current.lock().unwrap().page.clone())
        })
    }

    fn read_content<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, PageContent> {
        Box::pin(async move {
            self.content_reads.fetch_add(1, Ordering::Relaxed);
            if let Some(kind) = self.failure {
                return Err(upstream_error(kind));
            }
            Ok(self.current.lock().unwrap().clone())
        })
    }
}

fn target() -> PageId {
    PageId(ID.into())
}

fn time_only(time: &str) -> ExpectedRevision {
    ExpectedRevision {
        last_edited_time: Some(time.into()),
        markdown_sha256: None,
    }
}

#[tokio::test]
async fn equal_timestamp_instants_pass_without_fetching_content() {
    let reader = fixture();
    let current = check_revision(&reader, &target(), &time_only("2026-10-03T14:30:00+02:00"))
        .await
        .unwrap();
    assert_eq!(
        current,
        RevisionMetadata {
            page_id: target(),
            last_edited_time: T0.into()
        }
    );
    assert_eq!(reader.metadata_reads.load(Ordering::Relaxed), 1);
    assert_eq!(reader.content_reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn fresh_markdown_hash_checks_exact_source_and_requires_every_condition() {
    let reader = fixture();
    let exact = markdown_sha256("Private original content");
    let expected = ExpectedRevision {
        last_edited_time: Some(T0.into()),
        markdown_sha256: Some(exact.to_uppercase()),
    };
    assert!(check_revision(&reader, &target(), &expected).await.is_ok());
    assert_eq!(reader.content_reads.load(Ordering::Relaxed), 1);
    assert_eq!(reader.metadata_reads.load(Ordering::Relaxed), 0);

    let wrong_timestamp = ExpectedRevision {
        last_edited_time: Some(T1.into()),
        markdown_sha256: Some(exact),
    };
    assert!(matches!(
        check_revision(&reader, &target(), &wrong_timestamp).await,
        Err(RevisionError::Conflict(_))
    ));

    let wrong_hash = ExpectedRevision {
        last_edited_time: Some(T0.into()),
        markdown_sha256: Some(markdown_sha256("Different private content")),
    };
    assert!(matches!(
        check_revision(&reader, &target(), &wrong_hash).await,
        Err(RevisionError::Conflict(_))
    ));
}

#[tokio::test]
async fn concurrent_edit_blocks_stale_write_and_explicit_refresh_allows_retry() {
    let reader = fixture();
    let stale = time_only(T0);

    // Another writer modifies the page after the agent's earlier read.
    {
        let mut source = reader.current.lock().unwrap();
        source.page.last_edited_time = T1.into();
        source.markdown = "An unrelated human edit".into();
    }

    let error = check_revision(&reader, &target(), &stale)
        .await
        .unwrap_err();
    let RevisionError::Conflict(metadata) = error else {
        panic!("a stale revision must produce a conflict");
    };
    assert_eq!(metadata.page_id, target());
    assert_eq!(metadata.last_edited_time, T1);
    assert!(!format!("{metadata:?}").contains("An unrelated human edit"));

    // Caller must re-fetch and consciously construct a new precondition.
    let reread = reader.fetch_page(&target()).await.unwrap();
    assert!(
        check_revision(&reader, &target(), &time_only(&reread.last_edited_time))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn same_timestamp_different_content_is_caught_by_sha256() {
    let reader = fixture();
    let expected = ExpectedRevision {
        last_edited_time: None,
        markdown_sha256: Some(markdown_sha256("Private original content")),
    };
    reader.current.lock().unwrap().markdown = "New content".into();
    assert!(matches!(
        check_revision(&reader, &target(), &expected).await,
        Err(RevisionError::Conflict(_))
    ));
}

#[tokio::test]
async fn absent_or_malformed_preconditions_fail_before_any_upstream_read() {
    let reader = fixture();
    for expected in [
        ExpectedRevision::default(),
        time_only("not a timestamp"),
        time_only(""),
        ExpectedRevision {
            last_edited_time: None,
            markdown_sha256: Some("abc".into()),
        },
        ExpectedRevision {
            last_edited_time: Some(T0.into()),
            markdown_sha256: Some("x".repeat(64)),
        },
    ] {
        assert_eq!(
            check_revision(&reader, &target(), &expected).await,
            Err(RevisionError::InvalidPrecondition)
        );
    }
    assert_eq!(reader.metadata_reads.load(Ordering::Relaxed), 0);
    assert_eq!(reader.content_reads.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn archived_or_inconsistent_source_fails_closed_without_content_in_errors() {
    let reader = fixture();
    reader.current.lock().unwrap().page.archived = true;
    assert!(matches!(
        check_revision(&reader, &target(), &time_only(T0)).await,
        Err(RevisionError::Inactive(_))
    ));

    let reader = fixture();
    reader.current.lock().unwrap().page.id = PageId("different".into());
    assert!(matches!(
        check_revision(&reader, &target(), &time_only(T0)).await,
        Err(RevisionError::Read(BackendError {
            kind: BackendErrorKind::Internal,
            ..
        }))
    ));

    let reader = fixture();
    reader.current.lock().unwrap().page.last_edited_time = "malformed".into();
    assert!(matches!(
        check_revision(&reader, &target(), &time_only(T0)).await,
        Err(RevisionError::Read(BackendError {
            kind: BackendErrorKind::Internal,
            ..
        }))
    ));

    let mut reader = fixture();
    reader.failure = Some(BackendErrorKind::Unavailable);
    assert!(matches!(
        check_revision(&reader, &target(), &time_only(T0)).await,
        Err(RevisionError::Read(BackendError {
            kind: BackendErrorKind::Unavailable,
            ..
        }))
    ));
}
