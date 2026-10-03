use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use notion_knowledge_core::backend::*;

struct MockBackend {
    content: Mutex<PageContent>,
}

impl NotionRead for MockBackend {
    fn fetch_page<'a>(&'a self, page_id: &'a PageId) -> BackendFuture<'a, Page> {
        Box::pin(async move {
            let content = self.content.lock().unwrap();
            if &content.page.id != page_id {
                return Err(BackendError {
                    kind: BackendErrorKind::NotFound,
                    operation: "fetch_page",
                    retry_after: None,
                });
            }
            Ok(content.page.clone())
        })
    }

    fn read_content<'a>(&'a self, _: &'a PageId) -> BackendFuture<'a, PageContent> {
        Box::pin(async move { Ok(self.content.lock().unwrap().clone()) })
    }
}

impl NotionWrite for MockBackend {
    fn replace_content(&self, request: ReplacePageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            let mut content = self.content.lock().unwrap();
            assert_eq!(content.page.id, request.page_id);
            content.markdown = request.markdown;
            Ok(content.page.clone())
        })
    }

    fn append_content(&self, request: AppendPageContent) -> BackendFuture<'_, Page> {
        Box::pin(async move {
            let mut content = self.content.lock().unwrap();
            assert_eq!(content.page.id, request.page_id);
            content.markdown.push_str(&request.markdown);
            Ok(content.page.clone())
        })
    }

    fn create_page(&self, _: CreatePage) -> BackendFuture<'_, Page> {
        Box::pin(async {
            Err(BackendError {
                kind: BackendErrorKind::RateLimited,
                operation: "create_page",
                retry_after: Some(Duration::from_secs(3)),
            })
        })
    }
}

// These mocks finish immediately. A pending future is a test failure, not a
// homemade executor for network I/O.
fn ready<T>(mut future: BackendFuture<'_, T>) -> Result<T, BackendError> {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("mock must be immediately ready"),
    }
}

fn mock() -> MockBackend {
    MockBackend {
        content: Mutex::new(PageContent {
            page: Page {
                id: PageId("page-1".into()),
                url: "https://notion.so/page-1".into(),
                title: "Title".into(),
                last_edited_time: "2026-10-03T00:00:00Z".into(),
                archived: false,
                properties: BTreeMap::new(),
            },
            markdown: "original".into(),
        }),
    }
}

#[test]
fn application_can_replace_and_append_through_an_object_safe_backend() {
    let backend: Box<dyn NotionBackend> = Box::new(mock());
    let page_id = PageId("page-1".into());
    let original = ready(backend.fetch_page(&page_id)).unwrap();
    let replaced = ready(backend.replace_content(ReplacePageContent {
        page_id: page_id.clone(),
        markdown: "replacement".into(),
    }))
    .unwrap();
    assert_eq!(replaced.id, original.id);
    ready(backend.append_content(AppendPageContent {
        page_id: page_id.clone(),
        markdown: " plus append".into(),
    }))
    .unwrap();
    assert_eq!(
        ready(backend.read_content(&page_id)).unwrap().markdown,
        "replacement plus append"
    );
}

#[test]
fn read_only_consumers_receive_normalized_missing_page_errors() {
    let backend: Box<dyn NotionRead> = Box::new(mock());
    let error = ready(backend.fetch_page(&PageId("missing".into()))).unwrap_err();
    assert_eq!(error.kind, BackendErrorKind::NotFound);
    assert_eq!(error.operation, "fetch_page");
    assert_eq!(error.retry_after, None);
}

#[test]
fn write_errors_preserve_retry_advice_without_upstream_transport_types() {
    let backend: Box<dyn NotionWrite> = Box::new(mock());
    let error = ready(backend.create_page(CreatePage {
        parent_page_id: PageId("root".into()),
        title: "New page".into(),
        markdown: "Content".into(),
    }))
    .unwrap_err();
    assert_eq!(error.kind, BackendErrorKind::RateLimited);
    assert_eq!(error.retry_after, Some(Duration::from_secs(3)));
    assert_eq!(error.to_string(), "create_page failed: RateLimited");
}

