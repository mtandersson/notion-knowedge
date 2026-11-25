use std::{future::Future, pin::Pin, time::Duration};

use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind},
    lifecycle::LifecycleScope,
};
use notion_knowledge_mcp::view::{Request, SavedViewQuery};
use notion_knowledge_notion::NotionClient;
use serde_json::Value;

pub(crate) struct Runtime {
    pub source: NotionClient,
    pub scope: LifecycleScope,
}

impl SavedViewQuery for Runtime {
    fn query(
        &self,
        input: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Value, BackendError>> + Send + '_>> {
        Box::pin(async move {
            let continuation = input.continuation.as_ref().map(|next| {
                (
                    next.query_id.as_str(),
                    next.cursor.as_str(),
                    next.view_revision.as_str(),
                )
            });
            tokio::time::timeout(
                Duration::from_secs(60),
                self.source.query_saved_view(
                    &self.scope,
                    &input.view_id,
                    input.page_size.unwrap_or(10),
                    continuation,
                ),
            )
            .await
            .map_err(|_| BackendError {
                kind: BackendErrorKind::Unavailable,
                operation: "notion.view_query",
                retry_after: None,
                committed_page_id: None,
            })?
        })
    }
}
