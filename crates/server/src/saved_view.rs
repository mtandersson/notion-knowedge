use std::{future::Future, pin::Pin, time::Duration};

use notion_knowledge_core::{
    backend::{BackendError, BackendErrorKind as Kind},
    lifecycle::LifecycleScope,
    logging::{self, Outcome},
};
use notion_knowledge_mcp::view::{Continuation, Request, SavedViewQuery};
use notion_knowledge_notion::{
    NotionClient,
    views::{ViewProjection, normalize_view_id},
};
use ring::hmac;
use serde_json::{Value, json};

use crate::config::SecretToken;

pub(crate) struct Runtime {
    source: NotionClient,
    scope: LifecycleScope,
    continuation_key: hmac::Key,
}

/// Name the failing stage in the operator log; the caller still gets the
/// generic classified message.
fn log_failure(started: std::time::Instant, failure: &BackendError) {
    let outcome = match failure.kind {
        Kind::PermissionDenied | Kind::NotFound | Kind::Unauthenticated | Kind::InvalidInput => {
            Outcome::Rejected
        }
        _ => Outcome::Failed,
    };
    logging::emit_view_query_failure(
        outcome,
        started.elapsed(),
        &format!("{:?}", failure.kind),
        failure.operation,
    );
}

fn error(kind: Kind) -> BackendError {
    BackendError {
        kind,
        operation: "notion.view_query",
        retry_after: None,
        committed_page_id: None,
    }
}

impl Runtime {
    pub(crate) fn new(source: NotionClient, scope: LifecycleScope, secret: &SecretToken) -> Self {
        // Separate this use from Notion authentication and all webhook signatures.
        let credential = hmac::Key::new(hmac::HMAC_SHA256, secret.expose_secret().as_bytes());
        let domain_key = hmac::sign(&credential, b"notion-knowledge/saved-view/continuation/v1");
        Self {
            source,
            scope,
            continuation_key: hmac::Key::new(hmac::HMAC_SHA256, domain_key.as_ref()),
        }
    }

    fn continuation_payload(&self, view: &str, next: &Continuation) -> String {
        json!([
            self.scope.workspace_id,
            self.scope.generation,
            self.scope
                .roots
                .iter()
                .map(|root| root.0.as_str())
                .collect::<Vec<_>>(),
            view,
            next.query_id,
            next.cursor,
            next.view_revision
        ])
        .to_string()
    }

    fn verify(&self, view: &str, next: &Continuation) -> Result<(), BackendError> {
        if next.signature.len() != 64
            || !next.signature.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(error(Kind::InvalidInput));
        }
        let mut tag = [0u8; 32];
        for (byte, pair) in tag
            .iter_mut()
            .zip(next.signature.as_bytes().as_chunks::<2>().0)
        {
            *byte = std::str::from_utf8(pair)
                .ok()
                .and_then(|s| u8::from_str_radix(s, 16).ok())
                .ok_or_else(|| error(Kind::InvalidInput))?;
        }
        hmac::verify(
            &self.continuation_key,
            self.continuation_payload(view, next).as_bytes(),
            &tag,
        )
        .map_err(|_| error(Kind::InvalidInput))
    }

    fn sign(&self, view: &str, next: &mut Value) -> Result<(), BackendError> {
        if !next.is_null() {
            let mut fields = next.take();
            fields["signature"] = json!("");
            let continuation: Continuation =
                serde_json::from_value(fields).map_err(|_| error(Kind::Internal))?;
            let payload = self.continuation_payload(view, &continuation);
            let tag = hmac::sign(&self.continuation_key, payload.as_bytes());
            let signature: String = tag
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            *next = json!({
                "query_id":continuation.query_id, "cursor":continuation.cursor,
                "view_revision":continuation.view_revision, "signature":signature
            });
        }
        Ok(())
    }
}

impl SavedViewQuery for Runtime {
    fn query(
        &self,
        input: Request,
    ) -> Pin<Box<dyn Future<Output = Result<Value, BackendError>> + Send + '_>> {
        Box::pin(async move {
            let view =
                normalize_view_id(&input.view_id).ok_or_else(|| error(Kind::InvalidInput))?;
            let continuation = if let Some(next) = &input.continuation {
                self.verify(&view, next)?;
                Some((
                    next.query_id.as_str(),
                    next.cursor.as_str(),
                    next.view_revision.as_str(),
                ))
            } else {
                None
            };
            let started = std::time::Instant::now();
            let mut output = match tokio::time::timeout(
                Duration::from_secs(60),
                self.source.query_saved_view(
                    &self.scope,
                    &view,
                    input.page_size.unwrap_or(10),
                    continuation,
                    ViewProjection {
                        fields: input.fields.as_deref(),
                        row_revision: input.include_row_revision.unwrap_or(false),
                    },
                ),
            )
            .await
            {
                Ok(Ok(output)) => output,
                Ok(Err(failure)) => {
                    log_failure(started, &failure);
                    return Err(failure);
                }
                Err(_) => {
                    let failure = error(Kind::Unavailable);
                    log_failure(started, &failure);
                    return Err(failure);
                }
            };
            self.sign(&view, &mut output["continuation"])?;
            Ok(output)
        })
    }
}
