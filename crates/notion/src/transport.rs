//! Shared request admission and conservative replay policy. No payload diagnostics.
use crate::NotionClient;
use notion_knowledge_core::backend::{BackendError, BackendErrorKind};
use notion_knowledge_core::logging::{self, Operation, Outcome};
use reqwest::{RequestBuilder, Response};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant as StdInstant},
};
use tokio::{sync::Mutex, time::Instant};

const INTERVAL: Duration = Duration::from_millis(334);
const MAX_WAIT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u64 = 3;

/// Cumulative, payload-free counters shared by clones of an integration client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestMetrics {
    pub attempts: u64,
    pub retries: u64,
    pub rate_limits: u64,
}

pub(crate) struct Transport {
    next: Mutex<Instant>,
    overflow_delay: AtomicU64,
    attempts: AtomicU64,
    retries: AtomicU64,
    rate_limits: AtomicU64,
    jitter: AtomicU64,
    max_retries: u64,
}
impl Default for Transport {
    fn default() -> Self {
        // A per-client seed spreads independent integration retries; never derived from secrets.
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(1)
            | 1;
        Self {
            next: Mutex::new(Instant::now()),
            overflow_delay: AtomicU64::new(0),
            attempts: AtomicU64::new(0),
            retries: AtomicU64::new(0),
            rate_limits: AtomicU64::new(0),
            jitter: AtomicU64::new(seed),
            max_retries: MAX_RETRIES,
        }
    }
}
fn error(
    operation: &'static str,
    kind: BackendErrorKind,
    retry_after: Option<Duration>,
) -> BackendError {
    BackendError {
        operation,
        kind,
        retry_after,
        committed_page_id: None,
    }
}
impl Transport {
    async fn admit(&self, operation: &'static str) -> Result<(), BackendError> {
        // Never hold the mutex during sleep: a 429 must extend the clock even
        // while existing waiters are queued. Re-check after waking; no reserved
        // slot means cancellation cannot leave holes or cause a later burst.
        loop {
            let mut next = self.next.lock().await;
            let overflow = self.overflow_delay.load(Ordering::Relaxed);
            if overflow > 0 {
                return Err(error(
                    operation,
                    BackendErrorKind::RateLimited,
                    Some(Duration::from_secs(overflow)),
                ));
            }
            let now = Instant::now();
            if *next <= now {
                *next = now + INTERVAL;
                return Ok(());
            }
            let wait = next.saturating_duration_since(now);
            if wait > MAX_WAIT {
                return Err(error(operation, BackendErrorKind::RateLimited, Some(wait)));
            }
            let until = *next;
            drop(next);
            tokio::time::sleep_until(until).await;
        }
    }

    async fn defer(&self, delay: Duration) {
        let mut next = self.next.lock().await;
        if let Some(until) = Instant::now().checked_add(delay) {
            *next = (*next).max(until);
        } else {
            // An unrepresentable upstream deadline must never turn into an early retry.
            self.overflow_delay
                .fetch_max(delay.as_secs(), Ordering::Relaxed);
        }
    }
    fn backoff(&self, retry: u64) -> Duration {
        let base = 250_u64 << retry.min(3);
        let mut old = self.jitter.fetch_add(0x9e3779b97f4a7c15, Ordering::Relaxed);
        old = (old ^ (old >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        old = (old ^ (old >> 27)).wrapping_mul(0x94d049bb133111eb);
        old ^= old >> 31;
        // Equal jitter: [half the exponential bound, bound], always nonzero.
        Duration::from_millis(base / 2 + old % (base / 2 + 1))
    }
}
impl NotionClient {
    pub fn request_metrics(&self) -> RequestMetrics {
        RequestMetrics {
            attempts: self.transport.attempts.load(Ordering::Relaxed),
            retries: self.transport.retries.load(Ordering::Relaxed),
            rate_limits: self.transport.rate_limits.load(Ordering::Relaxed),
        }
    }
    #[cfg(any(test, feature = "test-fixtures"))]
    pub(crate) fn without_retries(mut self) -> Self {
        self.transport = std::sync::Arc::new(Transport {
            max_retries: 0,
            ..Default::default()
        });
        self
    }
    pub(crate) async fn send(
        &self,
        request: RequestBuilder,
        replay_safe: bool,
        operation: &'static str,
    ) -> Result<Response, BackendError> {
        let id = logging::current_id();
        let started = StdInstant::now();
        let result = self.send_inner(request, replay_safe, operation).await;
        let (outcome, status) = match &result {
            Ok(response) if response.status().is_success() => {
                (Outcome::Success, Some(response.status().as_u16()))
            }
            Ok(response) if response.status().is_client_error() => {
                (Outcome::Rejected, Some(response.status().as_u16()))
            }
            Ok(response) => (Outcome::Failed, Some(response.status().as_u16())),
            Err(_) => (Outcome::Failed, None),
        };
        logging::emit(
            &id,
            Operation::NotionApi,
            outcome,
            started.elapsed(),
            status,
            outcome.level(),
        );
        result
    }

    async fn send_inner(
        &self,
        request: RequestBuilder,
        replay_safe: bool,
        operation: &'static str,
    ) -> Result<Response, BackendError> {
        let mut retry = 0;
        loop {
            tokio::time::timeout(MAX_WAIT, self.transport.admit(operation))
                .await
                .map_err(|_| error(operation, BackendErrorKind::RateLimited, Some(MAX_WAIT)))??;
            let attempt = request
                .try_clone()
                .ok_or_else(|| error(operation, BackendErrorKind::Internal, None))?;
            self.transport.attempts.fetch_add(1, Ordering::Relaxed);
            if retry > 0 {
                self.transport.retries.fetch_add(1, Ordering::Relaxed);
            }
            let mut result = attempt.send().await;
            let rate_delay = result
                .as_ref()
                .ok()
                .filter(|r| matches!(r.status().as_u16(), 429 | 529))
                .map(|r| {
                    r.headers()
                        .get("Retry-After")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .map(Duration::from_secs)
                });
            if let Some(delay) = rate_delay {
                self.transport.rate_limits.fetch_add(1, Ordering::Relaxed);
                let effective_delay = delay.unwrap_or_default().max(self.transport.backoff(retry));
                self.transport.defer(effective_delay).await;
                // The documented permanent workspace request block is not retryable.
                // Bound the discarded error body; neither its content nor reason is logged.
                let response = result.as_mut().expect("rate response");
                let mut bytes = Vec::new();
                loop {
                    match response.chunk().await {
                        Ok(Some(chunk)) if bytes.len() + chunk.len() <= 16 * 1024 => {
                            bytes.extend_from_slice(&chunk)
                        }
                        Ok(None) => break,
                        _ => return Err(error(operation, BackendErrorKind::RateLimited, delay)),
                    }
                }
                if serde_json::from_slice::<serde_json::Value>(&bytes)
                    .ok()
                    .is_some_and(|v| {
                        v["additional_data"]["rate_limit_reason"] == "public_api_request_blocked"
                    })
                {
                    return Err(error(operation, BackendErrorKind::RateLimited, None));
                }
                let delay = effective_delay;
                if retry >= self.transport.max_retries || delay > MAX_WAIT {
                    return Err(error(operation, BackendErrorKind::RateLimited, Some(delay)));
                }
                // Notion documents 429 as rejected by request admission; unlike ambiguous
                // transport/5xx failures, this response is safe to resubmit for writes too.
            } else {
                let transient = replay_safe
                    && match &result {
                        Err(_) => true,
                        Ok(response) => matches!(response.status().as_u16(), 500 | 502 | 503 | 504),
                    };
                if !transient || retry >= self.transport.max_retries {
                    return result
                        .map_err(|_| error(operation, BackendErrorKind::Unavailable, None));
                }
                tokio::time::sleep(self.transport.backoff(retry)).await;
            }
            retry += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    async fn mock(
        statuses: Vec<(u16, &'static str)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<Instant>>) {
        mock_steps(
            statuses
                .into_iter()
                .map(|(status, headers)| (status, headers, "{}".to_owned()))
                .collect(),
        )
        .await
    }
    async fn mock_steps(
        statuses: Vec<(u16, &'static str, String)>,
    ) -> (NotionClient, tokio::task::JoinHandle<Vec<Instant>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = NotionClient::integration("secret-fixture").unwrap();
        client.api_root = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut times = Vec::new();
            for (status, headers, body) in statuses {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut bytes = [0; 1024];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&bytes[..n]);
                    if request.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                times.push(Instant::now());
                socket.write_all(format!("HTTP/1.1 {status} Result\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            }
            times
        });
        (client, task)
    }
    async fn send(client: &NotionClient, safe: bool) -> Result<Response, BackendError> {
        client
            .send(client.http.get(&client.api_root), safe, "notion.test")
            .await
    }
    #[tokio::test]
    async fn rejected_requests_honor_retry_after_and_expose_only_counters() {
        let (client, task) = mock(vec![(429, "Retry-After: 1\r\n"), (200, "")]).await;
        assert_eq!(send(&client, false).await.unwrap().status(), 200);
        let times = task.await.unwrap();
        assert!(times[1].duration_since(times[0]) >= Duration::from_secs(1));
        assert_eq!(
            client.request_metrics(),
            RequestMetrics {
                attempts: 2,
                retries: 1,
                rate_limits: 1
            }
        );
        assert!(!format!("{:?}", client.request_metrics()).contains("secret"));
    }
    #[tokio::test]
    async fn read_transients_are_bounded_but_mutations_and_auth_fail_fast() {
        for (safe, statuses, expected, retries) in [
            (true, vec![(503, ""); 4], 503, 3),
            (false, vec![(503, "")], 503, 0),
            (true, vec![(401, "")], 401, 0),
            (true, vec![(403, "")], 403, 0),
            (true, vec![(400, "")], 400, 0),
            (true, vec![(404, "")], 404, 0),
            (true, vec![(409, "")], 409, 0),
            (true, vec![(501, "")], 501, 0),
        ] {
            let (client, task) = mock(statuses).await;
            assert_eq!(send(&client, safe).await.unwrap().status(), expected);
            task.await.unwrap();
            assert_eq!(client.request_metrics().retries, retries);
        }
    }
    #[tokio::test]
    async fn concurrent_clones_share_admission_and_retry_cooldown() {
        let (client, task) = mock(vec![(200, ""); 3]).await;
        let other = client.clone();
        let third = client.clone();
        let (a, b, c) = tokio::join!(send(&client, true), send(&other, true), send(&third, true));
        assert!(a.is_ok() && b.is_ok() && c.is_ok());
        let times = task.await.unwrap();
        for pair in times.windows(2) {
            assert!(pair[1].duration_since(pair[0]) >= Duration::from_millis(330));
        }
        assert_eq!(other.request_metrics().attempts, 3);
        client.transport.defer(Duration::from_secs(31)).await;
        assert_eq!(
            send(&other, true).await.unwrap_err().kind,
            BackendErrorKind::RateLimited
        );
        assert_eq!(client.request_metrics().attempts, 3);
    }
    #[tokio::test]
    async fn excessive_retry_after_returns_actionable_delay_without_early_replay() {
        let (client, task) = mock(vec![(429, "Retry-After: 120\r\n")]).await;
        let error = send(&client, true).await.unwrap_err();
        assert_eq!(error.retry_after, Some(Duration::from_secs(120)));
        task.await.unwrap();
        assert_eq!(client.request_metrics().retries, 0);
        assert_eq!(
            send(&client.clone(), true).await.unwrap_err().kind,
            BackendErrorKind::RateLimited
        );
    }
    #[tokio::test(start_paused = true)]
    async fn admission_cancellation_does_not_reserve_future_slots() {
        let client = NotionClient::integration("secret-fixture").unwrap();
        client.transport.admit("test").await.unwrap();
        let other = client.clone();
        let waiting = tokio::spawn(async move { other.transport.admit("test").await });
        tokio::task::yield_now().await;
        waiting.abort();
        let start = Instant::now();
        client.transport.admit("test").await.unwrap();
        assert_eq!(Instant::now() - start, INTERVAL);
    }
    #[tokio::test]
    async fn public_read_and_mutation_boundaries_use_shared_retry_policy() {
        use notion_knowledge_core::backend::{AppendPageContent, CreatePage, PageId};
        const ID: &str = "12345678-1234-1234-1234-123456789abc";
        let page = serde_json::json!({"object":"page","id":ID,"url":format!("https://www.notion.so/{ID}"),"archived":false,"last_edited_time":"2026-10-03T12:30:00Z","properties":{"title":{"id":"title","type":"title","title":[{"plain_text":"Title"}]}}}).to_string();
        let markdown = serde_json::json!({"object":"page_markdown","id":ID,"markdown":"# Read","truncated":false,"unknown_block_ids":[]}).to_string();
        let (client, task) = mock_steps(vec![
            (503, "", "{}".into()),
            (200, "", page.clone()),
            (200, "", page.clone()),
            (503, "", "{}".into()),
            (200, "", markdown.clone()),
            (429, "Retry-After: 0\r\n", "{}".into()),
            (200, "", page),
            (529, "Retry-After: 0\r\n", "{}".into()),
            (200, "", markdown),
        ])
        .await;
        assert_eq!(client.fetch_page(ID).await.unwrap().id.0, ID);
        assert_eq!(client.read_content(ID).await.unwrap().markdown, "# Read");
        assert_eq!(
            client
                .create_page(CreatePage {
                    parent_page_id: PageId(ID.into()),
                    title: "Title".into(),
                    markdown: "# New".into()
                })
                .await
                .unwrap()
                .id
                .0,
            ID
        );
        client
            .clone()
            .append_content(AppendPageContent {
                page_id: PageId(ID.into()),
                markdown: "Extra".into(),
            })
            .await
            .unwrap();
        assert_eq!(task.await.unwrap().len(), 9);
        assert_eq!(
            client.request_metrics(),
            RequestMetrics {
                attempts: 9,
                retries: 4,
                rate_limits: 2
            }
        );
    }
    #[tokio::test]
    async fn permanent_workspace_block_and_ambiguous_create_failure_never_replay() {
        use notion_knowledge_core::backend::{CreatePage, PageId};
        let (client, task) = mock_steps(vec![(
            429,
            "Retry-After: 1\r\n",
            r#"{"additional_data":{"rate_limit_reason":"public_api_request_blocked"}}"#.into(),
        )])
        .await;
        assert_eq!(
            send(&client, true).await.unwrap_err().kind,
            BackendErrorKind::RateLimited
        );
        task.await.unwrap();
        assert_eq!(client.request_metrics().retries, 0);
        let (client, task) = mock(vec![(503, "")]).await;
        assert_eq!(
            client
                .create_page(CreatePage {
                    parent_page_id: PageId("12345678123412341234123456789abc".into()),
                    title: "Title".into(),
                    markdown: "Body".into()
                })
                .await
                .unwrap_err()
                .kind,
            BackendErrorKind::Unavailable
        );
        task.await.unwrap();
        assert_eq!(client.request_metrics().attempts, 1);
    }
    #[tokio::test(start_paused = true)]
    async fn rejection_extends_already_waiting_clone_admission() {
        let client = NotionClient::integration("fixture").unwrap();
        client.transport.admit("test").await.unwrap();
        let other = client.clone();
        let task = tokio::spawn(async move {
            other.transport.admit("test").await.unwrap();
            Instant::now()
        });
        tokio::task::yield_now().await;
        let start = Instant::now();
        client.transport.defer(Duration::from_secs(2)).await;
        assert_eq!(task.await.unwrap() - start, Duration::from_secs(2));
    }
    #[tokio::test]
    async fn unrepresentable_upstream_deadline_never_becomes_an_early_retry() {
        let (client, task) = mock(vec![(429, "Retry-After: 18446744073709551615\r\n")]).await;
        let error = send(&client, true).await.unwrap_err();
        assert_eq!(error.retry_after, Some(Duration::from_secs(u64::MAX)));
        task.await.unwrap();
        assert_eq!(
            send(&client.clone(), true).await.unwrap_err().retry_after,
            error.retry_after
        );
        assert_eq!(client.request_metrics().attempts, 1);
    }
    #[test]
    fn exponential_backoff_has_nonzero_bounded_jitter() {
        let transport = Transport::default();
        for retry in 0..4 {
            let bound = 250_u64 << retry;
            let values: Vec<_> = (0..32)
                .map(|_| transport.backoff(retry).as_millis())
                .collect();
            assert!(
                values
                    .iter()
                    .all(|&n| n >= u128::from(bound / 2) && n <= u128::from(bound))
            );
            assert!(values.windows(2).any(|p| p[0] != p[1]));
        }
    }
}
