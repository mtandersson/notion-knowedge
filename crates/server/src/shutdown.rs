//! Process signals and a single bounded HTTP/worker/owned-effect shutdown.
use std::{future::Future, io, time::Duration};
use tokio_util::sync::CancellationToken;

/// SIGTERM is the ordinary container/orchestrator stop signal; SIGINT also
/// remains supported for terminals and the existing image contract.
pub async fn signal() -> io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

/// `work` includes transport shutdown AND all worker/coordinator drains. Stop
/// admission first, then give their combined cleanup one shared deadline.
pub async fn supervise<T>(
    stop: CancellationToken,
    timeout: Duration,
    work: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    tokio::pin!(work);
    let requested = async {
        tokio::select! {
            result = signal() => result,
            _ = stop.cancelled() => Ok(()),
        }
    };
    tokio::select! {
        result = &mut work => result,
        request = requested => {
            stop.cancel();
            let result = tokio::time::timeout(timeout, work).await.map_err(|_| {
                io::Error::new(io::ErrorKind::TimedOut, "shutdown deadline exceeded; pending work requires recovery")
            })?;
            request?;
            result
        }
    }
}

/// Operator commands retain structured output even when cleanup exceeds the
/// process deadline. A timeout is incomplete recovery work, never success.
pub async fn operator(
    operation: &'static str,
    stop: CancellationToken,
    timeout: Duration,
    work: impl Future<Output = crate::reconciliation_command::Output>,
) -> crate::reconciliation_command::Output {
    match supervise(stop, timeout, async { Ok(work.await) }).await {
        Ok(output) => output,
        Err(error) => crate::reconciliation_command::Output {
            json: serde_json::json!({"operation":operation,"status":if error.kind()==io::ErrorKind::TimedOut {"shutdown_timeout"} else {"shutdown_failed"}}),
            exit: 1,
        },
    }
}

/// A default Tokio runtime drop waits indefinitely for blocking tasks. An
/// expired HTTP deadline must not become another unbounded wait on a native
/// drain/embedding task. Successful serving already awaited its owned drains;
/// after failure the process exits nonzero and durable work is recovered.
pub fn run<T>(work: impl Future<Output = T>) -> io::Result<T> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(work);
    runtime.shutdown_timeout(Duration::ZERO);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn operator_timeout_is_structured_unsuccessful_and_clean_output_is_preserved() {
        for operation in ["index", "reconciliation"] {
            let stop = CancellationToken::new();
            stop.cancel();
            let output = operator(
                operation,
                stop,
                Duration::from_millis(10),
                std::future::pending(),
            )
            .await;
            assert_eq!(output.exit, 1);
            assert_eq!(
                output.json,
                serde_json::json!({"operation":operation,"status":"shutdown_timeout"})
            );
            let expected =
                serde_json::json!({"operation":operation,"status":"complete","fixture_count":3});
            let output = operator(
                operation,
                CancellationToken::new(),
                Duration::from_secs(1),
                async {
                    crate::reconciliation_command::Output {
                        json: expected.clone(),
                        exit: 0,
                    }
                },
            )
            .await;
            assert_eq!(output.exit, 0);
            assert_eq!(output.json, expected);
        }
    }

    #[tokio::test]
    async fn shutdown_waits_for_cleanup_but_reports_an_expired_shared_deadline() {
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let clean = supervise(stop.clone(), Duration::from_secs(1), async move {
            stopped.cancelled().await;
            Ok(())
        });
        stop.cancel();
        clean.await.unwrap();

        let stop = CancellationToken::new();
        stop.cancel();
        let error = supervise(
            stop,
            Duration::from_millis(10),
            std::future::pending::<io::Result<()>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }
}
