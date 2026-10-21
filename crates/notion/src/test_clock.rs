//! Controlled virtual time for real loopback HTTP policy tests.
use crate::NotionClient;
use std::{
    collections::BTreeSet,
    future::Future,
    sync::{Arc, mpsc},
};
use tokio::{
    sync::{mpsc as async_mpsc, oneshot},
    time::Instant,
};

pub(crate) enum ClockEvent {
    Sleep(Instant),
    RequestStarted,
    ResponseReady,
}

#[derive(Clone)]
pub(crate) struct ClockHandle {
    sender: async_mpsc::UnboundedSender<ClockEvent>,
}
impl ClockHandle {
    pub(crate) fn track(&self, client: &mut NotionClient) {
        client.transport = Arc::new(crate::transport::Transport::clocked(self.sender.clone()));
    }
}

pub(crate) struct TestClock {
    // An outstanding blocking task inhibits Tokio's automatic time advance
    // while real socket I/O is pending. The channel closes even on test panic.
    _release: mpsc::Sender<()>,
    _guard: tokio::task::JoinHandle<()>,
    waits: async_mpsc::UnboundedReceiver<ClockEvent>,
    sender: async_mpsc::UnboundedSender<ClockEvent>,
}
impl TestClock {
    /// Poll exactly once to establish the waiting state before cancellation or
    /// policy replacement, rather than hoping a spawned task ran after a yield.
    pub(crate) async fn park<F: Future>(future: F) -> std::pin::Pin<Box<F>> {
        let mut future = Box::pin(future);
        std::future::poll_fn(|context| {
            assert!(
                future.as_mut().poll(context).is_pending(),
                "contract requires an admitted waiter"
            );
            std::task::Poll::Ready(())
        })
        .await;
        future
    }
    pub(crate) async fn new() -> Self {
        tokio::time::pause();
        let (release, stopped) = mpsc::channel();
        let (ready, started) = oneshot::channel();
        let guard = tokio::task::spawn_blocking(move || {
            let _ = ready.send(());
            let _ = stopped.recv();
        });
        started.await.unwrap();
        let (sender, waits) = async_mpsc::unbounded_channel();
        Self {
            _release: release,
            _guard: guard,
            waits,
            sender,
        }
    }
    /// Hold a real exchange precisely at its next admission/backoff wait.
    pub(crate) async fn park_until_wait<F: Future>(
        &mut self,
        future: F,
    ) -> (std::pin::Pin<Box<F>>, Instant) {
        let mut future = Box::pin(future);
        loop {
            tokio::select! {
                _ = &mut future => panic!("exchange completed before admission wait"),
                event = self.waits.recv() => match event.expect("clock remains live") {
                    ClockEvent::Sleep(until) => return (future, until),
                    ClockEvent::RequestStarted | ClockEvent::ResponseReady => {},
                }
            }
        }
    }
    pub(crate) fn handle(&self) -> ClockHandle {
        ClockHandle {
            sender: self.sender.clone(),
        }
    }
    pub(crate) async fn advance(duration: std::time::Duration) {
        // Tokio's timer wheel rounds deadlines to millisecond ticks. Cross
        // that virtual tick even when socket I/O completed between ticks.
        tokio::time::advance(duration + std::time::Duration::from_millis(1)).await;
    }
    pub(crate) async fn run<F: Future>(&mut self, future: F) -> F::Output {
        tokio::pin!(future);
        let mut pending = BTreeSet::new();
        let mut in_flight = 0usize;
        loop {
            tokio::select! {
                biased;
                result = &mut future => return result,
                Some(event) = self.waits.recv() => match event {
                    ClockEvent::Sleep(until) => { pending.insert(until); }
                    ClockEvent::RequestStarted => in_flight += 1,
                    ClockEvent::ResponseReady => in_flight = in_flight.checked_sub(1).expect("response follows admitted request"),
                },
                _ = std::future::ready(()), if in_flight == 0 && !pending.is_empty() => {
                    // A real response is a barrier: advancing while its socket
                    // is pending could collapse distinct admission timestamps.
                    let until = pending.pop_first().unwrap();
                    Self::advance(until.saturating_duration_since(Instant::now())).await;
                }
            }
        }
    }
}
