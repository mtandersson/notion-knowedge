#![cfg(all(target_os = "linux", target_env = "gnu"))]
//! #656: memory freed on many runtime threads must be reused, not retained
//! per thread. Its own test binary, because the allocator bound is
//! process-wide and must precede thread creation. Keep this the only test in
//! this binary: another test's allocations would share its arenas.
use notion_knowledge_server::allocator::{DEFAULT_ARENA_MAX, bound_allocator_arenas};
use std::sync::{Arc, Barrier};

const THREADS: usize = 16;
const TRANSIENT: usize = 8 << 20;
const PIECE: usize = 64 << 10;

fn heap_arena_bytes() -> usize {
    // SAFETY: mallinfo2 only reads allocator statistics.
    unsafe { libc::mallinfo2() }.arena
}

/// Fill and drop a page-sized working set, like one embedding's activations.
fn transient_work() -> Box<[u8]> {
    let pieces: Vec<Vec<u8>> = (0..TRANSIENT / PIECE)
        .map(|i| vec![i as u8; PIECE])
        .collect();
    // A small survivor above the working set keeps the freed span below the
    // arena top, as long-lived sync state does.
    let survivor = vec![1u8; 4096].into_boxed_slice();
    std::hint::black_box(&pieces);
    drop(pieces);
    survivor
}

#[test]
fn sequential_page_work_on_many_threads_keeps_retained_heap_near_one_page() {
    if std::env::var_os("MALLOC_ARENA_MAX").is_some()
        && std::env::var_os("NK_ALLOCATOR_GUARD_FORCE").is_none()
    {
        eprintln!("skipped: MALLOC_ARENA_MAX is set by the environment");
        return;
    }
    if std::env::var_os("MALLOC_ARENA_MAX").is_none() {
        assert_eq!(bound_allocator_arenas(), Some(DEFAULT_ARENA_MAX));
    }
    let start = Arc::new(Barrier::new(THREADS));
    let turns: Vec<_> = (0..THREADS)
        .map(|_| std::sync::mpsc::sync_channel::<std::sync::mpsc::SyncSender<()>>(0))
        .collect();
    let mut senders = Vec::new();
    let mut workers = Vec::new();
    for (send, receive) in turns {
        senders.push(send);
        let start = start.clone();
        workers.push(std::thread::spawn(move || {
            // Contend once so an unbounded allocator gives each thread an arena.
            start.wait();
            let warm: Vec<Vec<u8>> = (0..256).map(|_| vec![0u8; 512]).collect();
            drop(warm);
            let mut survivors = Vec::new();
            while let Ok(done) = receive.recv() {
                survivors.push(transient_work());
                done.send(()).unwrap();
            }
            survivors.len()
        }));
    }
    // One page at a time, each on a different thread, as the sync loop does.
    for round in 0..THREADS * 2 {
        let (done, finished) = std::sync::mpsc::sync_channel(0);
        senders[round % THREADS].send(done).unwrap();
        finished.recv().unwrap();
    }
    let retained = heap_arena_bytes();
    eprintln!("retained_heap_mib={}", retained >> 20);
    drop(senders);
    for worker in workers {
        assert_eq!(worker.join().unwrap(), 2);
    }
    assert!(
        retained < 6 * TRANSIENT,
        "heap arenas retain {} MiB after sequential 8 MiB pages",
        retained >> 20
    );
}
