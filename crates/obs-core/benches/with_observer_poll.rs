//! `bench_with_observer_poll` — measures the per-poll cost of the
//! `Future::with_observer` adapter (per-task observer override).
//! Spec 71 § 4 / spec 13 § 3.

#![allow(missing_docs, clippy::expect_used)]

use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use criterion::{Criterion, criterion_group, criterion_main};
use obs_core::{Observer, WithObserver as _, observer::InMemoryObserver};

/// Polls a no-op future once. Used to measure the per-poll cost of
/// the override push/pop without amortising it across many polls.
struct OnePoll;

impl Future for OnePoll {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        std::hint::black_box(());
        Poll::Ready(())
    }
}

fn bench_with_observer_poll(c: &mut Criterion) {
    let override_observer: Arc<dyn Observer> = Arc::new(InMemoryObserver::new());
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    c.bench_function("with_observer_poll_once", |b| {
        b.iter(|| {
            let fut = OnePoll.with_observer(override_observer.clone());
            let pinned = std::pin::pin!(fut);
            let _ = pinned.poll(&mut cx);
        });
    });
}

criterion_group!(benches, bench_with_observer_poll);
criterion_main!(benches);
