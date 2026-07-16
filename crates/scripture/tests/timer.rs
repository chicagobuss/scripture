//! Deterministic [`ManualTimer`] tests — no wall clock in the core path.
//! [`SystemTimer`] isolation is covered separately with short wall sleeps.

use std::sync::Arc;
use std::task::Context;
use std::time::Duration;

use futures::executor::{LocalPool, block_on};
use futures::task::{SpawnExt, noop_waker};
use scripture::{ManualClock, ManualTimer, SystemTimer, Timer};

#[test]
fn manual_timer_sleep_stays_pending_until_deadline() {
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let mut pool = LocalPool::new();
    let spawner = pool.spawner();

    let (sender, mut receiver) = futures::channel::oneshot::channel();
    let sleeper = timer.clone();
    spawner
        .spawn(async move {
            sleeper.sleep_until(Duration::from_millis(10)).await;
            let _ = sender.send(());
        })
        .expect("spawn sleeper");

    pool.run_until_stalled();
    assert!(receiver.try_recv().expect("open").is_none());

    timer.advance(Duration::from_millis(9));
    pool.run_until_stalled();
    assert!(receiver.try_recv().expect("open").is_none());

    timer.advance(Duration::from_millis(1));
    pool.run_until_stalled();
    assert!(receiver.try_recv().expect("open").is_some());
}

#[test]
fn manual_timer_advance_wakes_sleepers_in_deadline_order() {
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    let order = Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut pool = LocalPool::new();
    let spawner = pool.spawner();
    for (index, millis) in [(2_u64, 30), (0, 10), (1, 20)] {
        let timer = timer.clone();
        let order = Arc::clone(&order);
        spawner
            .spawn(async move {
                timer.sleep_until(Duration::from_millis(millis)).await;
                order.lock().expect("order").push(index);
            })
            .expect("spawn");
    }

    pool.run_until_stalled();
    assert!(order.lock().expect("order").is_empty());

    timer.advance(Duration::from_millis(10));
    pool.run_until_stalled();
    assert_eq!(&*order.lock().expect("order"), &[0]);

    timer.advance(Duration::from_millis(10));
    pool.run_until_stalled();
    assert_eq!(&*order.lock().expect("order"), &[0, 1]);

    timer.advance(Duration::from_millis(10));
    pool.run_until_stalled();
    assert_eq!(&*order.lock().expect("order"), &[0, 1, 2]);
}

#[test]
fn manual_timer_already_due_completes_without_wall_clock() {
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    timer.advance(Duration::from_secs(1));
    block_on(timer.sleep_until(Duration::from_millis(500)));
}

#[test]
fn dropped_sleep_until_futures_unregister_sleepers() {
    let clock = Arc::new(ManualClock::new());
    let timer = ManualTimer::new(Arc::clone(&clock));
    assert_eq!(timer.sleeper_count(), 0);

    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    for _ in 0..64 {
        let mut sleep = timer.sleep_until(Duration::from_secs(60));
        // Drive past the registration await point, then drop before completion.
        let _ = sleep.as_mut().poll(&mut context);
        assert_eq!(
            timer.sleeper_count(),
            1,
            "sleeper should be registered after first poll"
        );
        drop(sleep);
        assert_eq!(
            timer.sleeper_count(),
            0,
            "ManualSleepUntil Drop must unregister"
        );
    }
    assert_eq!(timer.sleeper_count(), 0);
}

#[test]
fn system_timer_drop_does_not_cancel_sibling_sleep() {
    let timer = SystemTimer::new();
    let waker = noop_waker();
    let mut context = Context::from_waker(&waker);
    // Spawn the long-deadline sleeper thread, then cancel only that sleeper.
    let mut cancelled = timer.sleep_until(Duration::from_secs(60));
    let _ = cancelled.as_mut().poll(&mut context);
    let sibling = timer.sleep_until(Duration::from_millis(40));
    drop(cancelled);
    block_on(sibling);
}
