pub use aleph_bft_crypto::{
    Indexed, MultiKeychain, Multisigned, NodeCount, PartialMultisignature, PartiallyMultisigned,
    Signable, Signature, Signed, UncheckedSigned,
};
use async_trait::async_trait;
use core::fmt::Debug;
use futures::future::pending;
use futures_timer::Delay;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    fmt::Formatter,
    ops::{Add, Div, Mul},
    time::{Duration, Instant},
};

/// Abstraction of a task-scheduling logic
///
/// Because the network can be faulty, the task of sending a message must be performed multiple
/// times to ensure that the recipient receives each message.
/// The trait [`TaskScheduler<T>`] describes in what intervals some abstract task of type `T`
/// should be performed.
#[async_trait::async_trait]
pub trait TaskScheduler<T>: Send + Sync {
    fn add_task(&mut self, task: T);
    async fn next_task(&mut self) -> T;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTask<T> {
    task: T,
    delay: Duration,
}

impl<T> ScheduledTask<T> {
    fn new(task: T, delay: Duration) -> Self {
        ScheduledTask { task, delay }
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct IndexedInstant(Instant, usize);

impl IndexedInstant {
    fn at(instant: Instant, i: usize) -> Self {
        IndexedInstant(instant, i)
    }
}

/// A basic task scheduler scheduling tasks with an exponential slowdown
///
/// A scheduler parameterized by a duration `initial_delay`. When a task is added to the scheduler
/// it is first scheduled immediately, then it is scheduled indefinitely, where the first delay is
/// `initial_delay`, and each following delay for that task is two times longer than the previous
/// one.
pub struct DoublingDelayScheduler<T> {
    initial_delay: Duration,
    scheduled_instants: BinaryHeap<Reverse<IndexedInstant>>,
    scheduled_tasks: Vec<ScheduledTask<T>>,
}

impl<T> Debug for DoublingDelayScheduler<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DoublingDelayScheduler")
            .field("initial delay", &self.initial_delay)
            .field("scheduled instant count", &self.scheduled_instants.len())
            .field("scheduled task count", &self.scheduled_tasks.len())
            .finish()
    }
}

impl<T> DoublingDelayScheduler<T> {
    pub fn new(initial_delay: Duration) -> Self {
        DoublingDelayScheduler::with_tasks(vec![], initial_delay)
    }

    pub fn with_tasks(initial_tasks: Vec<T>, initial_delay: Duration) -> Self {
        let mut scheduler = DoublingDelayScheduler {
            initial_delay,
            scheduled_instants: BinaryHeap::new(),
            scheduled_tasks: Vec::new(),
        };
        if initial_tasks.is_empty() {
            return scheduler;
        }
        let delta = initial_delay.div((initial_tasks.len()) as u32); // safety: len is non-zero
        for (i, task) in initial_tasks.into_iter().enumerate() {
            scheduler.add_task_after(task, delta.mul(i as u32));
        }
        scheduler
    }

    fn add_task_after(&mut self, task: T, delta: Duration) {
        let i = self.scheduled_tasks.len();
        let instant = Instant::now().add(delta);
        let indexed_instant = IndexedInstant::at(instant, i);
        self.scheduled_instants.push(Reverse(indexed_instant));
        let scheduled_task = ScheduledTask::new(task, self.initial_delay);
        self.scheduled_tasks.push(scheduled_task);
    }
}

#[async_trait]
impl<T: Send + Sync + Clone> TaskScheduler<T> for DoublingDelayScheduler<T> {
    fn add_task(&mut self, task: T) {
        self.add_task_after(task, Duration::ZERO);
    }

    async fn next_task(&mut self) -> T {
        match self.scheduled_instants.peek() {
            Some(&Reverse(IndexedInstant(instant, _))) => {
                let now = Instant::now();
                if now < instant {
                    Delay::new(instant - now).await;
                }
            }
            None => pending().await,
        }

        let Reverse(IndexedInstant(instant, i)) = self
            .scheduled_instants
            .pop()
            .expect("By the logic of the function, there is an instant available");
        let scheduled_task = &mut self.scheduled_tasks[i];

        let task = scheduled_task.task.clone();
        self.scheduled_instants
            .push(Reverse(IndexedInstant(instant + scheduled_task.delay, i)));

        scheduled_task.delay *= 2;
        task
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduler::{DoublingDelayScheduler, TaskScheduler};
    use std::{
        ops::{Add, Mul},
        time::Duration,
    };
    use tokio::time::Instant;

    #[tokio::test]
    async fn scheduler_yields_proper_order_of_tasks() {
        let mut scheduler = DoublingDelayScheduler::new(Duration::from_millis(25));

        scheduler.add_task(0);
        tokio::time::sleep(Duration::from_millis(2)).await;
        scheduler.add_task(1);

        let task = scheduler.next_task().await;
        assert_eq!(task, 0);
        let task = scheduler.next_task().await;
        assert_eq!(task, 1);
        let task = scheduler.next_task().await;
        assert_eq!(task, 0);
        let task = scheduler.next_task().await;
        assert_eq!(task, 1);

        tokio::time::sleep(Duration::from_millis(2)).await;
        scheduler.add_task(2);

        let task = scheduler.next_task().await;
        assert_eq!(task, 2);
        let task = scheduler.next_task().await;
        assert_eq!(task, 2);
        let task = scheduler.next_task().await;
        assert_eq!(task, 0);
        let task = scheduler.next_task().await;
        assert_eq!(task, 1);
        let task = scheduler.next_task().await;
        assert_eq!(task, 2);
    }

    #[tokio::test]
    async fn scheduler_properly_handles_initial_bunch_of_tasks() {
        let tasks = (0..5).collect();
        let before = Instant::now();
        let mut scheduler = DoublingDelayScheduler::with_tasks(tasks, Duration::from_millis(25));

        for i in 0..5 {
            let task = scheduler.next_task().await;
            assert_eq!(task, i);
            let now = Instant::now();
            // 0, 5, 10, 15, 20
            assert!(now - before >= Duration::from_millis(5).mul(i));
        }

        for i in 0..5 {
            let task = scheduler.next_task().await;
            assert_eq!(task, i);
            let now = Instant::now();
            // 25, 30, 35, 40, 45
            assert!(
                now - before
                    >= Duration::from_millis(5)
                        .mul(i)
                        .add(Duration::from_millis(25))
            );
        }
    }

    #[tokio::test]
    async fn asking_empty_scheduler_for_next_task_blocks() {
        let mut scheduler: DoublingDelayScheduler<u32> =
            DoublingDelayScheduler::new(Duration::from_millis(25));
        let future = tokio::time::timeout(Duration::from_millis(30), scheduler.next_task());
        let result = future.await;
        assert!(result.is_err()); // elapsed
    }
}
