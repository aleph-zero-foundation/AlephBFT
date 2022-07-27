use std::{
    cmp::Ordering,
    collections::{binary_heap::PeekMut, BinaryHeap},
    time,
    time::Duration,
};

#[derive(Eq, PartialEq)]
struct ScheduledTask<T: Eq> {
    task: T,
    scheduled_time: time::Instant,
}

impl<T: Eq> PartialOrd for ScheduledTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Eq> Ord for ScheduledTask<T> {
    /// Compare tasks so that earlier times come first in a max-heap.
    fn cmp(&self, other: &Self) -> Ordering {
        other.scheduled_time.cmp(&self.scheduled_time)
    }
}

pub struct TaskQueue<T: Eq + PartialEq> {
    queue: BinaryHeap<ScheduledTask<T>>,
}

/// Implements a queue allowing for scheduling tasks for some time in the future.
///
/// Note that this queue is passive - nothing will happen until you call `pop_due_task`.
impl<T: Eq> TaskQueue<T> {
    /// Creates an empty queue.
    pub fn new() -> Self {
        Self {
            queue: BinaryHeap::new(),
        }
    }

    /// Schedules `task` for as soon as possible.
    pub fn schedule_now(&mut self, task: T) {
        self.schedule(task, time::Instant::now());
    }

    /// Schedules `task` for execution after `delay`.
    pub fn schedule_in(&mut self, task: T, delay: Duration) {
        self.schedule(task, time::Instant::now() + delay)
    }

    /// Schedules `task` for execution at `scheduled_time`.
    pub fn schedule(&mut self, task: T, scheduled_time: time::Instant) {
        self.queue.push(ScheduledTask {
            task,
            scheduled_time,
        })
    }

    /// Returns `Some(task)` if `task` is the most overdue task, and `None` if there are no overdue
    /// tasks.
    pub fn pop_due_task(&mut self) -> Option<T> {
        let scheduled_task = self.queue.peek_mut()?;

        if scheduled_task.scheduled_time <= time::Instant::now() {
            Some(PeekMut::pop(scheduled_task).task)
        } else {
            None
        }
    }

    /// Returns an iterator over all pending tasks.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.queue.iter().map(|x| &x.task)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_scheduling() {
        let mut q = TaskQueue::new();
        q.schedule_now(1);
        q.schedule_in(2, Duration::from_millis(5));
        q.schedule_in(3, Duration::from_millis(30));

        thread::sleep(Duration::from_millis(10));

        assert_eq!(Some(1), q.pop_due_task());
        assert_eq!(Some(2), q.pop_due_task());
        assert_eq!(None, q.pop_due_task());
    }
}
