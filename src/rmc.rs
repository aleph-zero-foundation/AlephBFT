use crate::{
    nodes::{NodeCount, NodeIndex},
    signed::{PartiallyMultisigned, Signable, Signed, UncheckedSigned},
    Indexed, MultiKeychain, Multisigned, PartialMultisignature, Signature,
};
use async_trait::async_trait;
use core::fmt::Debug;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    FutureExt, StreamExt,
};
use log::debug;
use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap},
    hash::Hash,
};
use tokio::time;

/// A message consists of either a signed (indexed) hash, or a multisigned hash.
#[derive(Clone)]
pub enum Message<H: Signable, S: Signature, M: PartialMultisignature> {
    SignedHash(UncheckedSigned<Indexed<H>, S>),
    MultisignedHash(UncheckedSigned<H, M>),
}

impl<H: Signable, S: Signature, M: PartialMultisignature> Message<H, S, M> {
    pub fn hash(&self) -> &H {
        match self {
            Message::SignedHash(unchecked) => unchecked.as_signable().as_signable(),
            Message::MultisignedHash(unchecked) => unchecked.as_signable(),
        }
    }
}

pub enum Task<H: Signable, MK: MultiKeychain> {
    BroadcastMessage(Message<H, MK::Signature, MK::PartialMultisignature>),
}

#[async_trait]
pub trait TaskScheduler<T> {
    fn add_task(&mut self, task: T);
    async fn next_task(&mut self) -> Option<T>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTask<T> {
    scheduled_time: time::Instant,
    task: T,
    delay: time::Duration,
}

impl<T> ScheduledTask<T> {
    fn new(scheduled_time: time::Instant, task: T, delay: time::Duration) -> Self {
        ScheduledTask {
            scheduled_time,
            task,
            delay,
        }
    }
}

impl<T: Ord> Ord for ScheduledTask<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .scheduled_time
            .cmp(&self.scheduled_time)
            .then_with(|| self.task.cmp(&other.task))
            .then_with(|| self.delay.cmp(&other.delay))
    }
}

impl<T: Ord> PartialOrd for ScheduledTask<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct DoublingDelayScheduler<T> {
    initial_delay: time::Duration,
    scheduled_tasks: BinaryHeap<ScheduledTask<T>>,
    on_new_task_tx: UnboundedSender<T>,
    on_new_task_rx: UnboundedReceiver<T>,
}

impl<T: Ord> DoublingDelayScheduler<T> {
    pub fn new(initial_delay: time::Duration) -> Self {
        let (on_new_task_tx, on_new_task_rx) = unbounded();
        DoublingDelayScheduler {
            initial_delay,
            scheduled_tasks: BinaryHeap::new(),
            on_new_task_tx,
            on_new_task_rx,
        }
    }
}

#[async_trait]
impl<T: Ord + Send + Clone> TaskScheduler<T> for DoublingDelayScheduler<T> {
    fn add_task(&mut self, task: T) {
        self.on_new_task_tx
            .unbounded_send(task)
            .expect("We own the the rx, so this can't fail");
    }

    async fn next_task(&mut self) -> Option<T> {
        let mut delay: futures::future::Fuse<_> = match self.scheduled_tasks.peek() {
            Some(task) => tokio::time::delay_until(task.scheduled_time).fuse(),
            None => futures::future::Fuse::terminated(),
        };
        // wait until either the scheduled time of the peeked task or a next call of add_task
        futures::select! {
            _ = delay => {},
            task = self.on_new_task_rx.next() => {
                if let Some(task) = task {
                    let curr_time = time::Instant::now();
                    let scheduled_task = ScheduledTask::new(curr_time, task, self.initial_delay);
                    self.scheduled_tasks.push(scheduled_task);
                } else {
                    debug!(target: "rmc", "The tasks ended");
                    return None;
                }
            }
        }
        let ScheduledTask {
            scheduled_time,
            task,
            delay,
        } = self.scheduled_tasks.pop().expect("Task must be ready");
        let scheduled_task = ScheduledTask::new(scheduled_time + delay, task.clone(), 2 * delay);
        self.scheduled_tasks.push(scheduled_task);
        Some(task)
    }
}

pub struct HashAlreadyExistsError {}

pub struct ReliableMulticast<'a, H: Signable + Hash, MK: MultiKeychain> {
    hash_states: HashMap<H, PartiallyMultisigned<'a, H, MK>>,
    network_rx: UnboundedReceiver<Message<H, MK::Signature, MK::PartialMultisignature>>,
    network_tx: UnboundedSender<(
        NodeIndex,
        Message<H, MK::Signature, MK::PartialMultisignature>,
    )>,
    keychain: &'a MK,
    node_count: NodeCount,
    scheduler: Box<dyn TaskScheduler<Task<H, MK>>>,
    multisigned_hashes_tx: UnboundedSender<Multisigned<'a, H, MK>>,
    multisigned_hashes_rx: UnboundedReceiver<Multisigned<'a, H, MK>>,
}

impl<'a, H: Signable + Hash + Eq + Clone + Debug, MK: MultiKeychain> ReliableMulticast<'a, H, MK> {
    pub fn new(
        network_rx: UnboundedReceiver<Message<H, MK::Signature, MK::PartialMultisignature>>,
        network_tx: UnboundedSender<(
            NodeIndex,
            Message<H, MK::Signature, MK::PartialMultisignature>,
        )>,
        keychain: &'a MK,
        node_count: NodeCount,
        scheduler: impl TaskScheduler<Task<H, MK>> + 'static,
    ) -> Self {
        let (multisigned_hashes_tx, multisigned_hashes_rx) = unbounded();
        ReliableMulticast {
            hash_states: HashMap::new(),
            network_rx,
            network_tx,
            keychain,
            node_count,
            scheduler: Box::new(scheduler),
            multisigned_hashes_tx,
            multisigned_hashes_rx,
        }
    }

    pub fn start_rmc(&mut self, hash: H) {
        let indexed_hash = Indexed::new(hash, self.keychain.index());
        let signed_hash = Signed::sign(indexed_hash, self.keychain);
        let message = Message::SignedHash(signed_hash.into_unchecked());
        self.handle_message(message);
    }

    fn handle_message(&mut self, message: Message<H, MK::Signature, MK::PartialMultisignature>) {
        let hash = message.hash().clone();
        if let Some(PartiallyMultisigned::Complete { .. }) = self.hash_states.get(&hash) {
            return;
        }
        match message {
            Message::MultisignedHash(unchecked) => match unchecked.check_multi(self.keychain) {
                Ok(multisigned) => {
                    self.hash_states
                        .insert(hash, PartiallyMultisigned::Complete { multisigned });
                }
                Err(_) => {
                    debug!(target: "rmc", "Received a hash with a bad multisignature");
                }
            },
            Message::SignedHash(unchecked) => {
                let signed_hash = match unchecked.check(self.keychain) {
                    Ok(signed_hash) => signed_hash,
                    Err(_) => {
                        debug!(target: "rmc", "Received a hash with a bad signature");
                        return;
                    }
                };

                let new_state = match self.hash_states.remove(&hash) {
                    None => signed_hash.into_partially_multisigned(self.keychain),
                    Some(partial) => partial.add_signature(signed_hash, self.keychain),
                };
                if let PartiallyMultisigned::Complete { multisigned } = &new_state {
                    self.multisigned_hashes_tx
                        .unbounded_send(multisigned.clone())
                        .unwrap();
                }
                self.hash_states.insert(hash.clone(), new_state);
            }
        }
    }

    fn do_task(&self, task: Task<H, MK>) {
        let Task::BroadcastMessage(message) = task;
        for recipient in 0..self.node_count.0 {
            let recipient = NodeIndex(recipient);
            self.network_tx
                .unbounded_send((recipient, message.clone()))
                .expect("Sending message should succeed");
        }
    }

    pub fn get_multisigned(&self, hash: &H) -> Option<Multisigned<'a, H, MK>> {
        match self.hash_states.get(hash)? {
            PartiallyMultisigned::Complete { multisigned } => Some(multisigned.clone()),
            _ => None,
        }
    }

    pub async fn next_multisigned_hash(&mut self) -> Option<Multisigned<'a, H, MK>> {
        loop {
            tokio::select! {
                multisigned_hash = self.multisigned_hashes_rx.next() => {
                    return multisigned_hash;
                }

                incoming_message = self.network_rx.next() => {
                    if let Some(incoming_message) = incoming_message {
                        self.handle_message(incoming_message);
                    } else {
                        debug!(target: "rmc", "Network connection closed");
                    }
                }

                task = self.scheduler.next_task() => {
                    if let Some(task) = task {
                        self.do_task(task);
                    } else {
                        debug!(target: "rmc", "Tasks ended");
                    }
                }
            }
        }
    }
}
