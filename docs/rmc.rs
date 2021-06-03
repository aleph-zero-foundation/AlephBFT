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
    cmp::Reverse,
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

#[derive(Clone)]
pub enum Task<H: Signable, MK: MultiKeychain> {
    BroadcastMessage(Message<H, MK::Signature, MK::PartialMultisignature>),
}

#[async_trait]
pub trait TaskScheduler<T>: Send {
    fn add_task(&mut self, task: T);
    async fn next_task(&mut self) -> Option<T>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTask<T> {
    task: T,
    delay: time::Duration,
}

impl<T> ScheduledTask<T> {
    fn new(task: T, delay: time::Duration) -> Self {
        ScheduledTask { task, delay }
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct IndexedInstant(time::Instant, usize);

impl IndexedInstant {
    fn now(i: usize) -> Self {
        let curr_time = time::Instant::now();
        IndexedInstant(curr_time, i)
    }
}

pub struct DoublingDelayScheduler<T> {
    initial_delay: time::Duration,
    scheduled_instants: BinaryHeap<Reverse<IndexedInstant>>,
    scheduled_tasks: Vec<ScheduledTask<T>>,
    on_new_task_tx: UnboundedSender<T>,
    on_new_task_rx: UnboundedReceiver<T>,
}

impl<T> DoublingDelayScheduler<T> {
    pub fn new(initial_delay: time::Duration) -> Self {
        let (on_new_task_tx, on_new_task_rx) = unbounded();
        DoublingDelayScheduler {
            initial_delay,
            scheduled_instants: BinaryHeap::new(),
            scheduled_tasks: Vec::new(),
            on_new_task_tx,
            on_new_task_rx,
        }
    }
}

#[async_trait]
impl<T: Send + Clone> TaskScheduler<T> for DoublingDelayScheduler<T> {
    fn add_task(&mut self, task: T) {
        self.on_new_task_tx
            .unbounded_send(task)
            .expect("We own the the rx, so this can't fail");
    }

    async fn next_task(&mut self) -> Option<T> {
        let mut delay: futures::future::Fuse<_> = match self.scheduled_instants.peek() {
            Some(&Reverse(IndexedInstant(instant, _))) => tokio::time::delay_until(instant).fuse(),
            None => futures::future::Fuse::terminated(),
        };
        // wait until either the scheduled time of the peeked task or a next call of add_task
        futures::select! {
            _ = delay => {},
            task = self.on_new_task_rx.next() => {
                if let Some(task) = task {
                    let i = self.scheduled_tasks.len();
                    let indexed_instant = IndexedInstant::now(i);
                    self.scheduled_instants.push(Reverse(indexed_instant));
                    let scheduled_task = ScheduledTask::new(task, self.initial_delay);
                    self.scheduled_tasks.push(scheduled_task);
                } else {
                    debug!(target: "rmc", "The tasks ended");
                    return None;
                }
            }
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
        self.handle_message(message.clone());
        self.scheduler.add_task(Task::BroadcastMessage(message))
    }

    fn on_complete_multisignature(&mut self, multisigned: Multisigned<'a, H, MK>) {
        let hash = multisigned.as_signable().clone();
        self.hash_states.insert(
            hash,
            PartiallyMultisigned::Complete {
                multisigned: multisigned.clone(),
            },
        );
        self.multisigned_hashes_tx
            .unbounded_send(multisigned.clone())
            .unwrap();
        self.scheduler
            .add_task(Task::BroadcastMessage(Message::MultisignedHash(
                multisigned.into_unchecked(),
            )));
    }

    fn handle_message(&mut self, message: Message<H, MK::Signature, MK::PartialMultisignature>) {
        let hash = message.hash().clone();
        if let Some(PartiallyMultisigned::Complete { .. }) = self.hash_states.get(&hash) {
            return;
        }
        match message {
            Message::MultisignedHash(unchecked) => match unchecked.check_multi(self.keychain) {
                Ok(multisigned) => {
                    self.on_complete_multisignature(multisigned);
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
                match new_state {
                    PartiallyMultisigned::Complete { multisigned } => {
                        self.on_complete_multisignature(multisigned)
                    }
                    incomplete => {
                        self.hash_states.insert(hash.clone(), incomplete);
                    }
                }
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

    pub async fn next_multisigned_hash(&mut self) -> Multisigned<'a, H, MK> {
        loop {
            tokio::select! {
                multisigned_hash = self.multisigned_hashes_rx.next() => {
                    return multisigned_hash.expect("We own the tx, so it is not closed");
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

#[cfg(test)]
mod tests;
