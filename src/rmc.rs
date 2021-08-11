//! Reliable MultiCast - a primitive for Reliable Broadcast protocol.
use crate::{
    nodes::NodeCount,
    signed::{PartiallyMultisigned, Signable, Signed, UncheckedSigned},
    Indexed, MultiKeychain, Multisigned, PartialMultisignature, Signature,
};
use async_trait::async_trait;
use codec::{Decode, Encode};
use core::fmt::Debug;
use futures::{
    channel::mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
    FutureExt, StreamExt,
};
use futures_timer::Delay;
use log::{debug, warn};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    hash::Hash,
    time,
    time::Duration,
};

/// An RMC message consisting of either a signed (indexed) hash, or a multisigned hash.
#[derive(Debug, Encode, Decode, Clone, PartialEq, Eq, Hash)]
pub enum Message<H: Signable, S: Signature, M: PartialMultisignature> {
    SignedHash(UncheckedSigned<Indexed<H>, S>),
    MultisignedHash(UncheckedSigned<H, M>),
}

impl<H: Signable, S: Signature, M: PartialMultisignature> Message<H, S, M> {
    pub fn hash(&self) -> &H {
        match self {
            Message::SignedHash(unchecked) => unchecked.as_signable_strip_index(),
            Message::MultisignedHash(unchecked) => unchecked.as_signable(),
        }
    }
    pub fn is_complete(&self) -> bool {
        matches!(self, Message::MultisignedHash(_))
    }
}

/// A task of brodcasting a message.
#[derive(Clone)]
pub enum Task<H: Signable, MK: MultiKeychain> {
    BroadcastMessage(Message<H, MK::Signature, MK::PartialMultisignature>),
}

/// Abstraction of a task-scheduling logic
///
/// Because the network can be faulty, the task of sending a message must be performed multiple
/// times to ensure that the recipient receives each message.
/// The trait [`TaskScheduler<T>`] describes in what intervals some abstract task of type `T`
/// should be performed.
#[async_trait]
pub trait TaskScheduler<T>: Send + Sync {
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

/// A basic task scheduler scheduling tasks with an exponential slowdown
///
/// A scheduler parameterized by a duration `initial_delay`. When a task is added to the scheduler
/// it is first scheduled immediately, then it is scheduled indefinitely, where the first delay is
/// `initial_delay`, and each following delay for that task is two times longer than the previous
/// one.
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
impl<T: Send + Sync + Clone> TaskScheduler<T> for DoublingDelayScheduler<T> {
    fn add_task(&mut self, task: T) {
        self.on_new_task_tx
            .unbounded_send(task)
            .expect("We own the the rx, so this can't fail");
    }

    async fn next_task(&mut self) -> Option<T> {
        let mut delay: futures::future::Fuse<_> = match self.scheduled_instants.peek() {
            Some(&Reverse(IndexedInstant(instant, _))) => {
                let now = time::Instant::now();
                if now > instant {
                    Delay::new(Duration::new(0, 0)).fuse()
                } else {
                    Delay::new(instant - now).fuse()
                }
            }
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

/// Reliable Multicast Box
///
/// The instance of [`ReliableMulticast<'a, H, MK>`] reliably broadcasts hashes of type `H`,
/// and when a hash is successfully broadcasted, the multisigned hash `Multisigned<'a, H, MK>`
/// is asynchronously returned.
///
/// A node with an instance of [`ReliableMulticast<'a, H, MK>`] can initiate broadcasting
/// a message `msg: H` by calling the [`ReliableMulticast::start_rmc`] method. As a result,
/// the node signs `msg` and starts broadcasting the signed message via the network.
/// When sufficintly many nodes call [`ReliableMulticast::start_rmc`] with the same message `msg`
/// and a node collects enough signatures to form a complete multisignature under the message,
/// the multisigned message is yielded by the instance of [`ReliableMulticast`].
/// The multisigned messages can be polled by calling [`ReliableMulticast::next_multisigned_hash`].
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/reliable_broadcast.html
/// for a high-level description of this protocol and how it is used for fork alerts.
pub struct ReliableMulticast<H: Signable + Hash, MK: MultiKeychain> {
    hash_states: HashMap<H, PartiallyMultisigned<H, MK>>,
    network_rx: UnboundedReceiver<Message<H, MK::Signature, MK::PartialMultisignature>>,
    network_tx: UnboundedSender<Message<H, MK::Signature, MK::PartialMultisignature>>,
    keychain: MK,
    scheduler: Box<dyn TaskScheduler<Task<H, MK>>>,
    multisigned_hashes_tx: UnboundedSender<Multisigned<H, MK>>,
    multisigned_hashes_rx: UnboundedReceiver<Multisigned<H, MK>>,
}

impl<H: Signable + Hash + Eq + Clone + Debug, MK: MultiKeychain> ReliableMulticast<H, MK> {
    pub fn new(
        network_rx: UnboundedReceiver<Message<H, MK::Signature, MK::PartialMultisignature>>,
        network_tx: UnboundedSender<Message<H, MK::Signature, MK::PartialMultisignature>>,
        keychain: MK,
        //kept for compatibility
        _node_count: NodeCount,
        scheduler: impl TaskScheduler<Task<H, MK>> + 'static,
    ) -> Self {
        let (multisigned_hashes_tx, multisigned_hashes_rx) = unbounded();
        ReliableMulticast {
            hash_states: HashMap::new(),
            network_rx,
            network_tx,
            keychain,
            scheduler: Box::new(scheduler),
            multisigned_hashes_tx,
            multisigned_hashes_rx,
        }
    }

    /// Initiate a new instance of RMC for `hash`.
    pub async fn start_rmc(&mut self, hash: H) {
        debug!(target: "AlephBFT-rmc", "starting rmc for {:?}", hash);
        let signed_hash = Signed::sign_with_index(hash, &self.keychain).await;

        let message = Message::SignedHash(signed_hash.into_unchecked());
        self.handle_message(message.clone());
        let task = Task::BroadcastMessage(message);
        self.do_task(task.clone());
        self.scheduler.add_task(task);
    }

    fn on_complete_multisignature(&mut self, multisigned: Multisigned<H, MK>) {
        let hash = multisigned.as_signable().clone();
        self.hash_states.insert(
            hash,
            PartiallyMultisigned::Complete {
                multisigned: multisigned.clone(),
            },
        );
        self.multisigned_hashes_tx
            .unbounded_send(multisigned.clone())
            .expect("We own the the rx, so this can't fail");

        let task = Task::BroadcastMessage(Message::MultisignedHash(multisigned.into_unchecked()));
        self.do_task(task.clone());
        self.scheduler.add_task(task);
    }

    fn handle_message(&mut self, message: Message<H, MK::Signature, MK::PartialMultisignature>) {
        let hash = message.hash().clone();
        if let Some(PartiallyMultisigned::Complete { .. }) = self.hash_states.get(&hash) {
            return;
        }
        match message {
            Message::MultisignedHash(unchecked) => match unchecked.check_multi(&self.keychain) {
                Ok(multisigned) => {
                    self.on_complete_multisignature(multisigned);
                }
                Err(_) => {
                    warn!(target: "AlephBFT-rmc", "Received a hash with a bad multisignature");
                }
            },
            Message::SignedHash(unchecked) => {
                let signed_hash = match unchecked.check(&self.keychain) {
                    Ok(signed_hash) => signed_hash,
                    Err(_) => {
                        warn!(target: "AlephBFT-rmc", "Received a hash with a bad signature");
                        return;
                    }
                };

                let new_state = match self.hash_states.remove(&hash) {
                    None => signed_hash.into_partially_multisigned(&self.keychain),
                    Some(partial) => partial.add_signature(signed_hash, &self.keychain),
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
        self.network_tx
            .unbounded_send(message)
            .expect("Sending message should succeed");
    }

    /// Fetches final multisignature.
    pub fn get_multisigned(&self, hash: &H) -> Option<Multisigned<H, MK>> {
        match self.hash_states.get(hash)? {
            PartiallyMultisigned::Complete { multisigned } => Some(multisigned.clone()),
            _ => None,
        }
    }

    /// Perform underlying tasks until the multisignature for the hash of this instance is collected.
    pub async fn next_multisigned_hash(&mut self) -> Multisigned<H, MK> {
        loop {
            futures::select! {
                multisigned_hash = self.multisigned_hashes_rx.next() => {
                    return multisigned_hash.expect("We own the tx, so it is not closed");
                }

                incoming_message = self.network_rx.next() => {
                    if let Some(incoming_message) = incoming_message {
                        self.handle_message(incoming_message);
                    } else {
                        debug!(target: "AlephBFT-rmc", "Network connection closed");
                    }
                }

                task = self.scheduler.next_task().fuse() => {
                    if let Some(task) = task {
                        self.do_task(task);
                    } else {
                        debug!(target: "AlephBFT-rmc", "Tasks ended");
                    }
                }
            }
        }
    }
}
