use async_trait::async_trait;
use codec::{Decode, Encode};
use log::{debug, error};
use parking_lot::Mutex;

use futures::{
    channel::{
        mpsc::{unbounded, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
    Future, StreamExt,
};

use std::{
    cell::{Cell, RefCell},
    collections::{hash_map::DefaultHasher, HashMap},
    hash::Hasher as StdHasher,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use aleph_bft::{
    exponential_slowdown, Config, DataIO as DataIOT, DelayConfig, Hasher, Index, KeyBox as KeyBoxT,
    Member, MultiKeychain as MultiKeychainT, Network as NetworkT, NodeCount, NodeIndex,
    OrderedBatch, PartialMultisignature as PartialMultisignatureT, SpawnHandle, TaskHandle,
};

pub fn init_log() {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::max())
        .is_test(true)
        .try_init();
}

pub fn gen_config(node_ix: NodeIndex, n_members: NodeCount) -> Config {
    let delay_config = DelayConfig {
        tick_interval: Duration::from_millis(5),
        requests_interval: Duration::from_millis(50),
        unit_broadcast_delay: Arc::new(|t| exponential_slowdown(t, 100.0, 1, 3.0)),
        //100, 100, 300, 900, 2700, ...
        unit_creation_delay: Arc::new(|t| exponential_slowdown(t, 50.0, usize::MAX, 1.000)),
        //50, 50, 50, 50, ...
    };
    Config {
        node_ix,
        session_id: 0,
        n_members,
        delay_config,
        max_round: 5000,
    }
}

// A hasher from the standard library that hashes to u64, should be enough to
// avoid collisions in testing.
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct Hasher64;

impl Hasher for Hasher64 {
    type Hash = [u8; 8];

    fn hash(x: &[u8]) -> Self::Hash {
        let mut hasher = DefaultHasher::new();
        hasher.write(x);
        hasher.finish().to_ne_bytes()
    }
}

pub type Hash64 = <Hasher64 as Hasher>::Hash;

#[derive(Clone)]
pub struct Spawner {
    handles: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

impl SpawnHandle for Spawner {
    fn spawn(&self, _name: &str, task: impl Future<Output = ()> + Send + 'static) {
        self.handles.lock().push(tokio::spawn(task))
    }

    fn spawn_essential(
        &self,
        _: &str,
        task: impl Future<Output = ()> + Send + 'static,
    ) -> TaskHandle {
        let (res_tx, res_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            task.await;
            res_tx.send(()).expect("We own the rx.");
        });
        self.handles.lock().push(task);
        Box::pin(async move { res_rx.await.map_err(|_| ()) })
    }
}

impl Spawner {
    pub async fn wait(&self) {
        for h in self.handles.lock().iter_mut() {
            let _ = h.await;
        }
    }
    pub fn new() -> Self {
        Spawner {
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl Default for Spawner {
    fn default() -> Self {
        Spawner::new()
    }
}

type NetworkReceiver<H, D, S, MS> =
    UnboundedReceiver<(aleph_bft::NetworkData<H, D, S, MS>, NodeIndex)>;
type NetworkSender<H, D, S, MS> = UnboundedSender<(aleph_bft::NetworkData<H, D, S, MS>, NodeIndex)>;

pub struct Network<
    H: aleph_bft::Hasher,
    D: aleph_bft::Data,
    S: aleph_bft::Signature,
    MS: aleph_bft::PartialMultisignature,
> {
    rx: UnboundedReceiver<(aleph_bft::NetworkData<H, D, S, MS>, NodeIndex)>,
    tx: UnboundedSender<(aleph_bft::NetworkData<H, D, S, MS>, NodeIndex)>,
    peers: Vec<NodeIndex>,
    index: NodeIndex,
}

impl<
        H: aleph_bft::Hasher,
        D: aleph_bft::Data,
        S: aleph_bft::Signature,
        MS: aleph_bft::PartialMultisignature,
    > Network<H, D, S, MS>
{
    pub fn index(&self) -> NodeIndex {
        self.index
    }
}

#[async_trait::async_trait]
impl<
        H: aleph_bft::Hasher,
        D: aleph_bft::Data,
        S: aleph_bft::Signature,
        MS: aleph_bft::PartialMultisignature,
    > NetworkT<H, D, S, MS> for Network<H, D, S, MS>
{
    fn send(&self, data: aleph_bft::NetworkData<H, D, S, MS>, recipient: aleph_bft::Recipient) {
        use aleph_bft::Recipient::*;
        match recipient {
            Node(node) => self
                .tx
                .unbounded_send((data, node))
                .expect("send on channel should work"),
            Everyone => {
                for peer in self.peers.iter() {
                    if *peer != self.index {
                        self.send(data.clone(), Node(*peer));
                    }
                }
            }
        }
    }

    async fn next_event(&mut self) -> Option<aleph_bft::NetworkData<H, D, S, MS>> {
        Some(self.rx.next().await?.0)
    }
}

struct Peer<
    H: aleph_bft::Hasher,
    D: aleph_bft::Data,
    S: aleph_bft::Signature,
    MS: aleph_bft::PartialMultisignature,
> {
    tx: NetworkSender<H, D, S, MS>,
    rx: NetworkReceiver<H, D, S, MS>,
}

pub struct UnreliableRouter<
    H: aleph_bft::Hasher,
    D: aleph_bft::Data,
    S: aleph_bft::Signature,
    MS: aleph_bft::PartialMultisignature,
> {
    peers: RefCell<HashMap<NodeIndex, Peer<H, D, S, MS>>>,
    peer_list: Vec<NodeIndex>,
    hook_list: RefCell<Vec<Box<dyn NetworkHook<H, D, S, MS>>>>,
    reliability: f64, //a number in the range [0, 1], 1.0 means perfect reliability, 0.0 means no message gets through
}

impl<
        H: aleph_bft::Hasher,
        D: aleph_bft::Data,
        S: aleph_bft::Signature,
        MS: aleph_bft::PartialMultisignature,
    > UnreliableRouter<H, D, S, MS>
{
    pub fn new(peer_list: Vec<NodeIndex>, reliability: f64) -> Self {
        UnreliableRouter {
            peers: RefCell::new(HashMap::new()),
            peer_list,
            hook_list: RefCell::new(Vec::new()),
            reliability,
        }
    }

    pub fn add_hook<HK: NetworkHook<H, D, S, MS> + 'static>(&mut self, hook: HK) {
        self.hook_list.borrow_mut().push(Box::new(hook));
    }

    pub fn connect_peer(&mut self, peer: NodeIndex) -> Network<H, D, S, MS> {
        assert!(
            self.peer_list.iter().any(|p| *p == peer),
            "Must connect a peer in the list."
        );
        assert!(
            !self.peers.borrow().contains_key(&peer),
            "Cannot connect a peer twice."
        );
        let (tx_in_hub, rx_in_hub) = unbounded();
        let (tx_out_hub, rx_out_hub) = unbounded();
        let peer_entry = Peer {
            tx: tx_out_hub,
            rx: rx_in_hub,
        };
        self.peers.borrow_mut().insert(peer, peer_entry);
        Network {
            rx: rx_out_hub,
            tx: tx_in_hub,
            peers: self.peer_list.clone(),
            index: peer,
        }
    }
}

impl<
        H: aleph_bft::Hasher,
        D: aleph_bft::Data,
        S: aleph_bft::Signature,
        MS: aleph_bft::PartialMultisignature,
    > Future for UnreliableRouter<H, D, S, MS>
{
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut self;
        let mut disconnected_peers: Vec<NodeIndex> = Vec::new();
        let mut buffer = Vec::new();
        for (peer_id, peer) in this.peers.borrow_mut().iter_mut() {
            loop {
                // this call is responsible for waking this Future
                match peer.rx.poll_next_unpin(cx) {
                    Poll::Ready(Some(msg)) => {
                        buffer.push((*peer_id, msg));
                    }
                    Poll::Ready(None) => {
                        disconnected_peers.push(*peer_id);
                        break;
                    }
                    Poll::Pending => {
                        break;
                    }
                }
            }
        }
        for peer_id in disconnected_peers {
            this.peers.borrow_mut().remove(&peer_id);
        }
        for (sender, (mut data, recipient)) in buffer {
            let rand_sample = rand::random::<f64>();
            if rand_sample > this.reliability {
                debug!("Simulated network fail.");
                continue;
            }

            if let Some(peer) = this.peers.borrow().get(&recipient) {
                for hook in this.hook_list.borrow_mut().iter_mut() {
                    hook.update_state(&mut data, sender, recipient);
                }
                peer.tx
                    .unbounded_send((data, sender))
                    .expect("channel should be open");
            }
        }
        if this.peers.borrow().is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub trait NetworkHook<
    H: aleph_bft::Hasher,
    D: aleph_bft::Data,
    S: aleph_bft::Signature,
    MS: aleph_bft::PartialMultisignature,
>: Send
{
    fn update_state(
        &mut self,
        data: &mut aleph_bft::NetworkData<H, D, S, MS>,
        sender: NodeIndex,
        recipient: NodeIndex,
    );
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, Hash)]
pub struct Data {
    ix: NodeIndex,
    id: u64,
}

impl Data {
    fn new(ix: NodeIndex, id: u64) -> Self {
        Data { ix, id }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct Signature {}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Encode, Decode)]
pub struct PartialMultisignature {
    signed_by: Vec<NodeIndex>,
}

impl PartialMultisignatureT for PartialMultisignature {
    type Signature = Signature;
    fn add_signature(self, _: &Self::Signature, index: NodeIndex) -> Self {
        let Self { mut signed_by } = self;
        for id in &signed_by {
            if *id == index {
                return Self { signed_by };
            }
        }
        signed_by.push(index);
        Self { signed_by }
    }
}

pub struct DataIO {
    ix: NodeIndex,
    round_counter: Cell<u64>,
    tx: UnboundedSender<OrderedBatch<Data>>,
}

impl DataIOT<Data> for DataIO {
    type Error = ();
    fn get_data(&self) -> Data {
        self.round_counter.set(self.round_counter.get() + 1);
        Data::new(self.ix, self.round_counter.get())
    }

    fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), ()> {
        self.tx.unbounded_send(data).map_err(|e| {
            error!(target: "data-io", "Error when sending data from DataIO {:?}.", e);
        })
    }
}

impl DataIO {
    pub fn new(ix: NodeIndex) -> (Self, UnboundedReceiver<OrderedBatch<Data>>) {
        let (tx, rx) = unbounded();
        let data_io = DataIO {
            ix,
            round_counter: Cell::new(0),
            tx,
        };
        (data_io, rx)
    }
}

#[derive(Clone)]
pub struct KeyBox {
    count: NodeCount,
    ix: NodeIndex,
}

impl KeyBox {
    pub fn new(count: NodeCount, ix: NodeIndex) -> Self {
        KeyBox { count, ix }
    }
}

impl Index for KeyBox {
    fn index(&self) -> NodeIndex {
        self.ix
    }
}

#[async_trait]
impl KeyBoxT for KeyBox {
    type Signature = Signature;

    fn node_count(&self) -> NodeCount {
        self.count
    }

    async fn sign(&self, _msg: &[u8]) -> Signature {
        Signature {}
    }

    fn verify(&self, _msg: &[u8], _sgn: &Signature, _index: NodeIndex) -> bool {
        true
    }
}

impl MultiKeychainT for KeyBox {
    type PartialMultisignature = PartialMultisignature;
    fn from_signature(&self, _: &Self::Signature, index: NodeIndex) -> Self::PartialMultisignature {
        let signed_by = vec![index];
        PartialMultisignature { signed_by }
    }
    fn is_complete(&self, _: &[u8], partial: &Self::PartialMultisignature) -> bool {
        (self.count * 2) / 3 < NodeCount(partial.signed_by.len())
    }
}

pub fn configure_network<
    H: aleph_bft::Hasher,
    D: aleph_bft::Data,
    S: aleph_bft::Signature,
    MS: aleph_bft::PartialMultisignature,
>(
    n_members: usize,
    reliability: f64,
) -> (UnreliableRouter<H, D, S, MS>, Vec<Network<H, D, S, MS>>) {
    let peer_list = || (0..n_members).map(NodeIndex);

    let mut router = UnreliableRouter::new(peer_list().collect(), reliability);
    let mut networks = Vec::new();
    for ix in peer_list() {
        let network = router.connect_peer(ix);
        networks.push(network);
    }
    (router, networks)
}

pub fn spawn_honest_member_generic<D: aleph_bft::Data, H: Hasher, K: MultiKeychainT>(
    spawner: impl SpawnHandle,
    node_index: NodeIndex,
    n_members: usize,
    network: impl 'static + NetworkT<H, D, K::Signature, K::PartialMultisignature>,
    data_io: impl DataIOT<D> + Send + 'static,
    mk: impl ToOwned<Owned = K>,
) -> oneshot::Sender<()> {
    let config = gen_config(node_index, n_members.into());
    spawn_honest_member_with_config(spawner, config, network, data_io, mk)
}

pub fn spawn_honest_member_with_config<D: aleph_bft::Data, H: Hasher, K: MultiKeychainT>(
    spawner: impl SpawnHandle,
    config: Config,
    network: impl 'static + NetworkT<H, D, K::Signature, K::PartialMultisignature>,
    data_io: impl DataIOT<D> + Send + 'static,
    mk: impl ToOwned<Owned = K>,
) -> oneshot::Sender<()> {
    let (exit_tx, exit_rx) = oneshot::channel();
    let spawner_inner = spawner.clone();
    let keybox = mk.to_owned();
    let member_task = async move {
        let member = Member::new(data_io, &keybox, config, spawner_inner.clone());
        member.run_session(network, exit_rx).await;
    };
    spawner.spawn("member", member_task);
    exit_tx
}
