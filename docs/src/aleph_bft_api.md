## 3 API of AlephBFT

### 3.1 Required Trait Implementations.

#### 3.1.1 DataProvider & FinalizationHandler.

The DataProvider trait is an abstraction for a component that provides data items. `DataProvider` is parametrized with a `Data` generic type representing the type of items we would like to order. Below we give examples of what these might be.

```rust
pub trait DataProvider {
    type Output: Data;

    async fn get_data(&mut self) -> Option<Self::Output>;
}
```

AlephBFT internally calls `get_data()` whenever a new unit is created and data needs to be placed inside. If no data is currently available, the method should return `None` immediately to prevent halting unit creation.

The FinalizationHandler trait is an abstraction for a component that should handle finalized items. Same as `DataProvider` is parametrized with a `Data` generic type.

```rust
pub trait FinalizationHandler<Data> {
    fn data_finalized(&mut self, data: Data);
}
```

Calls to function `data_finalized` represent the order of the units that AlephBFT produced and that hold some data.


#### 3.1.2 Network.

The Network trait defines the functionality we expect the network layer to satisfy and is quite straightforward:

```rust
pub trait Network<H: Hasher, D: Data, S: Encode + Decode>: Send {
    fn send(&self, data: NetworkData<H, D, S>, recipient: Recipient);
    async fn next_event(&mut self) -> Option<NetworkData<H, D, S>>;
}
```

Here `NetworkData` is a type representing possible network messages for the AlephBFT protocol. For the purpose of implementing the Network trait what matters the most is that they implement the `Encode` and `Decode` traits, i.e., allow for serialization/deserialization thus can be treated as byte arrays if that is more convenient. The `Recipient` represents who should receive the message, either everyone or a node with a specific index:

```rust
pub enum Recipient {
    Everyone,
    Node(NodeIndex),
}
```

Additionally `NetworkData` implements a `included_data` method which returns all the `Data` that might end up ordered as a result of this message being passed to AlephBFT. The implementation of `Network` should ensure that the user system is ready to have that `Data` be ordered. In the case of `Data` only representing actual data being ordered (e.g. hashes of blocks of transactions), this means ensuring data availability before passing the messages on.

The `send` method has straightforward semantics: sending a message to a single or to all the nodes. `next_event` is an asynchronous method for receiving messages from other nodes.

**Note on Rate Control**: it is assumed that Network **implements a rate control mechanism** guaranteeing that no node is allowed to spam messages without limits. We do not specify details yet, but in future releases we plan to publish recommended upper bounds for the amounts of bandwidth and number of messages allowed per node per a unit of time. These bounds must be carefully crafted based upon the number of nodes `N` and the configured delays between subsequent Dag rounds, so that at the same time spammers are cut off but honest nodes are able function correctly within these bounds.

**Note on Network Reliability**: it is not assumed that each message that AlephBFT orders to send reaches its intended recipient, there are some built-in reliability mechanisms within AlephBFT that will automatically detect certain failures and resend messages as needed. Clearly, the less reliable the network is, the worse the performance of AlephBFT will be (generally slower to produce output). Also, not surprisingly if the percentage of dropped messages is too high AlephBFT might stop making progress, but from what we observe in tests, this happens only when the reliability is extremely bad, i.e., drops below 50% (which means there is some significant issue with the network).

#### 3.1.3 Keychain.

The `Keychain` trait is an abstraction for digitally signing arbitrary data and verifying signatures created by other nodes.

```rust
pub trait Keychain: Index + Clone + Send + Sync + 'static {
    type Signature: Signature;
    fn sign(&self, msg: &[u8]) -> Self::Signature;
    fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool;
}
```

A typical implementation of Keychain would be a collection of `N` public keys, an index `i` and a single private key corresponding to the public key number `i`. The meaning of `sign` is then to produce a signature using the given private key, and `verify(msg, s, j)` is to verify whether the signature `s` under the message `msg` is correct with respect to the public key of the `j`th node.

#### 3.1.4 Read & Write – recovering mid session crashes

The `std::io::Write` and `std::io::Read` traits are used for creating backups of Units created in a session. This is a part of crash recovery. Units created are needed for member to recover after crash during a session for Aleph to be BFT. This means that user needs to provide two traits `std::io::Write` and `std::io::Read` that are used for storing and reading Unit that are processed during operation. At first (without any crash) `std::io::Read` should return nothing. After crash it should contain all data that was stored before in this session.

These traits are optional. If you do not want to recover crashes mid session or your session handling ensures AlephBFT will not run in the same session twice you can pass NOOP implementation here.

[`std::io::Write`](https://doc.rust-lang.org/std/io/trait.Write.html#) should provide a way of writing data generated during session which should be backed up. **`flush` method should block until the written data is backed up.**

[`std::io::Read`](https://doc.rust-lang.org/std/io/trait.Read.html#) should provide a way of retrieving backups of all data generated during session by this member in case of crash. **`std::io::Read` should have a copy of all data so that writing to `std::io::Write` has no effect on reading.**

### 3.2 Examples

While the implementations of `Keychain`, `std::io::Write`, `std::io::Read` and `Network` are pretty much universal, the implementation of `DataProvider` and `FinalizationHandler` depends on the specific application. We consider two examples here.

#### 3.2.1 Blockchain Finality Gadget.

Consider a blockchain that does not have an intrinsic finality mechanism, so for instance it might be a PoW chain with probabilistic finality or a chain based on PoS that uses some probabilistic or round-robin block proposal mechanism. Each of the `N` nodes in the network has its own view of the chain and at certain moments in time there might be conflicts on what the given nodes consider as the "tip" of the blockchain (because of possible forks or network delays). We would like to design a finality mechanism based on AlephBFT that will allow the nodes to have agreement on what the "tip" is. Towards this end we just need to implement a suitable `DataProvider` object and filtering of network messages for AlephBFT.

For concreteness, suppose each node holds the genesis block `B[0]` and its hash `hash(B[0])` and treats it as the highest finalized block so far.

We start by defining the `Data` type: this will be simply `Hash` representing the block hash in our blockchain. Subsequently, we implement `get_data` (note that we use pseudocode instead of Rust, for clarity)

```
def get_data():
	let B be the tip of the longest chain extending from the most recently finalized block
	return Some(hash(B))
```

This is simply the hash of the block the node thinks is the current "tip".

Using the `included_data` method of `NetworkData` we can filter incoming network messages in our implementation of `Network`. The code handling incoming network messages could be

```
def handle_incoming_message(M):
 let hashes = M.included_data()
 if we have all the blocks referred to by hashes:
	 if we have all the ancestors of these blocks:
			add M to ready_messages
			return
	add M with it's required hashes to waiting_messages
	request all the blocks we don't have from random nodes
```

We note that availability of a block alone is not quite enough for this case as, for instance, if the parent of a block is missing from out storage, we cannot really think of the block as available because we cannot finalize it.

When we get a new block we can check all the messages in `wating_messages` and move the ones that now have all the hashes satisfied into `ready_messages`.

```
def handle_incoming_block(B):
	block_hash = hash(B)
	for M in waiting_messages:
		if M depends on block_hash:
			remove the dependency
			if we don't have an ancestor B' of B:
				add a dependency on hash(B') and request it from random nodes
			if M has no more dependencies:
				move M to ready_messages
```

The `next` method of `Network` simply returns messages from `ready_messages`.

Now we can implement handling block finalization by implementing trait `FinalizationHandler`.

```
def data_finalized(block_hash):
	let finalized = the highest finalized block so far
 // We have this block in local storage by the above filtering.
    let B be the block such that hash(B) == block_hash
    if finalized is an ancestor of B:
        finalize all the blocks on the path from finalized to B
```

Since (because of AlephBFT's guarantees) all the nodes locally observe hashes in the same order, the above implies that all the nodes will finalize the same blocks, with the only difference being possibly a slight difference in timing.

#### 3.2.2 State Machine Replication (Standalone Blockchain).

Suppose the set of `N` nodes would like to implement State Machine Replication, so roughly speaking, a blockchain. Each of the nodes keeps a local transaction pool: transactions it received from users, and the goal is to keep producing blocks with transactions, or in other words produce a linear ordering of these transactions. As previously, we demonstrate how one should go about implementing the `DataProvider` and `FinalizationHandler` objects for this application.

First of all, `Data` in this case is `Vec<Transaction>`, i.e., a list of transactions.

```
def get_data():
	tx_list = []
	while tx_pool.not_empty() and tx_list.len() < 100:
		tx = tx_pool.pop()
		tx_list.append(tx)
	return Some(tx_list)
```

We simply fetch at most 100 transactions from the local pool and return such a list of transactions.

```
def data_finalized(tx_list):
	let k be the number of the previous block
	remove duplicated from tx_list
	remove from tx_list all transactions that appeared in blocks B[0], B[1], ..., B[k]
	form block B[k+1] using tx_list as its content
	add B[k+1] to the blockchain
```

Whenever a new batch is received we simply create a new block out of the batch's content.

When it comes to availability, in this case `Data` is not a cryptographic fingerprint of data, but the data itself, so no filtering of network messages is necessary. However, they can be inspected to precompute some operations as an optimization.

### 3.3 Guarantees of AlephBFT.

Let `round_delay` be the average delay between two consecutive rounds in the Dag that can be configured in AlephBFT (default value: 0.5 sec). Under the assumption that there are at most `floor(N/3)` dishonest nodes in the committee and the network behaves reasonably well (we do not specify the details here, but roughly speaking, a weak form of partial synchrony is required) AlephBFT guarantees that:

1. Each honest node will make progress in producing to the `out` stream at a pace of roughly `1` ordered batch per `round_delay` seconds (by default, two batches per second).
2. For honest nodes it is guaranteed that the data items they input in the protocol (from their local `DataProvider` object) will have `FinalizationHandler::data_finalized` called on them, usually with a delay of roughly `~round_delay*4` from the time of inputting it.

### 3.3.1 What happens when not enough nodes are honest.

If there are less than `floor(2/3N)+1` nodes that are online and are honestly following the protocol rules then certain failures might happen. There are two possibilities:

1. **Stall** -- the output streams of nodes stop producing data items. This is also what will happen when the nodes are generally honest, but there is either a significant network partition or lots of nodes crash. If this is not caused by malicious behavior but network issues, the protocol will recover by itself and eventually resume its normal execution.
2. **Inconsistent Output** -- this is the most extreme failure that can happen and can only be a result of malicious behavior of a significant fraction of all the nodes. It means that the honest nodes' output streams stop being consistent. In practice for this to happen the adversary must control _lots_ of nodes, i.e., around `(2/3)N`. The type of failure that would usually happen if the adversary controls barely above `floor(1/3N)+1` is stall.

### 3.4 AlephBFT Sessions.

Currently the API of AlephBFT allows to run a single Session that is expected to last a fixed number of rounds and thus to finalize a fixed number of output batches. By default a AlephBFT Session is `5000` rounds long but out of these `5000` there are `3000` rounds that the protocol proceeds at a regular speed (i.e., `500ms` per round) and after that starts to slow down (each round is `1.005` times slower than the previous one) so that round `5000` is virtually impossible to reach.

There are essentially two ways to use AlephBFT:

1. **Single Session** -- just run a single session to make consensus regarding some specific one-time question. In this case one can run the default configuration and just terminate the protocol once the answer is in the output stream.
2. **Multiple Sessions** -- a mode of execution when AlephBFT is run several times sequentially. An important motivating example is the use of AlephBFT as a finality gadget for a blockchain. Think of session `k` as being responsible for finalizing blocks at heights `[100k, 100(k+1)-1]`. There should be then an external mechanism to run a new AlephBFT session when the last block of a session gets finalized (and stop inactive sessions as well). This example gives a clear answer for why we opted for the slowdown after round `3000` as explained above: this is to make sure that no matter the variance in block-time of the block production mechanism, and no matter whether there are stalls, network issues, crashes, etc it is guaranteed that the prespecified segment of blocks will be finalized in a given session. Readers who are experienced with consensus engines are surely aware of how problematic it would be if at the end of a session, say, only `70` out of the intended `100` blocks would be finalized. That's why it's better to slow down consensus but make sure it achieves the intended goal.

**Why are there even sessions in AlephBFT?** To answer this question one would need to make a deep dive into the internal workings of AlephBFT, but a high level summary is: we want to make AlephBFT blazing fast, hence we need to keep everything in RAM (no disk), hence we need to have a round limit, hence we need sessions. For every "hence" in the previous sentence there are extensive arguments to back it, but they are perhaps beyond the scope of this document. We are aware of the inconvenience that it brings -- being forced to implement a session manager, but:

1. We feel that depending on the application there might be different ways to deal with sessions and its better if we leave the task of session managing to the user.
2. In one of the future releases we plan to add an optional default session manager, but will still encourage the user to implement a custom one for a particular use-case.
