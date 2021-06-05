## 6 Reliable Broadcast

Recall that Reliable Broadcast is the primitive we use to broadcast `fork alerts` among nodes -- see [the section on alerts](how_alephbft_does_it.md#25-alerts----dealing-with-fork-spam). There are two requirements for a protocol realizing reliable broadcast:

1. If an honest sender initiates a reliable broadcast with some message `m` then the protocol terminates and as a result all honest nodes receive `m`.
2. If a malicious sender initiates a reliable broadcast then either it terminates for all honest nodes and they receive the same message `m`, or it does not terminate for any honest node.

So, roughly speaking, we want a broadcast primitive that is consistent even if a malicious node is the sender. There is the possibility that a malicious broadcast will not terminate, but it is not hard to see that this is the best we can hope for.

### 6.1 Consistent Broadcast using multisignatures -- RMC

The main idea behind the reliable broadcast implementation in AlephBFT is the use of multisignatures. Without going too much into details, think of a multisignature over a message `m` as a list of `N-f` signatures by `N-f` different committee nodes over the same message `m` (more efficient ways to achieve such a functionality are possible, like threshold signatures or signature aggregates, but they are beyond the scope of this section). Someone holding a multisignature over `m` can be sure that a large fraction of nodes "agree" (with the meaning of "agree" depending on the particular application) on `m`.

The RMC (Reliable MultiCast) protocol is a way to reliably disseminate a single hash `h` among all nodes (in the next section we explain how to extend it to disseminating arbitrary data and not only a hash). The idea is as follows:

1. Whenever a node `i` wishes to disseminate a hash `h` it initiates a reliable multicast instance by signing `h` and sending such a signed hash `SIG(h, i, sig_i)` to all other nodes.
2. Upon receiving such a signed hash, each node `j` signs `h` and sends its signed hash: `SIG(h, j, sig_j)` to all other nodes.
3. Each node keeps receiving signatures under `h` from different nodes. Upon receiving `N-f` of them, this node combines the signatures into a single multisignature `msig` and sends to all nodes a message `MULTISIG(h, msig)`.
4. Upon receiving `MULTISIG(h, msig)` under `h`, each node passes it also to all other nodes.

The moment when a node receives `MULTISIG(h, msig)` is considered as the completion of the multicast for this node (and even though the node still keeps resubmitting messages) this instance of RMC is considered as successful. If a RMC succeeds for some honest node then it is guaranteed to succeed for all honest nodes (but maybe with some delay). We refer to the file `/src/rmc.rs` for a thorough documentation of this component.

### 6.1 Reliable Broadcast based on RMC

Having the idea of RMC, one can modify it quite easily to achieve reliable broadcast. A naive way to do so would be to let the sender node hash the message `m` it intends to reliably broadcast into `h=hash(m)` and use RMC on the hash `h`. This almost works, except for the data availability problem -- a malicious sender might simply send a random meaningless hash `h` and then the honest nodes would never be able to recover the underlying data.

To circumvent the data availability problem we instruct the sender to send data `m` to all the nodes and only then to initiate RMC on `h = hash(m)`, if we make sure that no honest node proceeds with RMC before it receives the data `m`, then a successful RMC has the guarantee that most of the honest nodes hold the data `m`. This is the basic idea behind the protocol implemented for fork alerts in AlephBFT, we refer to `/src/alerts.rs` for details.
