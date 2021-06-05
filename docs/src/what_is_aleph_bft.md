## 1. What is AlephBFT?

AlephBFT is a Rust implementation of the [Aleph Consensus Protocol](https://arxiv.org/abs/1908.05156) that offers a convenient API allowing it to be easily applied to various problems. The prime application of AlephBFT is to be the consensus engine (sometimes called the "finality gadget") of the [Aleph Zero blockchain](https://alephzero.org/).

### 1.1 High level idea of what AlephBFT does.

From the high level perspective AlephBFT allows a set of `N` prespecified nodes to agree on an ordering of items arriving in some unreliable streams of data that these nodes locally observe. An illustrative example to have in mind here is when these nodes all observe a growing blockchain that does not have a built-in notion of finality, and would like to finalize blocks. Then the above mentioned "streams of data" are simply the advancing sequences of blockchain "tips" that each of the nodes sees. Note that in this (and in all the interesting examples), because of possible forks, or network delays, the data streams of individual nodes might not be consistent.
The goal of a consensus protocol, is to ensure consistency of the decisions, even though relying on an unreliable data source. Consequently, what AlephBFT produces is a single stream of data that "combines" all the individual streams of the `N` nodes and importantly **is consistent** among the nodes. Thus, in the example above, all the nodes would produce a unique sequence of **finalized** blocks (see also the corresponding [AlephBFT API section](aleph_bft_api.md#321-blockchain-finality-gadget) for a more detailed description on how to use AlephBFT as a finality gadget for a blockchain).

### 1.2 High level idea of AlephBFT requirements.

Let us index the nodes taking part in the protocol as `i = 0, 1, ..., N-1` and call them "the committee", below we list some high-level requirements to be able to run AlephBFT among this nodes:

1. The nodes are connected via a network (that is not assumed to be 100% reliable) allowing them to send arbitrary messages to each other,
2. Each node knows the identities of all other nodes (via their public keys) and holds a private key allowing it to sign messages.
3. Each node `i` holds a data source object (to be explained in detail in the [DataIO subsection of AlephBFT API section](aleph_bft_api.md#311-dataio) -- see `DataIO`) that allows it 1) to receive fresh pieces of data (we refer to it as the input stream `in_i`), and 2) to check that a piece of data received from another node is "available". The availability is best understood when thinking about the blockchain example and data being block hashes. Then the availability question for a blockhash is essentially whether we locally hold a block with such a hash.
4. At most `(N-1)/3` of the nodes can be malicious (act with the intent to break the protocol guarantees).

### 1.3 High level idea of AlephBFT guarantees.

AlephBFT guarantees (as long as at most `(N-1)/3` nodes act maliciously) that each node `i` running the protocol produces a stream of data `out_i` such that:

1. The output streams of any two nodes `out_i` and `out_j` are consistent, in the sense that at any given time one is a prefix of another. Moreover, none of the streams gets "stuck", they keep producing items until the protocol is shut down. So, intuitively, all the streams produce the same items, but just at possibly different paces.
2. Each item in the output stream is "available" to at least half the honest (= not malicious) nodes in the committee.
3. Roughly speaking, most of the data items "proposed" by honest nodes (i.e., data coming from `in` streams of honest nodes) eventually land in the output stream.
