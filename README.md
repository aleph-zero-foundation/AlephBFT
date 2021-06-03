[![Crate][crate-image]][crate-link]
[![Docs][docs-image]][docs-link]
[![Build Status][build-image]][build-link]
[![Apache 2.0 Licensed][license-image]][license-link]
![Rust Stable][rustc-image]

### Aleph Consensus

![aleph logo](logo.jpg "Aleph logo")

### Overview

Aleph is an asynchronous and Byzantine fault tolerant consensus protocol aimed
at ordering arbitrary messages (transactions). It has been designed to operate
continuously under conditions where there is no bound on message-delivery delay
and under the assumption that there is a significant probability of malicious
behavior, making it an excellent fit for blockchain-related applications.
For more information, check [the paper][paper-link]

This repository contains a Rust implementation of Aleph that offers a convenient
API enabling seamless application to various problems. The prime application of
the repository is the consensus engine (sometimes called the "finality gadget")
of the [Aleph Zero blockchain][aleph-node-link].

### Detailed documentation

If the crate's [documentation][docs-link] seems to be not comprehensive enough,
please refer to the [detailed version][reference-link].

### Implementation status

- current version is asynchronous, so it's driven by consensus events as opposed
  to some clock ticks
- while being asynchronous, the performance is still optimal in partially
  synchronous environment
- guaranteed safety in partially synchronous environment
- BFT - secure if less than one third of the committee is malicious
- secure against fork bombs, for details see [the paper][paper-link]
- network overhead optimized to not send all parents hashes but a bitmap and a control hash
- thorough testing, including malicious scenarios, and high code coverage

### Future work

- asynchronous liveness - current version is not secure in a rather theoretic
  scenario when an adversary controls all the delays in the network, including
  honest nodes messages. However, we take security very seriously, so we plan to
  make the consensus secure even in such a unusual scenario.
- get rid of the only major dependency - `parity-scale-codec`

### Using the crate

- Import rush in your crate
  ```
  [dependencies]:
  rush = "1"
  ```
- Rush requires user to provide it with an implementation of the following traits:
  - The [DataIO][dataio-link] trait is an abstraction for a component that provides data items,
    checks availability of data items and allows to input ordered data items. `DataIO` is
    parametrized with a `Data` generic type representing the type of items we would like to order.
    ```
    pub trait DataIO<Data> {
      type Error: Debug + 'static;
        fn get_data(&self) -> Data;
        fn check_availability(
          &self,
          data: &Data,
        ) -> Option<Pin<Box<dyn Future<Output = Result<(), Self::Error>> + Send>>>;
        fn send_ordered_batch(&mut self, data: OrderedBatch<Data>) -> Result<(), Self::Error>;
    }
    ```
  - The [KeyBox][keybox-link] trait is an abstraction for digitally signing arbitrary data and
    verifying signatures created by other nodes.
    ```
    pub trait KeyBox: Index + Clone + Send {
        type Signature: Signature;
        fn sign(&self, msg: &[u8]) -> Self::Signature;
        fn verify(&self, msg: &[u8], sgn: &Self::Signature, index: NodeIndex) -> bool;
    }
    ```
  - The [Network][network-link] trait defines the functionality we expect from the network layer:
    ```
    pub trait Network<H: Hasher, D: Data, S: Encode + Decode>: Send {
        type Error: Debug;
        fn send(&self, data: NetworkData<H, D, S>, node: NodeIndex) -> Result<(), Self::Error>;
        fn broadcast(&self, data: NetworkData<H, D, S>) -> Result<(), Self::Error>;
        async fn next_event(&mut self) -> Option<NetworkData<H, D, S>>;
    }
    ```
- Having all the above traits implemented, one can create a [Committee Member][member-link] and
  run it as an asynchronous task with an execution engine of choice.

### Dependencies

The repository is mainly self-contained. It is implemented using Rust's async features and depends only on the
`futures` create from the standard library. Moreover, it has some usual dependencies like
`log` and `rand` and one bigger for encoding, namely `parity-scale-codec`. In future work, we plan to get
rid of this dependency.

### Examples

There is a basic implementation of an honest committee member that is not
cryptographically secure and serves only as a working example of what has to be
implemented and not how it should be implemented.
The example may be run using:

    cargo run --example dummy_honest my_id n_members n_finalized

    my_id -- our index, 0-based
    n_members -- size of the committee
    n_finalized -- number of data to be finalized

### Tests

There are many unit tests and several integration tests that may be run by standard command
`cargo test --lib` or `cargo test --lib --skip medium` if you want to run just small tests.

### Resources

- Papers: [current version][paper-link], [old version][old-paper-link]
- docs: [crate documentation][docs-link], [reference][reference-link]

### License

rush is licensed under the terms of the the Apache License 2.0.

### Founding

The repository is founded by [Aleph Zero Foundation][webpage-link].

[//]: ### "badges"
[crate-image]: https://img.shields.io/crates/v/rush.svg
[crate-link]: https://crates.io/crates/rush
[docs-image]: https://docs.rs/rush/badge.svg
[docs-link]: https://docs.rs/rush
[build-image]: https://github.com/Cardinal-Cryptography/rush/workflows/CI/badge.svg
[build-link]: https://github.com/Cardinal-Cryptography/rush/actions?query=workflow%3ACI
[license-image]: https://img.shields.io/badge/license-Apache2.0-blue.svg
[license-link]: https://github.com/Cardinal-Cryptography/rush/blob/main/LICENSE
[rustc-image]: https://img.shields.io/badge/rustc-stable-blue.svg
[//]: ### "general links"
[reference-link]: https://Cardinal-Cryptography.github.io/rush/index.html
[paper-link]: https://dl.acm.org/doi/10.1145/3318041.3355467
[old-paper-link]: https://arxiv.org/abs/1810.05256
[aleph-node-link]: https://github.com/Cardinal-Cryptography/aleph-node
[webpage-link]: https://alephzero.org
