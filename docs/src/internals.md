## 5 AlephBFT Internals

To explain the inner workings of AlephBFT it is instructive to follow the path of a unit: from the very start when it is created to the moment when its round is decided and it's data is placed in one of the output batches. Here we give a brief overview and subsequently go more into details of specific components in dedicated subsections.

1. The unit is created by one of the node's `Creator` component -- implemented in `creation/`. Creator sends the produced unit to `runway/`, which then sends it to `member.rs`.
2. A recurring task of broadcasting this unit is put in the task queue. The unit will be broadcast to all other nodes a few times (with some delays in between).
3. The unit is received by another node -- happens in `member.rs` and immediately send to `runway/` where it passes some validation (signature checks etc.). If all these checks pass and the unit is not detected to be a fork, then it is placed in the `UnitStore` -- the `store` field of the `Runway` struct.
4. The idea is that this store keeps only **legit units** in the sense defined in [the section on alerts](how_alephbft_does_it.md#25-alerts----dealing-with-fork-spam). Thus no fork is ever be put there unless coming from an alert.
5. At a suitable moment the units from the store are further moved to a component responsible for reconstructing the explicit parents for these units -- implemented in `reconstruction/parents.rs`.
6. Each unit whose parents are successfully decoded, is added to the "Dag". Each unit in the Dag is legit + has all its parents in the Dag. This is ensured by the implementation in `reconstruction/dag.rs`.
7. Dag units are passed to a component called the `Extender` -- see the files in `extension/`. The role of the extender is to efficiently run the `OrderData` algorithm, described in the [section on AlephBFT](how_alephbft_does_it.md).
8. Once a unit's data is placed in one of batches by the `Extender` then its path is over and can be safely discarded.

### 5.1 Creator

The creator produces units according to the AlephBFT protocol rules. It will wait until the prespecified delay has passed and attempt to create a unit using a maximal number of parents. If it is not possible yet, it will wait till the first moment enough parents are available. After creating the last unit, the creator stops producing new ones, although this is never expected to happen during correct execution.

### 5.2 Unit Store in Runway

As mentioned, the idea is that this stores only legit units and passes them to the reconstructing component. In case a fork is detected by a node `i`, all `i`'s units are attached to the appropriate alert.

### 5.3 Reconstruction

The next step is to reconstruct the structure of the Dag from the somewhat compressed information in the units.

#### 5.3.1 Parents

The reconstruction service receives legit units, but the information about their parents is only present as a control hash, i.e. which nodes created the parents and what was the combined hash of all the parents' hashes. Parents reconstruction remembers the first unit for any creator-round combination it encounters and optimistically uses this information to check the combined hash. If there are no dishonest nodes, which is the usual situation, then this means that every unit might at most have some parents that cannot yet be checked, because the node has not yet received them. In such a case requests for these missing units are sent to `Member`. After the units are received, the control hash check succeeds and thus the parents are reconstructed successfully.

If dishonest nodes participate in the protocol, then two additional things can go wrong:

1. either the unit has one or multiple parents that are forks, with variants different from the first ones received by this node to be precise. The reconstructing service might or might not have access to the correct variants, but in either case it does not attempt to perform the naive check on different variants -- guessing the correct variants might require exponential time so there is no point to even try it,
2.  or the unit's creator is dishonest and just put a control hash in the unit that does not "unhash" to anything meaningful.

In any case the reconstruction triggers a request to `Member` to download the full list of the unit's parent hashes, so that the ambiguity is resolved. Once a response is received by `Member` then it is passed back to the reconstruction so that it can "decode" the parents and proceed.

#### 5.3.2 Dag

The units parents might, for many reasons, not be reconstructed in an order agreeing with the Dag order, i.e. some of their ancestors might not yet be reconstructed. The Dag component ensures that units are only added to the Dag after their parents were already added, and thus any units emitted by this component are in an order agreeing with the Dag order.

### 5.4 Extender

The `Extender`'s role is to receive Dag units (in an order agreeing with the Dag order) and extend the output stream. Towards this end it elects the `Head` for each `round`. Such an election works by going through candidate units from this round either eliminating them or eventually electing one. Votes are computed and cached for each candidate until a decision on it is made, after which the election moves on to the next round (if elected as `Head`) or to the next unit (otherwise). After electing every `Head` the `Extender` deterministically orders all its unordered ancestors and the `Head` itself and returns the resulting batch.
