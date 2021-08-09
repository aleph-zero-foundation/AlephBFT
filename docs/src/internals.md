## 5 AlephBFT Internals

To explain the inner workings of AlephBFT it is instructive to follow the path of a unit: from the very start when it is created to the moment when its round is decided and it's data is placed in one of the output batches. Here we give a brief overview and subsequently go more into details of specific components in dedicated subsections.

1. The unit is created by one of the node's `Creator` component -- implemented in `src/creator.rs`. Creator sends a notification to an outer component.
2. The newly created unit is filled with data, session information and a signature. This is done in `src/member.rs`. Subsequently a recurring task of broadcasting this unit is put in the task queue. The unit will be broadcast to all other nodes a few times (with some delays in between).
3. The unit is received by another node -- happens in `src/member.rs` and passes some validation (signature checks etc.). If all these checks pass and the unit is not detected to be a fork, then it is placed in the `UnitStore` -- the `store` field of the `Member` struct.
4. The idea is that this store keeps only **legit units** in the sense defined in [the section on alerts](how_alephbft_does_it.md#25-alerts----dealing-with-fork-spam). Thus no fork is ever be put there unless coming from an alert.
5. At a suitable moment the units from the store are further moved to a component called `Terminal` -- implemented in `src/terminal.rs`.
6. Roughly speaking, terminal is expected to "unpack" the unit, so that their parents become explicit (instead of being control hashes only).
7. Each unit whose parents are successfully decoded, is added to the "Dag". Each unit in the Dag is legit + has all its parents in the Dag.
8. Dag units are passed to a component called the `Extender` -- see `src/extender.rs`. The role of the extender is to efficiently run the `OrderData` algorithm, described in the [section on AlephBFT](how_alephbft_does_it.md).
9. Once a unit's data is placed in one of batches by the `Extender` then its path is over and can be safely discarded.

### 5.1 Creator

The creator produces units according to the AlephBFT protocol rules. It will wait until the prespecified delay has passed and attempt to create a unit using a maximal number of parents. If it is not possible yet, it will wait till the first moment enough parents are available. After creating the last unit, the creator stops producing new ones, although this is never expected to happen during correct execution.

Since the creator does not have access to the `DataIO` object and to the `KeyBox` it is not able to create the unit "fully", for this reason it only chooses parents, the rest is filled by the `Member`.

### 5.2 Unit Store in Member

As mentioned, the idea is that this stores only legit units and passes them to the `Terminal`. In case a fork is detected by a node `i`, all `i`'s units are attached to the appropriate alert.

### 5.3 Terminal

The `Terminal` receives legit units, yet there might be two issues with such units that would not allow them to be added to Dag:

1. A unit `U` might have a "wrong" control hash. From the perspective of the `Terminal` this means that naively picking `U`'s parents as units from previous round determined by `U.parent_map` results in a failed control hash check. Note that in case `U.creator` is honest and there are no forkers among `U` parents creators then this cannot happen and the control hash check always succeeds. Fail can happen because:
   a) either `U`'s one or multiple parents are forks and the `Terminal` either does not have the correct variants yet, or just has them but performed the naive check on different variants (note that guessing the correct variants might require exponential time so there is no point for the terminal to even try it),
   b) or `U`'s creator is dishonest and just put a control hash in the unit that does not "unhash" to anything meaningful.
   In any case the terminal triggers a request to `Member` to download the full list of `U`'s parent hashes, so that the ambiguity is resolved. Once a correct reponse is received by `Member` then it is passed back to the terminal so that it can "decode" the parents and proceed.
2. A unit `U` might have parents that are not legit. Before adding a unit `U` to the Dag, the terminal waits for all parents of `U` to be added to the Dag first.

There is often a situation where the terminal receives a unit `U` and for some reason there is no unit yet for a particular slot in `U`'s parents, i.e., `U`'s parent map says that one of the parents was created by node `i` but terminal has no unit with "coordinates" `(U.round - 1, i)` (`UnitCoord` type in the implementation -- means a pair consising of `(V.round, V.creator)` for some unit `V`). In such a case terminal makes a request to the `Member` to get such a unit, which is then followed by `Member` sending a series of requests to random nodes in order to fetch such a unit.

### 5.4 Extender

The `Extender`'s role is to receive Dag units (from `Terminal`) and extend the output stream. Towards this end it maintains the `round` for which the next `Head` must be chosen and the current unit `U` that is being decided for being `Head` or not. There is some caching made in the implementation so as to not recompute all the votes from scratch whenever a fresh unit arrives.
