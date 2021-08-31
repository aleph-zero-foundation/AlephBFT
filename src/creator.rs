use crate::{
    config::{Config, DelaySchedule},
    nodes::{NodeCount, NodeIndex, NodeMap},
    runway::NotificationOut,
    units::{ControlHash, PreUnit, Unit},
    Hasher, Receiver, Round, Sender,
};
use futures::{channel::oneshot, FutureExt, StreamExt};
use futures_timer::Delay;
use log::{info, trace, warn};
use std::time::Duration;

/// A process responsible for creating new units. It receives all the units added locally to the Dag
/// via the parents_rx channel endpoint. It creates units according to an internal strategy respecting
/// always the following constraints: if round is equal to 0, U has no parents, otherwise for a unit U of round r > 0
/// - all U's parents are from round (r-1),
/// - all U's parents are created by different nodes,
/// - one of U's parents is the (r-1)-round unit by U's creator,
/// - U has > floor(2*N/3) parents.
/// - U will appear in the channel only if all U's parents appeared there before
/// The currently implemented strategy creates the unit U according to a delay schedule and when enough
/// candidates for parents are available for all the above constraints to be satisfied.
///
/// We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/internals.html
/// Section 5.1 for a discussion of this component.
pub(crate) struct Creator<H: Hasher> {
    node_ix: NodeIndex,
    parents_rx: Receiver<Unit<H>>,
    new_units_tx: Sender<NotificationOut<H>>,
    n_members: NodeCount,
    candidates_by_round: Vec<NodeMap<Option<H::Hash>>>,
    n_candidates_by_round: Vec<NodeCount>, // len of this - 1 is the highest round number of all known units
    create_lag: DelaySchedule,
    max_round: Round,
}

impl<H: Hasher> Creator<H> {
    pub(crate) fn new(
        conf: Config,
        parents_rx: Receiver<Unit<H>>,
        new_units_tx: Sender<NotificationOut<H>>,
    ) -> Self {
        let n_members = conf.n_members;
        Creator {
            node_ix: conf.node_ix,
            parents_rx,
            new_units_tx,
            n_members,
            candidates_by_round: vec![NodeMap::new_with_len(n_members)],
            n_candidates_by_round: vec![NodeCount(0)],
            create_lag: conf.delay_config.unit_creation_delay,
            max_round: conf.max_round,
        }
    }

    // initializes the vectors corresponding to the given round (and all between if not there)
    fn init_round(&mut self, round: Round) {
        if (round + 1) as usize > self.n_candidates_by_round.len() {
            self.candidates_by_round
                .resize((round + 1).into(), NodeMap::new_with_len(self.n_members));
            self.n_candidates_by_round
                .resize((round + 1).into(), NodeCount(0));
        }
    }

    fn create_unit(&mut self, round: Round) {
        let parents = {
            if round == 0 {
                NodeMap::new_with_len(self.n_members)
            } else {
                self.candidates_by_round[(round - 1) as usize].clone()
            }
        };

        let control_hash = ControlHash::new(&parents);
        let parent_hashes: Vec<H::Hash> = parents.into_iter().flatten().collect();

        let new_preunit = PreUnit::new(self.node_ix, round, control_hash);
        trace!(target: "AlephBFT-creator", "{:?} Created a new unit {:?} at round {:?}.", self.node_ix, new_preunit, round);
        self.new_units_tx
            .unbounded_send(NotificationOut::CreatedPreUnit(new_preunit, parent_hashes))
            .expect("Notification channel should be open");

        self.init_round(round + 1);
    }

    fn add_unit(&mut self, round: Round, pid: NodeIndex, hash: H::Hash) {
        self.init_round(round);
        if self.candidates_by_round[round as usize][pid].is_none() {
            // passing the check above means that we do not have any unit for the pair (round, pid) yet
            self.candidates_by_round[round as usize][pid] = Some(hash);
            self.n_candidates_by_round[round as usize] += NodeCount(1);
        }
    }

    async fn wait_until_ready(&mut self, round: Round) {
        let mut delay = Delay::new((self.create_lag)(round.into())).fuse();
        loop {
            // We need to require a number higher by one then currently highest round
            // (by 2 then length) to prevent attack when a malicious node is creating
            // units without any delay to achive max_round as soon as possible
            if ((round + 2) as usize) < self.n_candidates_by_round.len() {
                // Since we get unit from round r, we have enough units from previous
                // rounds to skip delay, because we are already behind
                break;
            }

            futures::select! {
                unit = self.parents_rx.next() => {
                    if let Some(u) = unit {
                        self.add_unit(u.round(), u.creator(), u.hash());
                    }
                    continue;
                }
                _ = &mut delay => {
                    break;
                }
            }
        }

        let prev_round_index = match round.checked_sub(1) {
            Some(prev_round) => prev_round as usize,
            None => {
                return;
            }
        };

        // To create a new unit, we need to have at least floor(2*N/3) + 1 parents available in previous round.
        // Additionally, our unit from previous round must be available.
        let threshold = (self.n_members * 2) / 3 + NodeCount(1);

        while self.n_candidates_by_round[prev_round_index] < threshold
            || self.candidates_by_round[prev_round_index][self.node_ix].is_none()
        {
            if let Some(u) = self.parents_rx.next().await {
                self.add_unit(u.round(), u.creator(), u.hash());
            } else {
                warn!(target: "AlephBFT-creator", "{:?} get error as result from channel with parents.", self.node_ix);
                return;
            }
        }
    }

    pub(crate) async fn create(&mut self, mut exit: oneshot::Receiver<()>) {
        for round in 0..self.max_round {
            let mut delay = Delay::new(Duration::from_secs(30 * 60)).fuse();
            loop {
                futures::select! {
                    _ = self.wait_until_ready(round).fuse() => {
                        break;
                    }
                    _ = &mut delay => {
                        warn!(target: "AlephBFT-creator", "{:?} more than half hour has passed since we created the previous unit.", self.node_ix);
                        delay = Delay::new(Duration::from_secs(30 * 60)).fuse();
                    }
                    _ = &mut exit => {
                        info!(target: "AlephBFT-creator", "{:?} received exit signal.", self.node_ix);
                        return;
                    }
                }
            }
            self.create_unit(round);
        }
        warn!(target: "AlephBFT-creator", "{:?} Maximum round reached. Not creating another unit.", self.node_ix);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        testing::mock::{gen_config, Data, Hasher64},
        units::{FullUnit, UnitCoord},
    };
    use futures::channel::mpsc;

    struct TestController {
        notifications_in: Receiver<NotificationOut<Hasher64>>,
        units_out: Vec<Sender<Unit<Hasher64>>>,
        units: usize,
        max_units: usize,
        n_candidates_by_round: Vec<NodeCount>,
        candidates_by_round: Vec<NodeMap<Option<<Hasher64 as Hasher>::Hash>>>,
        n_members: NodeCount,
    }

    impl TestController {
        fn new(
            notifications_in: Receiver<NotificationOut<Hasher64>>,
            max_units: usize,
            n_members: NodeCount,
        ) -> Self {
            Self {
                notifications_in,
                units_out: vec![],
                units: 0,
                max_units,
                candidates_by_round: vec![NodeMap::new_with_len(n_members)],
                n_candidates_by_round: vec![NodeCount(0)],
                n_members,
            }
        }

        async fn control(&mut self) {
            while self.units < self.max_units {
                if let Some(NotificationOut::CreatedPreUnit(pre_unit, _hash)) =
                    self.notifications_in.next().await
                {
                    self.units += 1;
                    if self.n_candidates_by_round.len() <= pre_unit.round().into() {
                        self.candidates_by_round
                            .push(NodeMap::new_with_len(self.n_members));
                        self.n_candidates_by_round.push(NodeCount(0));
                    }
                    if self.candidates_by_round[pre_unit.round() as usize][pre_unit.creator()]
                        .is_none()
                    {
                        self.candidates_by_round[pre_unit.round() as usize][pre_unit.creator()] =
                            Some([0; 8]);
                        self.n_candidates_by_round[pre_unit.round() as usize] += NodeCount(1);
                    }
                    let full_unit = FullUnit::<Hasher64, Data>::new(
                        pre_unit.clone(),
                        Data::new(UnitCoord::new(0, 0.into()), 0),
                        0,
                    );
                    for c in self.units_out.iter() {
                        if c.unbounded_send(full_unit.unit()).is_err() {
                            panic!("Failed to send a unit to a creator");
                        }
                    }
                }
            }
        }
    }

    async fn start(
        n_members: usize,
        n_fallen_members: usize,
        max_units: usize,
    ) -> (
        TestController,
        Vec<oneshot::Sender<()>>,
        Vec<tokio::task::JoinHandle<()>>,
        Sender<NotificationOut<Hasher64>>,
    ) {
        let (to_test_controller, notifications_in) = mpsc::unbounded();

        let mut test_controller = TestController::new(
            notifications_in,
            max_units,
            (n_members + n_fallen_members).into(),
        );

        let mut handles = vec![];
        let mut killers = vec![];

        for node_ix in 0..n_members {
            let (units_out, from_test_controller) = mpsc::unbounded();

            let mut creator = Creator::new(
                gen_config(node_ix.into(), (n_members + n_fallen_members).into()),
                from_test_controller,
                to_test_controller.clone(),
            );

            test_controller.units_out.push(units_out);

            let (killer, exit) = oneshot::channel::<()>();

            let handle = tokio::spawn(async move { creator.create(exit).await });

            killers.push(killer);
            handles.push(handle);
        }

        (test_controller, killers, handles, to_test_controller)
    }

    async fn finish(
        killers: Vec<oneshot::Sender<()>>,
        mut handles: Vec<tokio::task::JoinHandle<()>>,
    ) {
        for killer in killers {
            killer.send(()).unwrap();
        }

        for handle in handles.iter_mut() {
            handle.await.unwrap();
        }
    }

    // This test checks if 7 creators that start at the same time will create 350 units together, 50 units each
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn synchronous_creators_should_create_dag() {
        let n_members: usize = 7;
        let rounds: usize = 50;
        let max_units: usize = n_members * rounds;

        let (mut test_controller, killers, handles, _) = start(n_members, 0, max_units).await;
        test_controller.control().await;
        assert_eq!(
            test_controller.n_candidates_by_round[rounds - 1],
            test_controller.n_members
        );
        finish(killers, handles).await;
    }

    // Catching up test
    // This test checks if 5 creators that start at the same time and 2 creators
    // that start after those first 5 create 125 units,
    // will create 346 units together, 50 units each of first 5 and at least 48 units rest
    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn asynchronous_creators_should_create_dag() {
        let n_members: usize = 5;
        let mut rounds = 25;
        let n_fallen_members: usize = 2;
        let mut max_units: usize = n_members * rounds;

        let (mut test_controller, mut killers, mut handles, to_test_controller) =
            start(n_members, n_fallen_members, max_units).await;
        test_controller.control().await;

        rounds = 50;
        max_units = (n_members + n_fallen_members) * rounds - n_fallen_members * 2;
        test_controller.max_units = max_units;

        for node_ix in n_members..(n_members + n_fallen_members) {
            let (units_out, from_test_controller) = mpsc::unbounded();

            let mut creator = Creator::new(
                gen_config(node_ix.into(), (n_members + n_fallen_members).into()),
                from_test_controller,
                to_test_controller.clone(),
            );
            creator.n_candidates_by_round = test_controller.n_candidates_by_round.clone();
            creator.candidates_by_round = test_controller.candidates_by_round.clone();

            test_controller.units_out.push(units_out);

            let (killer, exit) = oneshot::channel::<()>();

            let handle = tokio::spawn(async move { creator.create(exit).await });

            killers.push(killer);
            handles.push(handle);
        }

        test_controller.control().await;
        assert!(test_controller.n_candidates_by_round[rounds - 1] >= n_members.into());
        assert_eq!(
            test_controller.n_candidates_by_round[rounds - 3],
            (n_members + n_fallen_members).into()
        );
        finish(killers, handles).await;
    }
}
