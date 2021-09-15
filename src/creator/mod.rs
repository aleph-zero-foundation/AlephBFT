use crate::{
    config::{Config as GeneralConfig, DelaySchedule},
    nodes::{NodeCount, NodeIndex},
    runway::NotificationOut,
    units::Unit,
    Hasher, Receiver, Round, Sender,
};
use futures::{channel::oneshot, FutureExt, StreamExt};
use futures_timer::Delay;
use log::{debug, info, warn, error};
use std::time::Duration;

mod creator;

use creator::Creator;

/// The configuration needed for the process creating new units.
pub struct Config {
    node_id: NodeIndex,
    n_members: NodeCount,
    create_lag: DelaySchedule,
    max_round: Round,
}

impl From<GeneralConfig> for Config {
    fn from(conf: GeneralConfig) -> Self {
        Config {
            node_id: conf.node_ix,
            n_members: conf.n_members,
            create_lag: conf.delay_config.unit_creation_delay,
            max_round: conf.max_round,
        }
        Ok(())
    }
}

pub struct IO<H: Hasher> {
    pub(crate) incoming_parents: Receiver<Unit<H>>,
    pub(crate) outgoing_units: Sender<NotificationOut<H>>,
}

async fn wait_until_ready<H: Hasher>(round: Round, creator: &mut Creator<H>, create_lag: &DelaySchedule, incoming_parents: &mut Receiver<Unit<H>>, mut exit: &mut oneshot::Receiver<()>) -> Result<(),()> {
    let mut delay = Delay::new(create_lag(round.into())).fuse();
    let mut delay_passed = false;
    while !delay_passed || !creator.can_create(round) {
        futures::select! {
            unit = incoming_parents.next() => match unit {
                Some(unit) => creator.add_unit(&unit),
                None => {
                    info!(target: "AlephBFT-creator", "Incoming parent channel closed, exiting.");
                    return Err(());
                }
            },
            _ = &mut delay => {
                if delay_passed {
                    warn!(target: "AlephBFT-creator", "More than half hour has passed since we created the previous unit.");
                }
                delay_passed = true;
                delay = Delay::new(Duration::from_secs(30 * 60)).fuse();
            },
            _ = exit => {
                info!(target: "AlephBFT-creator", "Received exit signal.");
                return Err(());
            },
        }
    }
    Ok(())
}

/// A process responsible for creating new units. It receives all the units added locally to the Dag
/// via the `incoming_parents` channel. It creates units according to an internal strategy respecting
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
pub async fn run<H: Hasher>(conf: Config, io: IO<H>, starting_round: oneshot::Receiver<Round>, mut exit: oneshot::Receiver<()>) {
    let Config {
        node_id,
        n_members,
        create_lag,
        max_round,
    } = conf;
    let mut creator = Creator::new(node_id, n_members);
    let IO {
        mut incoming_parents,
        outgoing_units,
    } = io;
    let starting_round = match starting_round.await {
        Ok(round) => round,
        Err(e) => {
            error!(target: "AlephBFT-creator", "Starting round not provided: {}", e);
            return;
        }
    };
    debug!(target: "AlephBFT-creator", "Creator starting from round {}", starting_round);
    for round in starting_round..max_round {
        if !creator.is_behind(round) {
            if wait_until_ready(round, &mut creator, &create_lag, &mut incoming_parents, &mut exit).await.is_err() {
                return;
            }
        }
        let (unit, parent_hashes) = creator.create_unit(round);
        if let Err(e) = outgoing_units.unbounded_send(NotificationOut::CreatedPreUnit(unit, parent_hashes)) {
            warn!(target: "AlephBFT-creator", "Notification send error: {}. Exiting.", e);
            return;
        }
    }
    warn!(target: "AlephBFT-creator", "Maximum round reached. Not creating another unit.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        testing::mock::{gen_config, Data, Hasher64},
        units::{FullUnit, UnitCoord},
        nodes::NodeMap,
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
        let (notifications_for_test_controller, notifications_from_creator) = mpsc::unbounded();

        let mut test_controller = TestController::new(
            notifications_from_creator,
            max_units,
            (n_members + n_fallen_members).into(),
        );

        let mut handles = vec![];
        let mut killers = vec![];

        for node_ix in 0..n_members {
            let (parents_for_creator, parents_from_test_controller) = mpsc::unbounded();

            let io = IO {
                incoming_parents: parents_from_test_controller,
                outgoing_units: notifications_for_test_controller.clone(),
            };
            let config = gen_config(node_ix.into(), (n_members + n_fallen_members).into());
            let (starting_round_for_consensus, starting_round) = oneshot::channel::<Round>();

            test_controller.units_out.push(parents_for_creator);

            let (killer, exit) = oneshot::channel::<()>();

            let handle = tokio::spawn(async move { run(config.into(), io, starting_round, exit).await });
            starting_round_for_consensus.send(0).expect("Sending the starting round should work.");

            killers.push(killer);
            handles.push(handle);
        }

        (test_controller, killers, handles, notifications_for_test_controller)
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

    // TODO(timorl): temporarily removed two tests here, because they were cheating in a way that is now
    // impossible. You shouldn't be reading this comment if you aren't me, if you are something
    // went wrong and let me know.
}
