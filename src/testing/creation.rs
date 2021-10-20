use crate::{
    creation::{run, IO},
    runway::NotificationOut as GenericNotificationOut,
    testing::mock::{gen_config, Data, Hasher64},
    units::{
        FullUnit as GenericFullUnit, PreUnit as GenericPreUnit, Unit as GenericUnit, UnitCoord,
    },
    NodeCount, Receiver, Round, Sender,
};

use futures::{
    channel::{mpsc, oneshot},
    FutureExt, StreamExt,
};

type PreUnit = GenericPreUnit<Hasher64>;
type Unit = GenericUnit<Hasher64>;
type FullUnit = GenericFullUnit<Hasher64, Data>;
type NotificationOut = GenericNotificationOut<Hasher64>;

fn preunit_to_unit(preunit: PreUnit) -> Unit {
    FullUnit::new(
        preunit,
        // The coord is wrong, but it doesn't matter.
        Data::new(UnitCoord::new(0, 0.into()), 0),
        0,
    )
    .unit()
}

struct TestController {
    max_round_per_creator: Vec<Round>,
    parents_for_creators: Sender<Unit>,
    units_from_creators: Receiver<NotificationOut>,
}

impl TestController {
    fn new(
        parents_for_creators: Sender<Unit>,
        units_from_creators: Receiver<NotificationOut>,
        n_members: NodeCount,
    ) -> Self {
        TestController {
            max_round_per_creator: vec![0; n_members.0],
            parents_for_creators,
            units_from_creators,
        }
    }

    async fn control_until(&mut self, max_round: Round) {
        let mut round_reached = 0;
        while round_reached < max_round {
            let notification = self
                .units_from_creators
                .next()
                .await
                .expect("Creator output channel isn't closed.");
            let preunit = match notification {
                NotificationOut::CreatedPreUnit(preunit, _) => preunit,
                _ => panic!("Unexpected notification from creator."),
            };
            let unit = preunit_to_unit(preunit);
            if unit.round() > round_reached {
                round_reached = unit.round();
            }
            self.max_round_per_creator[unit.creator().0] += 1;
            self.parents_for_creators
                .unbounded_send(unit.clone())
                .expect("Creator input channel isn't closed.");
        }
    }
}

fn setup_test(
    n_members: NodeCount,
) -> (
    TestController,
    Vec<oneshot::Sender<()>>,
    Vec<tokio::task::JoinHandle<()>>,
    Receiver<Unit>,
    Vec<Sender<Unit>>,
) {
    let (notifications_for_controller, notifications_from_creators) = mpsc::unbounded();
    let (units_for_creators, units_from_controller) = mpsc::unbounded();

    let test_controller =
        TestController::new(units_for_creators, notifications_from_creators, n_members);

    let mut handles = Vec::new();
    let mut killers = Vec::new();
    let mut units_for_creators = Vec::new();

    for node_ix in 0..n_members.0 {
        let (parents_for_creator, parents_from_controller) = mpsc::unbounded();

        let io = IO {
            incoming_parents: parents_from_controller,
            outgoing_units: notifications_for_controller.clone(),
        };
        let config = gen_config(node_ix.into(), n_members);
        let (starting_round_for_consensus, starting_round) = oneshot::channel::<Round>();

        units_for_creators.push(parents_for_creator);

        let (killer, exit) = oneshot::channel::<()>();

        let handle =
            tokio::spawn(async move { run(config.into(), io, starting_round, exit).await });
        starting_round_for_consensus
            .send(0)
            .expect("Sending the starting round should work.");

        killers.push(killer);
        handles.push(handle);
    }

    (
        test_controller,
        killers,
        handles,
        units_from_controller,
        units_for_creators,
    )
}

async fn finish(killers: Vec<oneshot::Sender<()>>, mut handles: Vec<tokio::task::JoinHandle<()>>) {
    for killer in killers {
        killer.send(()).unwrap();
    }

    for handle in handles.iter_mut() {
        handle.await.unwrap();
    }
}

// This test checks if 7 creators that start at the same time will create 50 units each
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn synchronous_creators_should_create_dag() {
    let n_members = NodeCount(7);
    let max_round: Round = 50;

    let (mut test_controller, killers, handles, mut units_from_controller, units_for_creators) =
        setup_test(n_members);
    loop {
        futures::select! {
            _ = test_controller.control_until(max_round).fuse() => break,
            unit = units_from_controller.next() => match unit {
                Some(unit) => for units_for_creator in &units_for_creators {
                    units_for_creator.unbounded_send(unit.clone()).expect("Channel to creator should be open");
                },
                None => panic!("Channel from controller should be open."),
            }
        }
    }
    assert!(test_controller
        .max_round_per_creator
        .iter()
        .all(|r| *r >= (max_round - 1)));
    finish(killers, handles).await;
}

// Disconnect test
// This test starts with 7 creators. After 25 rounds 2 of them are disconnected and reconnected
// again after the rest gets to round 50.
// Then it is checked if 5 creators achieve round 75 and rest at least round 73.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn disconnected_creators_should_create_dag() {
    let n_members = NodeCount(7);
    let max_round: Round = 25;

    let (mut test_controller, killers, handles, mut units_from_controller, units_for_creators) =
        setup_test(n_members);
    loop {
        futures::select! {
            _ = test_controller.control_until(max_round).fuse() => break,
            unit = units_from_controller.next() => match unit {
                Some(unit) => for units_for_creator in &units_for_creators {
                    units_for_creator.unbounded_send(unit.clone()).expect("Channel to creator should be open");
                },
                None => panic!("Channel from controller should be open."),
            }
        }
    }
    let max_round: Round = 50;
    let mut dropped_units = Vec::new();
    loop {
        futures::select! {
            _ = test_controller.control_until(max_round).fuse() => break,
            unit = units_from_controller.next() => match unit {
                Some(unit) => {
                    for units_for_creator in units_for_creators.iter().skip(2) {
                        units_for_creator.unbounded_send(unit.clone()).expect("Channel to creator should be open");
                    }
                    dropped_units.push(unit);
                },
                None => panic!("Channel from controller should be open."),
            }
        }
    }
    let max_round: Round = 75;
    for unit in dropped_units {
        for units_for_creator in units_for_creators.iter().take(2) {
            units_for_creator
                .unbounded_send(unit.clone())
                .expect("Channel to creator should be open");
        }
    }
    loop {
        futures::select! {
            _ = test_controller.control_until(max_round).fuse() => break,
            unit = units_from_controller.next() => match unit {
                Some(unit) => for units_for_creator in &units_for_creators {
                    units_for_creator.unbounded_send(unit.clone()).expect("Channel to creator should be open");
                },
                None => panic!("Channel from controller should be open."),
            }
        }
    }
    assert!(test_controller
        .max_round_per_creator
        .iter()
        .all(|r| *r >= (max_round - 2)));
    assert!(
        test_controller
            .max_round_per_creator
            .iter()
            .filter(|r| *r >= &(max_round - 1))
            .count()
            >= 5
    );
    finish(killers, handles).await;
}

// Catching up test
// This test checks if 5 creators that start at the same time and 2 creators
// that start after those first 5 reach round 25,
// will reach at least round 47, when at least one reaches round 50.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn late_creators_should_create_dag() {
    let n_members = NodeCount(7);
    let max_round: Round = 25;

    let (mut test_controller, killers, handles, mut units_from_controller, units_for_creators) =
        setup_test(n_members);
    let mut dropped_units = Vec::new();
    loop {
        futures::select! {
            _ = test_controller.control_until(max_round).fuse() => break,
            unit = units_from_controller.next() => match unit {
                Some(unit) => {
                    for units_for_creator in units_for_creators.iter().skip(2) {
                        units_for_creator.unbounded_send(unit.clone()).expect("Channel to creator should be open");
                    }
                    dropped_units.push(unit);
                },
                None => panic!("Channel from controller should be open."),
            }
        }
    }
    let max_round: Round = 50;
    for unit in dropped_units {
        for units_for_creator in units_for_creators.iter().take(2) {
            units_for_creator
                .unbounded_send(unit.clone())
                .expect("Channel to creator should be open");
        }
    }
    loop {
        futures::select! {
            _ = test_controller.control_until(max_round).fuse() => break,
            unit = units_from_controller.next() => match unit {
                Some(unit) => for units_for_creator in &units_for_creators {
                    units_for_creator.unbounded_send(unit.clone()).expect("Channel to creator should be open");
                },
                None => panic!("Channel from controller should be open."),
            }
        }
    }
    assert!(test_controller
        .max_round_per_creator
        .iter()
        .all(|r| *r >= (max_round - 2)));
    assert!(
        test_controller
            .max_round_per_creator
            .iter()
            .filter(|r| *r >= &(max_round - 1))
            .count()
            >= 5
    );
    finish(killers, handles).await;
}
