use futures::{
    channel::{mpsc, oneshot},
    StreamExt,
};
use log::{debug, info};

use crate::{
    config::Config,
    creator::Creator,
    extender::Extender,
    runway::{NotificationIn, NotificationOut},
    terminal::Terminal,
    utils::{into_infinite_stream, Barrier},
    Hasher, OrderedBatch, Receiver, Sender, SpawnHandle,
};

pub(crate) struct Consensus<H: Hasher, SH: SpawnHandle> {
    conf: Config,
    spawn_handle: SH,
    incoming_notifications: Receiver<NotificationIn<H>>,
    outgoing_notifications: Sender<NotificationOut<H>>,
    ordered_batch_tx: Sender<OrderedBatch<H::Hash>>,
}

impl<H: Hasher, SH: SpawnHandle> Consensus<H, SH> {
    pub(crate) fn new(
        conf: Config,
        spawn_handle: SH,
        incoming_notifications: Receiver<NotificationIn<H>>,
        outgoing_notifications: Sender<NotificationOut<H>>,
        ordered_batch_tx: Sender<OrderedBatch<H::Hash>>,
    ) -> Self {
        Consensus {
            conf,
            spawn_handle,
            incoming_notifications,
            outgoing_notifications,
            ordered_batch_tx,
        }
    }

    pub(crate) async fn run(self, mut exit: oneshot::Receiver<()>) {
        info!(target: "AlephBFT", "{:?} Starting all services...", self.conf.node_ix);

        let n_members = self.conf.n_members;

        let (electors_tx, electors_rx) = mpsc::unbounded();
        let mut extender = Extender::<H>::new(
            self.conf.node_ix,
            n_members,
            electors_rx,
            self.ordered_batch_tx,
        );
        let mut barrier = Barrier::new();

        let extender_barrier = barrier.clone("consensus/extender");
        let (extender_exit, exit_rx) = oneshot::channel();
        let extender_handle = self
            .spawn_handle
            .spawn_essential("consensus/extender", async move {
                extender.extend(exit_rx).await;

                extender_barrier.wait().await;
            });
        let mut extender_handle = into_infinite_stream(extender_handle).fuse();

        let (parents_tx, parents_rx) = mpsc::unbounded();
        let new_units_tx = self.outgoing_notifications.clone();
        let mut creator = Creator::new(self.conf.clone(), parents_rx, new_units_tx);

        let creator_barrier = barrier.clone("consensus/creator");
        let (creator_exit, exit_rx) = oneshot::channel();
        let creator_handle = self
            .spawn_handle
            .spawn_essential("consensus/creator", async move {
                creator.create(exit_rx).await;

                creator_barrier.wait().await;
            });
        let mut creator_handle = into_infinite_stream(creator_handle).fuse();

        let mut terminal = Terminal::new(
            self.conf.node_ix,
            self.incoming_notifications,
            self.outgoing_notifications,
        );

        // send a new parent candidate to the creator
        terminal.register_post_insert_hook(Box::new(move |u| {
            parents_tx
                .unbounded_send(u.into())
                .expect("Channel to creator should be open.");
        }));
        // try to extend the partial order after adding a unit to the dag
        terminal.register_post_insert_hook(Box::new(move |u| {
            electors_tx
                .unbounded_send(u.into())
                .expect("Channel to extender should be open.")
        }));

        let terminal_barrier = barrier.clone("consensus/terminal");
        let (terminal_exit, exit_rx) = oneshot::channel();
        let terminal_handle = self
            .spawn_handle
            .spawn_essential("consensus/terminal", async move {
                terminal.run(exit_rx).await;

                terminal_barrier.wait().await;
            });
        let mut terminal_handle = into_infinite_stream(terminal_handle).fuse();
        info!(target: "AlephBFT", "{:?} All services started.", self.conf.node_ix);

        futures::select! {
            _ = exit => {},
            _ = terminal_handle.next() => {
                debug!(target: "AlephBFT-consensus", "{:?} terminal task terminated early.", self.conf.node_ix);
            },
            _ = creator_handle.next() => {
                debug!(target: "AlephBFT-consensus", "{:?} creator task terminated early.", self.conf.node_ix);
            },
            _ = extender_handle.next() => {
                debug!(target: "AlephBFT-consensus", "{:?} extender task terminated early.", self.conf.node_ix);
            }
        }

        // we stop no matter if received Ok or Err
        let _ = terminal_exit.send(());
        let _ = creator_exit.send(());
        let _ = extender_exit.send(());

        barrier.wait().await;

        terminal_handle.next().await.unwrap();
        creator_handle.next().await.unwrap();
        extender_handle.next().await.unwrap();

        info!(target: "AlephBFT", "{:?} All services stopped.", self.conf.node_ix);
    }
}
