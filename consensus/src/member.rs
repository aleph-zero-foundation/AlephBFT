use crate::{
    dissemination::{Addressed, DisseminationMessage},
    handle_task_termination,
    network::{Hub as NetworkHub, NetworkData, UnitMessage},
    runway::{self, NetworkIO, RunwayIO},
    Config, Data, DataProvider, FinalizationHandler, Hasher, MultiKeychain, Network, NodeIndex,
    OrderedUnit, Receiver, Recipient, Sender, Signature, SpawnHandle, Terminator,
    UnitFinalizationHandler,
};
use futures::{channel::mpsc, pin_mut, AsyncRead, AsyncWrite, FutureExt, StreamExt};
use log::{debug, error, info, warn};
use std::marker::PhantomData;

/// This adapter allows to map an implementation of [`FinalizationHandler`] onto implementation of [`UnitFinalizationHandler`].
pub struct FinalizationHandlerAdapter<FH, D, H> {
    finalization_handler: FH,
    _phantom: PhantomData<(D, H)>,
}

impl<FH, D, H> From<FH> for FinalizationHandlerAdapter<FH, D, H> {
    fn from(value: FH) -> Self {
        Self {
            finalization_handler: value,
            _phantom: PhantomData,
        }
    }
}

impl<D: Data, H: Hasher, FH: FinalizationHandler<D>> UnitFinalizationHandler
    for FinalizationHandlerAdapter<FH, D, H>
{
    type Data = D;
    type Hasher = H;

    fn batch_finalized(&mut self, batch: Vec<OrderedUnit<Self::Data, Self::Hasher>>) {
        for unit in batch {
            if let Some(data) = unit.data {
                self.finalization_handler.data_finalized(data)
            }
        }
    }
}

#[derive(Clone)]
pub struct LocalIO<DP: DataProvider, UFH: UnitFinalizationHandler, US: AsyncWrite, UL: AsyncRead> {
    data_provider: DP,
    finalization_handler: UFH,
    unit_saver: US,
    unit_loader: UL,
}

impl<
        H: Hasher,
        DP: DataProvider,
        FH: FinalizationHandler<DP::Output>,
        US: AsyncWrite,
        UL: AsyncRead,
    > LocalIO<DP, FinalizationHandlerAdapter<FH, DP::Output, H>, US, UL>
{
    pub fn new(
        data_provider: DP,
        finalization_handler: FH,
        unit_saver: US,
        unit_loader: UL,
    ) -> Self {
        Self {
            data_provider,
            finalization_handler: finalization_handler.into(),
            unit_saver,
            unit_loader,
        }
    }
}

impl<DP: DataProvider, UFH: UnitFinalizationHandler, US: AsyncWrite, UL: AsyncRead>
    LocalIO<DP, UFH, US, UL>
{
    pub fn new_with_unit_finalization_handler(
        data_provider: DP,
        finalization_handler: UFH,
        unit_saver: US,
        unit_loader: UL,
    ) -> Self {
        Self {
            data_provider,
            finalization_handler,
            unit_saver,
            unit_loader,
        }
    }
}

struct Member<H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    own_id: NodeIndex,
    unit_messages_for_network: Sender<(UnitMessage<H, D, S>, Recipient)>,
    unit_messages_from_network: Receiver<UnitMessage<H, D, S>>,
    notifications_for_runway: Sender<DisseminationMessage<H, D, S>>,
    notifications_from_runway: Receiver<Addressed<DisseminationMessage<H, D, S>>>,
    exiting: bool,
}

impl<H, D, S> Member<H, D, S>
where
    H: Hasher,
    D: Data,
    S: Signature,
{
    fn new(
        own_id: NodeIndex,
        unit_messages_for_network: Sender<(UnitMessage<H, D, S>, Recipient)>,
        unit_messages_from_network: Receiver<UnitMessage<H, D, S>>,
        notifications_for_runway: Sender<DisseminationMessage<H, D, S>>,
        notifications_from_runway: Receiver<Addressed<DisseminationMessage<H, D, S>>>,
    ) -> Self {
        Self {
            own_id,
            unit_messages_for_network,
            unit_messages_from_network,
            notifications_for_runway,
            notifications_from_runway,
            exiting: false,
        }
    }

    fn index(&self) -> NodeIndex {
        self.own_id
    }

    fn send_unit_message(&mut self, message: Addressed<UnitMessage<H, D, S>>) {
        for recipient in message.recipients() {
            if self
                .unit_messages_for_network
                .unbounded_send((message.message().clone(), recipient.clone()))
                .is_err()
            {
                warn!(target: "AlephBFT-member", "{:?} Channel to network should be open", self.index());
                self.exiting = true;
            }
        }
    }

    fn on_unit_message_from_units(&mut self, message: Addressed<DisseminationMessage<H, D, S>>) {
        self.send_unit_message(message.into())
    }

    async fn run(mut self, mut terminator: Terminator) {
        loop {
            futures::select! {
                event = self.notifications_from_runway.next() => match event {
                    Some(message) => {
                        self.on_unit_message_from_units(message);
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from Runway closed.", self.index());
                        break;
                    },
                },

                event = self.unit_messages_from_network.next() => match event {
                    Some(message) => {
                        self.send_notification_to_runway(message.into())
                    },
                    None => {
                        error!(target: "AlephBFT-member", "{:?} Unit message stream from network closed.", self.index());
                        break;
                    },
                },

                _ = terminator.get_exit().fuse() => {
                    debug!(target: "AlephBFT-member", "{:?} received exit signal", self.index());
                    self.exiting = true;
                },
            }
            if self.exiting {
                debug!(target: "AlephBFT-member", "{:?} Member decided to exit.", self.index());
                terminator.terminate_sync().await;
                break;
            }
        }

        debug!(target: "AlephBFT-member", "{:?} Member stopped.", self.index());
    }

    fn send_notification_to_runway(&mut self, notification: DisseminationMessage<H, D, S>) {
        if self
            .notifications_for_runway
            .unbounded_send(notification)
            .is_err()
        {
            warn!(target: "AlephBFT-member", "{:?} Sender to runway with DisseminationMessage messages should be open", self.index());
            self.exiting = true;
        }
    }
}

/// Starts the consensus algorithm as an async task. It stops establishing consensus for new data items after
/// reaching the threshold specified in [`Config::max_round`] or upon receiving a stop signal from `exit`.
/// For a detailed description of the consensus implemented by `run_session` see
/// [docs for devs](https://cardinal-cryptography.github.io/AlephBFT/index.html)
/// or the [original paper](https://arxiv.org/abs/1908.05156).
///
/// Please note that in order to fulfill the constraint [`UnitFinalizationHandler<Data = DP::Output, Hasher
/// = H>`] it is enough to provide implementation of [`FinalizationHandler<DP::Output>`]. We provide
/// implementation of [`UnitFinalizationHandler<Data = DP::Output, Hasher = H>`] for anything that satisfies
/// the trait [`FinalizationHandler<DP::Output>`] (by means of [`FinalizationHandlerAdapter`]). Implementing
/// [`UnitFinalizationHandler`] directly is considered less stable since it exposes intrisics which might be
/// subject to change. Implement [`FinalizationHandler<DP::Output>`] instead, unless you absolutely know
/// what you are doing.
pub async fn run_session<
    DP: DataProvider,
    UFH: UnitFinalizationHandler<Data = DP::Output>,
    US: AsyncWrite + Send + Sync + 'static,
    UL: AsyncRead + Send + Sync + 'static,
    N: Network<NetworkData<UFH::Hasher, DP::Output, MK::Signature, MK::PartialMultisignature>>,
    SH: SpawnHandle,
    MK: MultiKeychain,
>(
    config: Config,
    local_io: LocalIO<DP, UFH, US, UL>,
    network: N,
    keychain: MK,
    spawn_handle: SH,
    mut terminator: Terminator,
) {
    let index = config.node_ix();
    info!(target: "AlephBFT-member", "{:?} Starting a new session.", index);
    debug!(target: "AlephBFT-member", "{:?} Spawning party for a session.", index);

    let (alert_messages_for_alerter, alert_messages_from_network) = mpsc::unbounded();
    let (alert_messages_for_network, alert_messages_from_alerter) = mpsc::unbounded();
    let (unit_messages_for_units, unit_messages_from_network) = mpsc::unbounded();
    let (unit_messages_for_network, unit_messages_from_units) = mpsc::unbounded();
    let (runway_messages_for_runway, runway_messages_from_network) = mpsc::unbounded();
    let (runway_messages_for_network, runway_messages_from_runway) = mpsc::unbounded();

    debug!(target: "AlephBFT-member", "{:?} Spawning network.", index);
    let network_terminator = terminator.add_offspring_connection("AlephBFT-network");

    let network_handle = spawn_handle
        .spawn_essential("member/network", async move {
            NetworkHub::new(
                network,
                unit_messages_from_units,
                unit_messages_for_units,
                alert_messages_from_alerter,
                alert_messages_for_alerter,
            )
            .run(network_terminator)
            .await
        })
        .fuse();
    pin_mut!(network_handle);
    debug!(target: "AlephBFT-member", "{:?} Network spawned.", index);

    debug!(target: "AlephBFT-member", "{:?} Initializing Runway.", index);
    let runway_terminator = terminator.add_offspring_connection("AlephBFT-runway");
    let network_io = NetworkIO {
        alert_messages_for_network,
        alert_messages_from_network,
        unit_messages_from_network: runway_messages_from_network,
        unit_messages_for_network: runway_messages_for_network,
    };
    let runway_io = RunwayIO::new(
        local_io.data_provider,
        local_io.finalization_handler,
        local_io.unit_saver,
        local_io.unit_loader,
    );
    let spawn_copy = spawn_handle.clone();
    let runway_handle = spawn_handle
        .spawn_essential("member/runway", async move {
            runway::run(
                config,
                runway_io,
                keychain.clone(),
                spawn_copy,
                network_io,
                runway_terminator,
            )
            .await
        })
        .fuse();
    pin_mut!(runway_handle);
    debug!(target: "AlephBFT-member", "{:?} Runway spawned.", index);

    debug!(target: "AlephBFT-member", "{:?} Initializing Member.", index);
    let member = Member::new(
        index,
        unit_messages_for_network,
        unit_messages_from_network,
        runway_messages_for_runway,
        runway_messages_from_runway,
    );
    let member_terminator = terminator.add_offspring_connection("AlephBFT-member");
    let member_handle = spawn_handle
        .spawn_essential("member", async move {
            member.run(member_terminator).await;
        })
        .fuse();
    pin_mut!(member_handle);
    debug!(target: "AlephBFT-member", "{:?} Member initialized.", index);

    futures::select! {
        _ = network_handle => {
            error!(target: "AlephBFT-member", "{:?} Network-hub terminated early.", index);
        },

        _ = runway_handle => {
            error!(target: "AlephBFT-member", "{:?} Runway terminated early.", index);
        },

        _ = member_handle => {
            error!(target: "AlephBFT-member", "{:?} Member terminated early.", index);
        },

        _ = terminator.get_exit().fuse() => {
            debug!(target: "AlephBFT-member", "{:?} exit channel was called.", index);
        },
    }

    debug!(target: "AlephBFT-member", "{:?} Run ending.", index);

    terminator.terminate_sync().await;

    handle_task_termination(network_handle, "AlephBFT-member", "Network", index).await;
    handle_task_termination(runway_handle, "AlephBFT-member", "Runway", index).await;
    handle_task_termination(member_handle, "AlephBFT-member", "Member", index).await;

    info!(target: "AlephBFT-member", "{:?} Session ended.", index);
}
