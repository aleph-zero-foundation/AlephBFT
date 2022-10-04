use crate::{NodeCount, NodeIndex, Round, SessionId};
use std::{
    fmt::{Debug, Formatter},
    sync::Arc,
    time::Duration,
};

/// A function answering the question of how long to delay the n-th retry.
pub type DelaySchedule = Arc<dyn Fn(usize) -> Duration + Sync + Send + 'static>;

/// A function answering the question of how many nodes to query on the n-th (0-based) try.
pub type RecipientCountSchedule = Arc<dyn Fn(usize) -> usize + Sync + Send + 'static>;

/// Configuration of several parameters related to delaying various tasks.
#[derive(Clone)]
pub struct DelayConfig {
    /// Tick frequency of the Member. Governs internal task queue of the Member.
    pub tick_interval: Duration,
    /// Minimum frequency of broadcast of top known units. Units have to be at least this old to be
    /// rebroadcast at all.
    pub unit_rebroadcast_interval_min: Duration,
    /// Maximum frequency of broadcast of top known units.
    pub unit_rebroadcast_interval_max: Duration,
    /// unit_creation_delay(k) represents the delay between creating the (k-1)th and kth unit.
    pub unit_creation_delay: DelaySchedule,
    /// coord_request_delay(k) represents the delay between the kth and (k+1)st try when requesting
    /// a unit by coords.
    pub coord_request_delay: DelaySchedule,
    /// coord_request_recipients(k) represents the number of nodes to ask at the kth try when
    /// requesting a unit by coords.
    pub coord_request_recipients: RecipientCountSchedule,
    /// parent_request_delay(k) represents the delay between the kth and (k+1)st try when requesting
    /// unknown parents of a unit.
    pub parent_request_delay: DelaySchedule,
    /// parent_request_recipients(k) represents the number of nodes to ask at the kth try when
    /// requesting unknown parents of a unit.
    pub parent_request_recipients: RecipientCountSchedule,
    /// newest_request_delay(k) represents the delay between the kth and (k+1)st try when sending
    /// a broadcast request for newest units
    pub newest_request_delay: DelaySchedule,
}

impl Debug for DelayConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelayConfig")
            .field("tick interval", &self.tick_interval)
            .field(
                "min unit rebroadcast interval",
                &self.unit_rebroadcast_interval_min,
            )
            .field(
                "max unit rebroadcast interval",
                &self.unit_rebroadcast_interval_max,
            )
            .finish()
    }
}

/// Main configuration of the consensus. We refer to [the documentation](https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html#34-alephbft-sessions)
/// Section 3.4 for a discussion of some of these parameters and their significance.
#[derive(Clone, Debug)]
pub struct Config {
    /// Identification number of the Member=0,..,(n_members-1).
    pub node_ix: NodeIndex,
    /// Id of the session for which this instance is run.
    pub session_id: SessionId,
    /// The size of the committee running the consensus.
    pub n_members: NodeCount,
    /// Configuration of several parameters related to delaying various tasks.
    pub delay_config: DelayConfig,
    /// Maximum allowable round of a unit.
    pub max_round: Round,
}

pub fn exponential_slowdown(
    t: usize,
    base_delay: f64,
    start_exp_delay: usize,
    exp_base: f64,
) -> Duration {
    // This gives:
    // base_delay, for t <= start_exp_delay,
    // base_delay * exp_base^(t - start_exp_delay), for t > start_exp_delay.
    let delay = if t < start_exp_delay {
        base_delay
    } else {
        let power = t - start_exp_delay;
        base_delay * exp_base.powf(power as f64)
    };
    let delay = delay.round() as u64;
    // the above will make it u64::MAX if it exceeds u64
    Duration::from_millis(delay)
}

/// A default configuration of what the creators of this package see as optimal parameters.
pub fn default_config(n_members: NodeCount, node_ix: NodeIndex, session_id: SessionId) -> Config {
    Config {
        node_ix,
        session_id,
        n_members,
        delay_config: DelayConfig {
            tick_interval: Duration::from_millis(10),
            unit_rebroadcast_interval_min: Duration::from_millis(15000),
            unit_rebroadcast_interval_max: Duration::from_millis(20000),
            unit_creation_delay: default_unit_creation_delay(),
            coord_request_delay: default_coord_request_delay(),
            coord_request_recipients: default_coord_request_recipients(),
            parent_request_delay: Arc::new(|_| Duration::from_millis(3000)),
            parent_request_recipients: Arc::new(|_| 1),
            newest_request_delay: Arc::new(|_| Duration::from_millis(3000)),
        },
        max_round: 5000,
    }
}

/// 5000, 500, 500, 500, ... (till step 3000), 500, 500*1.005, 500*(1.005)^2, 500*(1.005)^3, ..., 10742207 (last step)
fn default_unit_creation_delay() -> DelaySchedule {
    Arc::new(|t| match t {
        0 => Duration::from_millis(5000),
        _ => exponential_slowdown(t, 500.0, 3000, 1.005),
    })
}

/// 0, 50, 1000, 3000, 6000, 9000, ...
fn default_coord_request_delay() -> DelaySchedule {
    Arc::new(|t| match t {
        0 => Duration::from_millis(0),
        1 => Duration::from_millis(50),
        2 => Duration::from_millis(1000),
        _ => Duration::from_millis(3000 * (t as u64 - 2)),
    })
}

/// 3, 3, 3, 1, 1, 1, 1, ...
fn default_coord_request_recipients() -> RecipientCountSchedule {
    Arc::new(|t| if t <= 2 { 3 } else { 1 })
}
