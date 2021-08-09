use crate::{Round, SessionId};
use std::{sync::Arc, time::Duration};

use crate::nodes::{NodeCount, NodeIndex};

pub type DelaySchedule = Arc<dyn Fn(usize) -> Duration + Sync + Send + 'static>;

/// Configuration of several parameters related to delaying various tasks.
#[derive(Clone)]
pub struct DelayConfig {
    /// Tick frequency of the Member. Govers internal task queue of the Member.
    pub tick_interval: Duration,
    /// After what delay, we repeat a request for a coord or parents.
    pub requests_interval: Duration,
    /// DelaySchedule(k) represents the delay between the kth and (k+1)th broadcast.
    pub unit_broadcast_delay: DelaySchedule,
    /// DelaySchedule(k) represents the delay between creating the (k-1)th and kth unit.
    pub unit_creation_delay: DelaySchedule,
}

/// Main configuration of the consensus. We refer to the documentation https://cardinal-cryptography.github.io/AlephBFT/aleph_bft_api.html
/// Section 3.4 for a discussion of some of these parameters and their significance.
#[derive(Clone)]
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
    let unit_creation_delay = Arc::new(|t| {
        if t == 0 {
            Duration::from_millis(5000)
        } else {
            exponential_slowdown(t, 500.0, 3000, 1.005)
        }
    });
    let delay_config = DelayConfig {
        tick_interval: Duration::from_millis(100),
        requests_interval: Duration::from_millis(3000),
        unit_broadcast_delay: Arc::new(|t| exponential_slowdown(t, 4000.0, 0, 2.0)),
        // 4000, 8000, 16000, 32000, ...
        unit_creation_delay,
        // 5000, 500, 500, 500, ... (till step 3000), 500, 500*1.005, 500*(1.005)^2, 500*(1.005)^3, ..., 10742207 (last step)
    };
    Config {
        node_ix,
        session_id,
        n_members,
        delay_config,
        max_round: 5000,
    }
}
