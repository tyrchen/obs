//! L004: a MEASUREMENT field must declare a metric kind.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsBadMeasurement {
    #[obs(measurement)]
    pub latency_ms: u64,
}

fn main() {}
