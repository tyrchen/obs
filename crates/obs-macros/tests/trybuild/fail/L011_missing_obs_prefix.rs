//! L011: every event type name must start with `Obs` (default workspace
//! prefix per spec 10 § 7).

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct CheckoutStarted {
    #[obs(label, cardinality = "low")]
    pub status: String,
}

fn main() {}
