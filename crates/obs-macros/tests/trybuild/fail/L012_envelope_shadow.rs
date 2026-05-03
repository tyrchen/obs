//! L012: field name must not shadow envelope-reserved fields.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsShadowsTsNs {
    #[obs(attribute)]
    pub ts_ns: u64,
}

fn main() {}
