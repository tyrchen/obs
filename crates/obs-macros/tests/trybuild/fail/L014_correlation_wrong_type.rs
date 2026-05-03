//! L014: TRACE_ID / SPAN_ID / PARENT_SPAN_ID fields must have proto
//! type `string`. Spec 95 § 2.2.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsCheckoutStarted {
    #[obs(trace_id)]
    pub trace_id: Vec<u8>,
}

fn main() {}
