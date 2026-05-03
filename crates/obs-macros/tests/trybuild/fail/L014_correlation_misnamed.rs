//! L014: TRACE_ID / SPAN_ID / PARENT_SPAN_ID kind fields must be named
//! with the matching envelope slot. Spec 95 § 2.2.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsCheckoutStarted {
    #[obs(trace_id)]
    pub trc_id: String,
}

fn main() {}
