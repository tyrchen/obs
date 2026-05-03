//! L009: an event must declare at least one field.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsEmptyEvent {}

fn main() {}
