//! L007: field names must be snake_case.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsCamelCaseField {
    #[obs(label, cardinality = "low")]
    pub UserId: String,
}

fn main() {}
