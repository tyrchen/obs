//! L002: a PII-classified field cannot be a LABEL.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsBadPii {
    #[obs(label, cardinality = "low", classification = "pii")]
    pub email: String,
}

fn main() {}
