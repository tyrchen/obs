//! L001: a LABEL field cannot have High or Unbounded cardinality.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsBadLabel {
    #[obs(label, cardinality = "high")]
    pub user_id: String,
}

fn main() {}
