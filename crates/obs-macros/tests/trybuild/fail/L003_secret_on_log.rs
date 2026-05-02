//! L003: a SECRET-classified field cannot live on LOG or AUDIT tier.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsBadSecret {
    #[obs(attribute, classification = "secret")]
    pub api_key: String,
}

fn main() {}
