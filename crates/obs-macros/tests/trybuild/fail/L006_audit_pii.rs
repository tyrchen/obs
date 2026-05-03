//! L006: AUDIT-tier event cannot carry PII or SECRET fields.

use obs_macros::Event;

#[derive(Debug, Default, Event)]
#[event(tier = "audit", default_sev = "info")]
pub struct ObsAuditWithPii {
    #[obs(attribute, classification = "pii")]
    pub email: String,
}

fn main() {}
