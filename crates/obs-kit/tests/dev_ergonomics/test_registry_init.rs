//! `test_registry_init` — observer init walks `EVENT_SCHEMAS`; asserts
//! every `ObsXxx` defined in this crate appears in the registry's
//! `by_name`/`by_hash` indices. Spec 72 § 7.

use obs_kit::{Event, EventSchema};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsRegistryProbe {
    #[obs(label, cardinality = "low")]
    pub kind: String,
}

#[test]
fn test_registry_should_index_local_event_by_name_and_hash() {
    // The macro registers a `&dyn EventSchemaErased` into the linkme
    // distributed slice at link time. `from_link_section` walks it.
    let registry = obs_core::SchemaRegistry::from_link_section();
    let by_name: std::collections::HashSet<&'static str> = registry
        .iter()
        .map(obs_core::EventSchemaErased::full_name)
        .collect();
    assert!(
        by_name.contains(ObsRegistryProbe::FULL_NAME),
        "registry missing local event {} (saw {} entries)",
        ObsRegistryProbe::FULL_NAME,
        by_name.len()
    );

    // Build a minimal envelope with this event's hash + name and
    // confirm `lookup` resolves.
    let env = obs_proto::obs::v1::ObsEnvelope {
        full_name: ObsRegistryProbe::FULL_NAME.to_string(),
        schema_hash: ObsRegistryProbe::SCHEMA_HASH,
        ..Default::default()
    };
    let resolved = registry.lookup(&env).expect("lookup hit");
    assert_eq!(resolved.full_name(), ObsRegistryProbe::FULL_NAME);
    assert_eq!(resolved.schema_hash(), ObsRegistryProbe::SCHEMA_HASH);
}
