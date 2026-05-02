//! Smoke test for `#[derive(Event)]`. Drives the derive macro through
//! the obs-macros crate and asserts the EventSchema impl was emitted
//! correctly. Trybuild fixtures for the lint failures live under
//! `crates/obs-macros/tests/trybuild`.

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::expect_used
)]

use std::sync::Arc;

use obs_core::{
    Cardinality, Classification, Emit, EventSchema, FieldRole, InMemoryObserver, Observer,
    Severity, Tier, with_test_observer,
};

#[derive(Debug, Default, obs_macros::Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsHelloEmitted {
    #[obs(label, cardinality = "low")]
    pub who: String,
}

#[test]
fn test_derive_should_set_associated_consts() {
    // FULL_NAME is `<crate>.v1.<TypeName>` — the obs-macros test crate
    // is built with CARGO_PKG_NAME=obs-macros (normalised to obs_macros).
    assert!(ObsHelloEmitted::FULL_NAME.ends_with(".ObsHelloEmitted"));
    assert!(ObsHelloEmitted::FULL_NAME.contains(".v1."));
    assert_eq!(ObsHelloEmitted::TIER, Tier::Log);
    assert_eq!(ObsHelloEmitted::DEFAULT_SEV, Severity::Info);
    assert_eq!(ObsHelloEmitted::FIELDS.len(), 1);
    assert_eq!(ObsHelloEmitted::FIELDS[0].name, "who");
    assert_eq!(ObsHelloEmitted::FIELDS[0].role, FieldRole::Label);
    assert_eq!(ObsHelloEmitted::FIELDS[0].cardinality, Cardinality::Low);
    assert_eq!(
        ObsHelloEmitted::FIELDS[0].classification,
        Classification::Internal
    );
    // Schema hash should be stable and non-zero.
    assert_ne!(ObsHelloEmitted::SCHEMA_HASH, 0);
}

#[test]
fn test_derive_builder_should_emit_through_observer() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    with_test_observer(observer, || {
        ObsHelloEmitted::builder().who("world").emit();
    });
    let drained = handle.drain();
    assert_eq!(drained.len(), 1);
    // L011 default prefix uses CARGO_PKG_NAME; obs-macros's own test
    // sees "obs_macros" as the package, so FULL_NAME is "obs_macros.v1.ObsHelloEmitted".
    assert!(drained[0].full_name.ends_with("ObsHelloEmitted"));
    assert_eq!(drained[0].labels.get("who"), Some(&"world".to_string()));
}

#[test]
fn test_derive_emit_at_should_override_severity() {
    let observer = InMemoryObserver::new();
    let handle = observer.handle();
    let observer: Arc<dyn Observer> = Arc::new(observer);
    with_test_observer(observer, || {
        ObsHelloEmitted {
            who: "world".to_string(),
        }
        .emit_at(Severity::Warn);
    });
    let drained = handle.drain();
    assert_eq!(drained.len(), 1);
    // The wire `sev` must reflect the override (proto-side enum):
    assert!(matches!(
        drained[0].sev,
        ::buffa::EnumValue::Known(::obs_proto::obs::v1::Severity::SEVERITY_WARN)
    ));
}
