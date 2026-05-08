//! Phase-1 demo binary (impl-plan task 1.13).
//!
//! Wires up a `StandardObserver` with a `StdoutSink(FormatterStyle::Full)`
//! fallback and emits one `ObsHelloEmitted` event. This is the
//! end-to-end exit criterion for the M0 milestone.

#![allow(missing_docs)] // single-purpose demo binary

use std::sync::Arc;

use obs_kit::{Event, FormatterStyle, StandardObserver, StdoutSink, install_observer};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
struct ObsHelloEmitted {
    #[obs(label, cardinality = "low")]
    who: String,
}

fn main() -> anyhow::Result<()> {
    let observer = StandardObserver::builder()
        .service("obs-server", env!("CARGO_PKG_VERSION"))
        .instance("local")
        .sink_fallback(Arc::new(StdoutSink::new(FormatterStyle::Full)))
        .build()?;
    install_observer(observer);

    ObsHelloEmitted::builder().who("world").emit();
    Ok(())
}
