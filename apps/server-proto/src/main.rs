//! Phase-2 demo binary that exercises the proto-first authoring path
//! end-to-end. Reads `proto/myapp/v1/events.proto`, runs `obs-build`
//! in `build.rs`, and emits one `ObsHelloEmitted` event via the
//! generated builder.

#![allow(missing_docs)]

use std::sync::Arc;

use obs_sdk::{Emit, FormatterStyle, StandardObserver, StdoutSink, install_observer};

obs_sdk::include_schemas!("myapp.v1");

fn main() -> anyhow::Result<()> {
    let observer = StandardObserver::builder()
        .service("obs-server-proto", env!("CARGO_PKG_VERSION"))
        .instance("local")
        .sink_fallback(Arc::new(StdoutSink::new(FormatterStyle::Full)))
        .build()?;
    install_observer(observer);

    // Builder + emit is generated for every annotated proto message.
    myapp::v1::ObsHelloEmitted::builder().who("world").emit();

    // Direct emit via .emit() on the buffa message also works because
    // `impl EventSchema` is generated for it.
    let evt = myapp::v1::ObsRequestCompleted {
        route: "list_users".to_string(),
        status: "ok".to_string(),
        latency_ms: 42,
        ..Default::default()
    };
    evt.emit();
    Ok(())
}
