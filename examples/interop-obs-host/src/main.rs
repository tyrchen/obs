//! Example: obs-first app, 3rd-party tracing visible.
//!
//! Story: I'm building a new service with obs typed events. My deps
//! (`hyper`, `reqwest`, `tower`, etc.) emit via `tracing::*`. I want a
//! single observability pipeline so those bridged emits land in the
//! same observer as my typed events.
//!
//! Wiring:
//! - `StandardObserver` with a pretty stdout fallback sink (the primary sink in this minimal demo).
//! - `obs_tracing_bridge::init(...)` installs a global `tracing-subscriber` whose
//!   `TracingToObsLayer` forwards every `tracing::*` event to the active obs observer.
//! - We then emit a typed `ObsOrderPlaced`, simulate a few `tracing::*` calls that a 3rd-party
//!   crate (hyper) would normally make, then emit `ObsOrderShipped`.

#![allow(missing_docs)]

mod schema;

use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use obs_sdk::{
    FormatterStyle, Sink, StandardObserver, StdoutSink, install_observer, observer, scope,
};

use crate::schema::orders;

#[derive(Debug, Parser)]
#[command(
    name = "obs-example-interop-obs-host",
    about = "obs-first app that bridges 3rd-party tracing into the same obs pipeline."
)]
struct Cli {
    /// Install the tracing→obs bridge (default true). Pass `--no-bridge`
    /// to see what happens when 3rd-party `tracing::*` calls have no
    /// home: the typed obs emits still land in stdout, but the
    /// `tracing::*` lines vanish entirely.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    bridge: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    install_obs_observer()?;

    if cli.bridge {
        // Wire `tracing::*` → obs. The directive list mirrors what a
        // real app would set: `info` baseline, plus targeted bumps for
        // the 3rd-party crates we care about.
        obs_tracing_bridge::init::<&str>(Some("info,hyper=info,reqwest=info"))
            .map_err(|e| anyhow::anyhow!("install tracing bridge: {e}"))?;
        eprintln!("tracing→obs bridge installed");
    } else {
        eprintln!("tracing→obs bridge DISABLED — 3rd-party emits will be dropped");
    }

    // One scope frame for the whole demo flow. Anything emitted while
    // it's alive — typed obs events AND bridged tracing events — picks
    // up `request_id = "demo-001"` as a label.
    let _scope = scope!(request_id = "demo-001");

    // 1) Typed obs emit (the codegen builder for orders.v1.ObsOrderPlaced).
    orders::v1::ObsOrderPlaced::builder()
        .order_id("ord-1001".to_string())
        .customer_id("cust-42".to_string())
        .total_micros(12_345_000_u64)
        .currency("USD".to_string())
        .emit();

    // 2) Simulated 3rd-party `tracing::*` emits. In a real app these
    // come from `hyper::client::pool`, `reqwest::connect`, `tower::*`,
    // etc. once the bridge above is installed — every existing
    // `tracing::info!`/`warn!` in your dependency graph gets forwarded
    // to the obs observer with no code changes in those crates.
    tracing::info!(
        target: "hyper::client::pool",
        host = "api.example.com",
        port = 443,
        "checked out connection"
    );
    tracing::warn!(
        target: "hyper::client::pool",
        host = "api.example.com",
        idle_secs = 30u64,
        "connection idle timeout; recycling"
    );
    tracing::info!(
        target: "reqwest::connect",
        host = "api.example.com",
        scheme = "https",
        "established TLS connection"
    );

    // 3) Second typed emit at the end of the flow.
    orders::v1::ObsOrderShipped::builder()
        .order_id("ord-1001".to_string())
        .carrier("ups".to_string())
        .tracking_url("https://tracking.example.com/ord-1001".to_string())
        .emit();

    drop(_scope);

    // Drain queued events before exit so the stdout sink writes
    // everything out.
    observer().shutdown().await;
    Ok(())
}

fn install_obs_observer() -> Result<()> {
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));
    let observer = StandardObserver::builder()
        .service("obs-example-interop-obs-host", env!("CARGO_PKG_VERSION"))
        .sink_fallback(stdout)
        .build()
        .context("build StandardObserver")?;
    install_observer(observer);
    Ok(())
}
