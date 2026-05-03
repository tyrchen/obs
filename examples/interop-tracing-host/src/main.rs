//! Example: tracing-host app, internal obs library.
//!
//! Story: I have an existing service that uses `tracing-subscriber`.
//! I'm adopting an internal library (`payments_lib` below) that emits
//! obs typed events. I don't want to switch my whole observability
//! stack — I just want the obs events to land in my existing tracing
//! pipeline alongside everything else.
//!
//! Wiring:
//! - The host installs `tracing_subscriber::fmt()` first — this is the pre-existing telemetry setup
//!   that the rest of the app already speaks.
//! - The obs observer is built with `ObsToTracingSink::new()` as its ONLY sink. Every typed obs
//!   emit becomes a `tracing::Event` on the global dispatcher and reaches the same fmt subscriber.
//! - Net result: one log stream, two emit styles, zero extra plumbing.

#![allow(missing_docs)]

mod schema;

use std::sync::Arc;

use anyhow::{Context, Result};
use obs_sdk::{Sink, StandardObserver, install_observer, observer};
use obs_tracing_bridge::ObsToTracingSink;
use tracing_subscriber::EnvFilter;

/// Pretends to be the obs-typed internal library that the host has
/// just adopted. It speaks obs (`builder().emit()`); the host speaks
/// tracing. The bridge stitches them together at the sink.
mod payments_lib {
    use crate::schema::payments;

    pub fn authorize(payment_id: &str, merchant_id: &str, amount_micros: u64, card_brand: &str) {
        payments::v1::ObsPaymentAuthorized::builder()
            .payment_id(payment_id.to_string())
            .merchant_id(merchant_id.to_string())
            .amount_micros(amount_micros)
            .card_brand(card_brand.to_string())
            .emit();
    }

    pub fn decline(payment_id: &str, merchant_id: &str, reason: &str) {
        // `.emit()` (no `.emit_at`) — the proto sets
        // `default_sev: SEVERITY_WARN` so the bridge dispatches this
        // to `tracing::Level::WARN` automatically.
        payments::v1::ObsPaymentDeclined::builder()
            .payment_id(payment_id.to_string())
            .merchant_id(merchant_id.to_string())
            .decline_reason(reason.to_string())
            .emit();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // 1) Host's pre-existing telemetry: tracing_subscriber::fmt with
    // env-filter and target column on. RUST_LOG is honoured.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .init();

    // 2) Obs observer whose ONLY sink is the obs→tracing bridge. Every
    // typed obs emit becomes a `tracing::Event` on the global
    // dispatcher and reaches the fmt subscriber installed above.
    let bridge: Arc<dyn Sink> = Arc::new(ObsToTracingSink::new());
    let obs = StandardObserver::builder()
        .service(
            "obs-example-interop-tracing-host",
            env!("CARGO_PKG_VERSION"),
        )
        .sink_fallback(bridge)
        .build()
        .context("build StandardObserver with ObsToTracingSink")?;
    install_observer(obs);

    // 3) Host code is a normal tracing user.
    tracing::info!(target: "host::startup", "service ready");

    // 4) Internal library is an obs user. Its emit appears in the same
    // fmt output as the host's tracing call above — different authoring
    // style, identical destination.
    payments_lib::authorize("pay-7001", "merch-acme", 4_999_000, "visa");

    tracing::warn!(
        target: "host::policy",
        merchant = "merch-acme",
        "rate-limit exceeded; declining further charges this minute"
    );

    payments_lib::decline("pay-7002", "merch-acme", "rate_limited");

    // Drain the obs observer so the bridged events all land before
    // process exit. The fmt subscriber flushes on its own as drop runs.
    observer().shutdown().await;
    Ok(())
}
