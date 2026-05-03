//! `obs::forensic!` + `obs::SpanTrace` quickstart.
//!
//! Spec 95 § 3.11 / D8-5 / P2-AI. Demonstrates two of the dev-erg
//! patterns that the other examples don't cover:
//!
//! 1. **`obs::forensic!`** — emergency emit with per-callsite rate limiting. Use it to capture a
//!    one-off snapshot when a normal structured event is too rigid (e.g. unexpected error path).
//! 2. **`obs::SpanTrace`** — snapshot of the active scope ancestry, rendered into an error type so
//!    the rendered chain reaches user-visible logs.
//!
//! Run: `cargo run -p obs-example-forensic-and-spantrace`

use std::sync::Arc;

use obs_core::ScopeFrameBuilder;
use obs_sdk::{FormatterStyle, Sink, SpanTrace, StandardObserver, StdoutSink, install_observer};

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    let stdout: Arc<dyn Sink> = Arc::new(StdoutSink::new(FormatterStyle::Pretty));
    let observer = StandardObserver::builder()
        .service(
            "obs-example-forensic-and-spantrace",
            env!("CARGO_PKG_VERSION"),
        )
        .sink_fallback(stdout)
        .build()?;
    install_observer(observer);

    println!("\n--- demo: obs::forensic! escape hatch ---");
    demo_forensic();

    println!("\n--- demo: obs::SpanTrace inside scopes ---");
    demo_spantrace();

    println!("\n--- demo: forensic budget enforcement ---");
    demo_budget();

    Ok(())
}

fn demo_forensic() {
    obs_sdk::forensic!(
        site = "demo.unexpected_branch",
        message = "fell into the unreachable path of the request router",
        {
            "method" => "POST".to_string(),
            "path" => "/v1/widgets".to_string(),
        }
    );
}

fn demo_spantrace() {
    let outer = ScopeFrameBuilder::new()
        .label("tenant", "alpha")
        .into_frame();
    obs_core::scope::push_frame_pub(outer);

    let inner = ScopeFrameBuilder::new()
        .label("operation", "fetch_user")
        .into_frame();
    obs_core::scope::push_frame_pub(inner);

    let st = SpanTrace::capture();
    println!("captured SpanTrace: {st}");

    let _ = obs_core::scope::pop_frame_pub();
    let _ = obs_core::scope::pop_frame_pub();
}

fn demo_budget() {
    for i in 0..20 {
        obs_sdk::forensic!(
            site = "demo.budget_loop",
            message = "rate-limited forensic burst",
            { "iteration" => format!("{i}") }
        );
    }
    println!("emitted 20 forensic calls; observe the budget-exceeded self-event in stdout");
}
