//! `test_hot_reload` (spec 72 § 7) — `Observer::reload_filter()` bumps
//! the generation, which makes the next emit re-probe the
//! `ObsCallsite::enabled` cache.

use std::sync::Arc;

use obs_kit::{Filter, Observer, StandardObserver, Tier};

#[test]
fn test_should_bump_generation_on_reload() {
    let observer = StandardObserver::builder()
        .service("hot-reload", "0.0.0")
        .filter("info")
        .spawn_workers(false)
        .sink_for(Tier::Log, Arc::new(obs_kit::InMemorySink::new()))
        .build()
        .unwrap();
    let g0 = observer.generation();
    observer.reload_filter();
    let g1 = observer.generation();
    assert!(g1 > g0, "reload_filter must bump generation");
}

#[test]
fn test_filter_parse_should_route_via_default_level() {
    let f: Filter = "warn".parse().unwrap();
    assert!(f.default_level() >= obs_kit::Severity::Warn);
}
