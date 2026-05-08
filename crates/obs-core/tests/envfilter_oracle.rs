//! Oracle test: every fixture is parsed by both `obs_core::Filter`
//! and `tracing_subscriber::EnvFilter` and the two are asked the same
//! interest question for a curated callsite set. They must agree.
//!
//! Spec 13 § 7 promises *parity* with `EnvFilter` so operators can
//! copy a `RUST_LOG` directive into `OBS_FILTER`. Spec 95 § 3.7 / D8-2
//! / P2-AE wires that promise to a regression test.
//!
//! The oracle uses `EnvFilter::would_enable(target, level)` which is
//! the same API `tracing-subscriber::registry` consults at dispatch
//! time. The obs side calls `Filter::callsite_interest` and treats
//! `Always`/`Sometimes` as "enabled" and `Never` as "disabled" — this
//! mirrors how the runtime gates emits.

#![allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic
)]

use obs_core::{Filter as ObsFilter, ObsCallsite};
use obs_proto::obs::v1::Severity;
use tracing::Level;
use tracing_core::Subscriber;
use tracing_subscriber::EnvFilter;

/// One callsite in the curated probe set.
#[derive(Debug, Clone, Copy)]
struct Probe {
    full_name: &'static str,
    target: &'static str,
    sev: Severity,
}

const PROBES: &[Probe] = &[
    // Bare module — info severity.
    Probe {
        full_name: "myapp.v1.ObsRequestStarted",
        target: "myapp::auth",
        sev: Severity::Info,
    },
    // Bare module — debug severity.
    Probe {
        full_name: "myapp.v1.ObsRequestCompleted",
        target: "myapp::auth",
        sev: Severity::Debug,
    },
    // Different module.
    Probe {
        full_name: "myapp.v1.ObsCacheHit",
        target: "myapp::cache",
        sev: Severity::Info,
    },
    // Other crate.
    Probe {
        full_name: "otherapp.v1.ObsRouted",
        target: "otherapp::router",
        sev: Severity::Info,
    },
    // Warn from auth.
    Probe {
        full_name: "myapp.v1.ObsAuthFailed",
        target: "myapp::auth",
        sev: Severity::Warn,
    },
    // Trace from cache (always-low).
    Probe {
        full_name: "myapp.v1.ObsCacheTrace",
        target: "myapp::cache",
        sev: Severity::Trace,
    },
];

// Documented subset (filter.rs module docs): bare level, bare veto,
// target=level, target=off, multiple directives.
//
// EnvFilter has a quirk: a single target-specific directive
// (`my_mod=trace`) implicitly disables every other target — there's
// no implicit `info` floor. obs treats target directives as additive
// over a default `Info` floor (spec 13 § 7). The two agree once a
// bare floor is also stated, so every fixture here pairs the target
// directives with an explicit floor.
const FIXTURES: &[&str] = &[
    "info",
    "debug",
    "warn",
    "off",
    "info,myapp::auth=debug",
    "warn,myapp::cache=trace",
    "info,myapp::auth=off",
    "info,myapp::cache=off",
    "trace,myapp::auth=warn",
    "off,myapp::auth=info",
];

fn obs_enabled(filter: &ObsFilter, probe: &Probe) -> bool {
    let cs = ObsCallsite::new(probe.full_name, probe.sev, probe.target, "test.rs", 1);
    use obs_core::callsite::Interest;
    matches!(
        filter.callsite_interest(&cs),
        Interest::Always | Interest::Sometimes
    )
}

fn ts_enabled(filter: &EnvFilter, probe: &Probe) -> bool {
    let level = match probe.sev {
        Severity::Trace => Level::TRACE,
        Severity::Debug => Level::DEBUG,
        Severity::Info => Level::INFO,
        Severity::Warn => Level::WARN,
        _ => Level::ERROR,
    };
    let metadata = stub_metadata(probe.target, level);
    // Wrap EnvFilter in a minimal subscriber and consult its
    // `enabled` decision via the tracing dispatcher API. EnvFilter
    // implements `Subscriber` directly when paired with a Layer; the
    // simplest oracle is to layer it into a dummy registry and check
    // the resulting Subscriber's `enabled`.
    let subscriber = tracing_subscriber::registry::Registry::default();
    use tracing_subscriber::layer::SubscriberExt;
    let layered = subscriber.with(filter.clone());
    Subscriber::enabled(&layered, metadata)
}

/// Build a static `Metadata` whose target/level reflect the probe.
/// We can't easily construct one outside `tracing_core`'s callsite
/// machinery; for the oracle, build a tiny inline callsite per probe
/// via `tracing::callsite!`. Returns `&'static Metadata`.
fn stub_metadata(target: &'static str, level: Level) -> &'static tracing_core::Metadata<'static> {
    use std::{
        collections::HashMap,
        sync::{Mutex, OnceLock},
    };

    use tracing_core::{Metadata, callsite::Identifier, field::FieldSet};
    struct StubCallsite {
        meta: Metadata<'static>,
    }
    impl tracing_core::Callsite for StubCallsite {
        fn set_interest(&self, _: tracing_core::Interest) {}
        fn metadata(&self) -> &Metadata<'_> {
            &self.meta
        }
    }
    static CACHE: OnceLock<Mutex<HashMap<(&'static str, u8), &'static StubCallsite>>> =
        OnceLock::new();
    let level_key = match level {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    };
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut g = cache.lock().expect("lock");
    if let Some(cs) = g.get(&(target, level_key)) {
        return &cs.meta;
    }
    static FIELDS: [&str; 1] = ["message"];
    // We need a stable static `Callsite`; leak one per (target, level)
    // pair. The leak is fine for tests.
    let cs_box: Box<StubCallsite> = Box::new(StubCallsite {
        meta: Metadata::new(
            "stub_event",
            target,
            level,
            None,
            None,
            None,
            FieldSet::new(&FIELDS, Identifier(&PLACEHOLDER_CS)),
            tracing_core::Kind::EVENT,
        ),
    });
    let cs_static: &'static StubCallsite = Box::leak(cs_box);
    tracing_core::callsite::register(cs_static);
    g.insert((target, level_key), cs_static);
    &cs_static.meta
}

struct Placeholder;
impl tracing_core::Callsite for Placeholder {
    fn set_interest(&self, _: tracing_core::Interest) {}
    fn metadata(&self) -> &tracing_core::Metadata<'_> {
        static FIELDS: [&str; 0] = [];
        // This is only used for the FieldSet identifier and is never
        // actually dispatched.
        static M: std::sync::OnceLock<tracing_core::Metadata<'static>> = std::sync::OnceLock::new();
        M.get_or_init(|| {
            tracing_core::Metadata::new(
                "placeholder",
                "placeholder",
                Level::TRACE,
                None,
                None,
                None,
                tracing_core::field::FieldSet::new(
                    &FIELDS,
                    tracing_core::callsite::Identifier(&PLACEHOLDER_CS),
                ),
                tracing_core::Kind::EVENT,
            )
        })
    }
}
static PLACEHOLDER_CS: Placeholder = Placeholder;

#[test]
fn obs_filter_should_match_envfilter_for_documented_subset() {
    for fixture in FIXTURES {
        let obs = ObsFilter::parse(fixture).unwrap_or_else(|e| {
            panic!("obs_core::Filter rejected `{fixture}`: {e}");
        });
        let ts: EnvFilter = fixture.parse().unwrap_or_else(|e| {
            panic!("tracing_subscriber::EnvFilter rejected `{fixture}`: {e}");
        });

        for probe in PROBES {
            let obs_enabled = obs_enabled(&obs, probe);
            let ts_enabled = ts_enabled(&ts, probe);
            assert_eq!(
                obs_enabled, ts_enabled,
                "filter `{}` for probe {probe:?} — obs={obs_enabled} ts={ts_enabled}",
                fixture,
            );
        }
    }
}
