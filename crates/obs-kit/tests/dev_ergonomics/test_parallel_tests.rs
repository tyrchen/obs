//! `test_parallel_tests` — runs multiple `#[obs::test]`s in parallel
//! and asserts each test sees only its own events (no cross-thread
//! contamination via the per-thread observer slot). Spec 72 § 7.

use obs_kit::{Emit, Event};

#[derive(Debug, Default, Event)]
#[event(tier = "log", default_sev = "info")]
pub struct ObsParallelProbe {
    #[obs(label, cardinality = "low")]
    pub thread_label: String,
}

macro_rules! parallel_case {
    ($name:ident, $label:literal) => {
        #[obs_kit::test::test]
        fn $name() {
            // A small busy loop / sleep + emit pattern simulates the
            // realistic case where one test is doing work while
            // another emits at the same time.
            for _ in 0..16 {
                ObsParallelProbe {
                    thread_label: $label.to_string(),
                }
                .emit();
            }
            // Every captured envelope should carry our label, and no
            // other label values from sibling tests leaked into ours.
            let drained = obs_kit::test::take_emitted();
            assert!(
                !drained.is_empty(),
                "expected at least one captured envelope"
            );
            for env in &drained {
                assert_eq!(
                    env.labels.get("thread_label").map(String::as_str),
                    Some($label),
                    "found foreign thread_label on a captured envelope"
                );
            }
        }
    };
}

parallel_case!(test_parallel_alpha, "alpha");
parallel_case!(test_parallel_beta, "beta");
parallel_case!(test_parallel_gamma, "gamma");
parallel_case!(test_parallel_delta, "delta");
parallel_case!(test_parallel_epsilon, "epsilon");
parallel_case!(test_parallel_zeta, "zeta");
parallel_case!(test_parallel_eta, "eta");
parallel_case!(test_parallel_theta, "theta");
