//! Dev-ergonomics test suite — backs every claim in spec 60 § 13 +
//! spec 72 § 7. Each `mod` below is one of the named test files in
//! that catalogue. Lint-fail / compile-error suite (`test_compile_errors`)
//! delegates to the trybuild fixtures in `obs-macros`; keeping the
//! pinned snapshots in one place avoids duplicating expected outputs.
//!
//! `#[cfg(feature = "test")]` is intentionally **not** required at the
//! file level — `obs-sdk`'s `dev-dependencies` enable `obs-core/test`,
//! so the `#[obs::test]` macro and `assert_emitted!` are always
//! available within `cargo test`.

#![allow(
    missing_docs,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing
)]

mod test_in_memory_observer;
mod test_multi_tenant_observer;
mod test_no_observer_noop;
mod test_obs_test_attribute;
mod test_parallel_tests;
mod test_registry_init;
mod test_scrubbed_envelope;
