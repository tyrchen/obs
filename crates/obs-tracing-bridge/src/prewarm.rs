//! Pre-warm registry — built-in well-known third-party callsites that
//! `TracingToObsLayer::with_prewarm(true)` registers at observer init.
//!
//! Spec 31 § 3.3.

use obs_proto::obs::v1::Severity;

/// One pre-warm record.
#[derive(Debug, Clone, Copy)]
pub struct PrewarmEntry {
    /// Tracing target (`sqlx::query`, `tower_http::trace::on_response`).
    pub target: &'static str,
    /// Anchor / canonical short name (we don't have a real source path
    /// for third-party crates; we use the crate name as anchor).
    pub anchor: &'static str,
    /// Effective severity / level for the synthesised registration.
    pub level: Severity,
    /// Field names users typically promote.
    pub field_names: &'static [&'static str],
}

/// Curated list. PRs add entries as the ecosystem evolves. Items are
/// kept in target-prefix order to make duplicates obvious in review.
pub const PREWARM_CALLSITES: &[PrewarmEntry] = &[
    PrewarmEntry {
        target: "axum::rejection",
        anchor: "axum",
        level: Severity::Warn,
        field_names: &["error"],
    },
    PrewarmEntry {
        target: "h2::codec",
        anchor: "h2",
        level: Severity::Trace,
        field_names: &[],
    },
    PrewarmEntry {
        target: "hyper::client::connect",
        anchor: "hyper",
        level: Severity::Trace,
        field_names: &[],
    },
    PrewarmEntry {
        target: "reqwest::async_impl::client",
        anchor: "reqwest",
        level: Severity::Debug,
        field_names: &["status", "url"],
    },
    PrewarmEntry {
        target: "sqlx::query",
        anchor: "sqlx",
        level: Severity::Debug,
        field_names: &["rows_affected", "elapsed_secs", "summary"],
    },
    PrewarmEntry {
        target: "tower_http::trace::on_request",
        anchor: "tower-http",
        level: Severity::Debug,
        field_names: &["uri", "method"],
    },
    PrewarmEntry {
        target: "tower_http::trace::on_response",
        anchor: "tower-http",
        level: Severity::Info,
        field_names: &["status", "latency"],
    },
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prewarm_entries_should_be_unique_by_target() {
        let mut seen = std::collections::HashSet::new();
        for e in PREWARM_CALLSITES {
            assert!(seen.insert(e.target), "duplicate target: {}", e.target);
        }
    }
}
