//! Fluent builder on top of [`obs_core::self_event`] for multi-label
//! self-events. Spec § 5.3.

use obs_core::{ObsEnvelope, Severity, Tier, observer, self_event};

/// Fluent builder for runtime-style self-events.
///
/// Sinks, middleware, and tok's own init paths emit labels-only
/// self-events (`ObsConfigReloaded`, `ObsBatchSinkUploaded`, …). The
/// plain [`obs_core::self_event`] helper returns a bare envelope that
/// the caller mutates; the builder wraps that in a fluent chain for
/// call sites that set two or more labels.
///
/// ```no_run
/// use obs_kit::SelfEventBuilder;
/// use obs_kit::{Severity, Tier};
///
/// SelfEventBuilder::new("mylib.v1.WorkerRestart", Tier::Log, Severity::Warn)
///     .label("reason", "timeout")
///     .label("worker_id", "42")
///     .emit();
/// ```
#[derive(Debug)]
pub struct SelfEventBuilder {
    env: ObsEnvelope,
}

impl SelfEventBuilder {
    /// Start a new builder with the given `full_name`, `tier`, and
    /// `sev`. `sampling_reason` is set to `SAMPLING_REASON_RUNTIME`
    /// and `ts_ns` to the current wall-clock via
    /// [`obs_core::self_event`].
    #[must_use]
    pub fn new(full_name: &str, tier: Tier, sev: Severity) -> Self {
        Self {
            env: self_event(full_name, tier, sev),
        }
    }

    /// Insert (or replace) a label.
    #[must_use]
    pub fn label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.env.labels.insert(key.into(), value.into());
        self
    }

    /// Extend the envelope with an iterator of `(key, value)` pairs.
    /// Useful when the labels come from a pre-built
    /// `BTreeMap`/`HashMap`.
    #[must_use]
    pub fn labels<I, K, V>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (k, v) in iter {
            self.env.labels.insert(k.into(), v.into());
        }
        self
    }

    /// Emit the envelope through the active observer.
    pub fn emit(self) {
        observer().emit_envelope(self.env);
    }

    /// Return the built envelope without emitting. Useful for tests
    /// and for call sites that want to attach a payload before
    /// handing the envelope to a sink manually.
    #[must_use]
    pub fn build(self) -> ObsEnvelope {
        self.env
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_collect_labels() {
        let env = SelfEventBuilder::new("obs.runtime.v1.ObsTest", Tier::Log, Severity::Info)
            .label("k1", "v1")
            .label("k2", "v2")
            .build();
        assert_eq!(env.full_name, "obs.runtime.v1.ObsTest");
        assert_eq!(env.labels.get("k1"), Some(&"v1".to_string()));
        assert_eq!(env.labels.get("k2"), Some(&"v2".to_string()));
    }

    #[test]
    fn test_should_extend_from_iter() {
        let env = SelfEventBuilder::new("obs.runtime.v1.ObsTest", Tier::Log, Severity::Info)
            .labels([("a", "1"), ("b", "2")])
            .build();
        assert_eq!(env.labels.get("a"), Some(&"1".to_string()));
        assert_eq!(env.labels.get("b"), Some(&"2".to_string()));
    }
}
