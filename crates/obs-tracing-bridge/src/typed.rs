//! `TypedMatcher` — predicate over `tracing::Metadata` plus optional
//! field-presence requirements. Spec 30 § 2.5.

use regex::Regex;
use tracing::Level;
use tracing_core::Metadata;

/// Predicate. The matcher is configured through chained methods; the
/// expected uses match the spec table:
///
/// - Match a target prefix (`target("tower_http::trace::on_response")`) to lift access logs into a
///   typed event.
/// - Match `level_at_least(Level::ERROR)` + `field("error")` to lift any error-level tracing event.
#[derive(Debug, Clone, Default)]
pub struct TypedMatcher {
    target_eq: Option<&'static str>,
    target_prefix: Option<&'static str>,
    target_regex: Option<Regex>,
    name_eq: Option<&'static str>,
    level_min: Option<Level>,
    require_fields: Vec<&'static str>,
}

impl TypedMatcher {
    /// New empty matcher (matches everything).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Match exact target.
    #[must_use]
    pub fn target(mut self, t: &'static str) -> Self {
        self.target_eq = Some(t);
        self
    }

    /// Match target by prefix (e.g. `"sqlx::"`).
    #[must_use]
    pub fn target_prefix(mut self, t: &'static str) -> Self {
        self.target_prefix = Some(t);
        self
    }

    /// Match target by regex.
    ///
    /// # Errors
    ///
    /// Returns `regex::Error` when the pattern fails to compile.
    pub fn target_regex(mut self, r: &str) -> Result<Self, regex::Error> {
        self.target_regex = Some(Regex::new(r)?);
        Ok(self)
    }

    /// Match metadata `name`.
    #[must_use]
    pub fn name(mut self, n: &'static str) -> Self {
        self.name_eq = Some(n);
        self
    }

    /// Match if level is at-least the given threshold (TRACE < DEBUG <
    /// INFO < WARN < ERROR).
    #[must_use]
    pub fn level_at_least(mut self, l: Level) -> Self {
        self.level_min = Some(l);
        self
    }

    /// Require that a field with the given name appears on the
    /// metadata's static field set. Matchers can require multiple
    /// fields; all must be present.
    #[must_use]
    pub fn field(mut self, f: &'static str) -> Self {
        self.require_fields.push(f);
        self
    }

    /// Test the matcher against a metadata. The match is purely on
    /// `'static` parts — name, target, fields — so it is safe to cache
    /// the outcome per `tracing_core::callsite::Identifier`.
    #[must_use]
    pub fn matches(&self, meta: &'static Metadata<'static>) -> bool {
        if let Some(t) = self.target_eq {
            if meta.target() != t {
                return false;
            }
        }
        if let Some(p) = self.target_prefix {
            if !meta.target().starts_with(p) {
                return false;
            }
        }
        if let Some(re) = &self.target_regex {
            if !re.is_match(meta.target()) {
                return false;
            }
        }
        if let Some(n) = self.name_eq {
            if meta.name() != n {
                return false;
            }
        }
        if let Some(min) = self.level_min {
            if level_rank(*meta.level()) < level_rank(min) {
                return false;
            }
        }
        if !self.require_fields.is_empty() {
            for required in &self.require_fields {
                if meta.fields().field(required).is_none() {
                    return false;
                }
            }
        }
        true
    }
}

fn level_rank(l: Level) -> u8 {
    match l {
        Level::TRACE => 0,
        Level::DEBUG => 1,
        Level::INFO => 2,
        Level::WARN => 3,
        Level::ERROR => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_rank_should_be_total_order() {
        assert!(level_rank(Level::TRACE) < level_rank(Level::INFO));
        assert!(level_rank(Level::WARN) < level_rank(Level::ERROR));
    }

    #[test]
    fn test_matcher_should_default_to_match_all() {
        // Cannot synthesise a real `&'static Metadata` here, but the
        // empty matcher always returns `true` for any metadata it
        // sees — covered by the integration tests under
        // `tests/`. Smoke check the level helper.
        let m = TypedMatcher::new().level_at_least(Level::INFO);
        assert!(m.level_min.is_some());
    }
}
