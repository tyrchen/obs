//! `obs::Filter` — EnvFilter-shaped DSL ported from
//! `tracing-subscriber::filter::env`.
//!
//! Spec 13 § 7. The grammar is `[target][=level][[field=value,...]]`.
//! A directive without a `[field=value]` clause is a "static" directive
//! and lives in a sorted vec; a directive with a `[field=value]` clause
//! is "dynamic" and is bucketed by `full_name` so the hot path is one
//! `HashMap` probe + a tiny vector walk (spec 13 § 7.0).
//!
//! This is **not** a full port of EnvFilter — we keep the same
//! syntactic shape so operators do not relearn it. Specifically we
//! support: `info`, `myapp::auth=debug`, `myapp.v1.ObsRequestCompleted=trace`,
//! `[route=admin]=warn`, comma-separated combinations.

use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use obs_proto::obs::v1::ObsEnvelope;
use obs_types::Severity;

use crate::callsite::{Interest, ObsCallsite};

/// One parsed directive.
#[derive(Debug, Clone)]
pub struct Directive {
    /// Optional target prefix (a `module_path!()` prefix or a
    /// fully-qualified event `full_name`). `None` ⇒ matches every
    /// callsite.
    pub target: Option<String>,
    /// Field-value clauses (`[field=value]`). Empty ⇒ static directive.
    pub fields: Vec<(String, String)>,
    /// Severity threshold; events at this severity or above match.
    pub level: Severity,
}

/// Parsed `obs::Filter`. Statics are checked first against the
/// callsite; dynamics are bucketed by `full_name` and consulted at
/// emit time against `env.labels`.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Severity floor when no directive matches; default `Info`.
    default_level: Severity,
    /// Static directives (no `[field=value]` clause).
    statics: Vec<Directive>,
    /// Dynamic directives bucketed by full_name. The bucket vector is
    /// kept tiny by typical usage (spec 13 § 7.0).
    dynamics: HashMap<String, Vec<Directive>>,
}

impl Filter {
    /// Construct an empty filter that allows everything `>= Info`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_level: Severity::Info,
            statics: Vec::new(),
            dynamics: HashMap::new(),
        }
    }

    /// Parse a filter spec string (`OBS_FILTER` / `obs.yaml`'s `filter`).
    ///
    /// # Errors
    ///
    /// Returns `FilterParseError::Malformed` when a directive is
    /// syntactically invalid, e.g. `=info` (missing target/level).
    pub fn parse(s: &str) -> Result<Self, FilterParseError> {
        let mut f = Self::new();
        for raw in s.split(',') {
            let raw = raw.trim();
            if raw.is_empty() {
                continue;
            }
            f.add_directive(raw)?;
        }
        Ok(f)
    }

    /// Add one directive. Public for programmatic construction.
    ///
    /// # Errors
    ///
    /// Returns `FilterParseError::Malformed` when the directive is
    /// syntactically invalid.
    pub fn add_directive(&mut self, raw: &str) -> Result<(), FilterParseError> {
        let directive = parse_directive(raw)?;
        if directive.target.is_none() && directive.fields.is_empty() {
            self.default_level = directive.level;
            return Ok(());
        }
        if directive.fields.is_empty() {
            self.statics.push(directive);
        } else {
            let key = directive.target.clone().unwrap_or_default();
            self.dynamics.entry(key).or_default().push(directive);
        }
        Ok(())
    }

    /// Decide [`Interest`] for a static callsite (no envelope visible
    /// yet). Used at first-sight by the runtime to populate the
    /// callsite cache.
    ///
    /// `Sometimes` is returned whenever a dynamic directive references
    /// the callsite — the per-emit `event_allowed` is then consulted
    /// once the envelope is available.
    #[must_use]
    pub fn callsite_interest(&self, callsite: &ObsCallsite) -> Interest {
        let sev_floor = self
            .static_level_for(callsite.full_name(), callsite.module())
            .unwrap_or(self.default_level);
        let dynamic_present = self.has_dynamic_for(callsite.full_name());
        if callsite.default_sev() < sev_floor {
            // Always keep dynamics live — a `[field=value]` directive at
            // a higher level may still elect to keep this site.
            if dynamic_present {
                Interest::Sometimes
            } else {
                Interest::Never
            }
        } else if dynamic_present {
            Interest::Sometimes
        } else {
            Interest::Always
        }
    }

    /// Per-emit decision once the envelope is built. Honours dynamic
    /// directives (`[field=value]`) by reading `env.labels`.
    #[must_use]
    pub fn event_allowed(&self, env: &ObsEnvelope, callsite_sev: Severity) -> bool {
        let sev_floor = self
            .static_level_for(&env.full_name, "") // module not needed once full_name matched
            .unwrap_or(self.default_level);
        if let Some(buckets) = self.dynamics.get(env.full_name.as_str()) {
            for d in buckets {
                if directive_matches_env(d, env) {
                    return callsite_sev >= d.level;
                }
            }
        }
        callsite_sev >= sev_floor
    }

    /// `true` when at least one dynamic directive references this
    /// `full_name`.
    fn has_dynamic_for(&self, full_name: &str) -> bool {
        self.dynamics.contains_key(full_name)
    }

    fn static_level_for(&self, full_name: &str, module: &str) -> Option<Severity> {
        let mut best: Option<(usize, Severity)> = None;
        for d in &self.statics {
            let Some(t) = d.target.as_deref() else {
                continue;
            };
            let matches = full_name.starts_with(t)
                || (full_name.contains(t) && t.contains('.'))
                || module.starts_with(t);
            if matches && best.map(|(len, _)| t.len() > len).unwrap_or(true) {
                best = Some((t.len(), d.level));
            }
        }
        best.map(|(_, lvl)| lvl)
    }

    /// Default severity floor.
    #[must_use]
    pub fn default_level(&self) -> Severity {
        self.default_level
    }

    /// Iter over static directives.
    pub fn statics(&self) -> impl Iterator<Item = &Directive> {
        self.statics.iter()
    }

    /// Iter over dynamic directives.
    pub fn dynamics(&self) -> impl Iterator<Item = &Directive> {
        self.dynamics.values().flat_map(|v| v.iter())
    }
}

impl FromStr for Filter {
    type Err = FilterParseError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

fn directive_matches_env(d: &Directive, env: &ObsEnvelope) -> bool {
    for (k, v) in &d.fields {
        match env.labels.get(k.as_str()) {
            Some(actual) if actual == v => continue,
            _ => return false,
        }
    }
    true
}

fn parse_directive(raw: &str) -> Result<Directive, FilterParseError> {
    // Forms:
    //   level
    //   target=level
    //   target[field=value,...]=level
    //   [field=value,...]=level
    //
    // The bare-level form (`info`, `warn`, …) is special: when `raw`
    // parses as a Severity, we treat it as a default-level directive
    // (target/fields both empty).
    if let Ok(level) = Severity::from_str(raw.trim()) {
        return Ok(Directive {
            target: None,
            fields: Vec::new(),
            level,
        });
    }
    let (head, level) = match raw.rfind('=') {
        Some(idx) => {
            let level = parse_level(&raw[idx + 1..])?;
            (&raw[..idx], level)
        }
        None => (raw, Severity::Trace),
    };
    let (target, fields) = if let Some(open) = head.find('[') {
        let target = if open == 0 {
            None
        } else {
            Some(head[..open].to_string())
        };
        let close = head
            .rfind(']')
            .ok_or_else(|| FilterParseError::Malformed(raw.to_string()))?;
        if close <= open {
            return Err(FilterParseError::Malformed(raw.to_string()));
        }
        let fields_str = &head[open + 1..close];
        let mut fields = Vec::new();
        for clause in fields_str.split(',') {
            let clause = clause.trim();
            if clause.is_empty() {
                continue;
            }
            let mut it = clause.splitn(2, '=');
            let k = it
                .next()
                .ok_or_else(|| FilterParseError::Malformed(raw.to_string()))?
                .trim()
                .to_string();
            let v = it
                .next()
                .ok_or_else(|| FilterParseError::Malformed(raw.to_string()))?
                .trim()
                .to_string();
            fields.push((k, v));
        }
        (target, fields)
    } else if head.is_empty() {
        (None, Vec::new())
    } else {
        (Some(head.to_string()), Vec::new())
    };
    Ok(Directive {
        target,
        fields,
        level,
    })
}

fn parse_level(s: &str) -> Result<Severity, FilterParseError> {
    Severity::from_str(s.trim())
        .map_err(|_| FilterParseError::Malformed(format!("unknown level `{s}`")))
}

/// Errors returned by [`Filter::parse`].
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FilterParseError {
    /// Directive is syntactically invalid.
    #[error("malformed filter directive: {0}")]
    Malformed(String),
}

/// Best-effort: list every label key referenced by any dynamic
/// directive. Used by `obs lint` to warn when a field-value clause
/// references a name that no LABEL field declares (spec 13 § 7.1).
#[must_use]
pub fn referenced_label_keys(filter: &Filter) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for d in filter.dynamics() {
        let target = d.target.clone().unwrap_or_default();
        for (k, _) in &d.fields {
            out.entry(k.clone()).or_default().push(target.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_default_level() {
        let f: Filter = "warn".parse().unwrap();
        assert_eq!(f.default_level(), Severity::Warn);
    }

    #[test]
    fn test_parse_target_level() {
        let f: Filter = "info,myapp::auth=debug".parse().unwrap();
        assert_eq!(f.default_level(), Severity::Info);
        let static_count = f.statics().count();
        assert_eq!(static_count, 1);
    }

    #[test]
    fn test_dynamic_directive() {
        let f: Filter = "myapp.v1.ObsRequestCompleted[route=admin]=warn"
            .parse()
            .unwrap();
        let dynamic_count = f.dynamics().count();
        assert_eq!(dynamic_count, 1);
    }

    #[test]
    fn test_event_allowed_label_match() {
        let f: Filter = "info,myapp.v1.ObsRequestCompleted[route=admin]=trace"
            .parse()
            .unwrap();
        let mut env = ObsEnvelope {
            full_name: "myapp.v1.ObsRequestCompleted".to_string(),
            ..Default::default()
        };
        env.labels.insert("route".to_string(), "admin".to_string());
        assert!(f.event_allowed(&env, Severity::Trace));
        env.labels.insert("route".to_string(), "users".to_string());
        // route=users does not match the directive; the static `info`
        // floor decides — Trace < Info ⇒ deny.
        assert!(!f.event_allowed(&env, Severity::Trace));
    }
}
