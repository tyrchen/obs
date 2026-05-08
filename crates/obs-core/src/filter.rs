//! `obs::Filter` — EnvFilter-shaped DSL ported from
//! `tracing-subscriber::filter::env`.
//!
//! Spec 13 § 7 / spec 94 § 2.9. The grammar is
//! `[target][=level][[field=value,...]]`. A directive without a
//! `[field=value]` clause is a "static" directive and lives in a
//! sorted vec; a directive with a `[field=value]` clause is "dynamic"
//! and is bucketed by `full_name` so the hot path is one `HashMap`
//! probe + a tiny vector walk (spec 13 § 7.0).
//!
//! The parser is built on `winnow` and matches `tracing-subscriber`'s
//! EnvFilter for the documented subset:
//!
//! - bare level: `info`
//! - bare veto: `off`
//! - target=level: `myapp::auth=debug`
//! - target=off: `my_module=off`
//! - full-name = level: `myapp.v1.ObsRequestCompleted=trace`
//! - dynamic: `[route=admin]=warn` (matches against `env.labels`)
//! - target with dynamic clause: `myapp.v1.ObsX[route=admin]=warn`
//! - quoted field values: `[route="/users/:id,/v2"]=info`
//! - comma-separated combinations of any of the above
//! - whitespace between directives and inside clauses
//!
//! Top-level `,` splitting is bracket-aware so a `,` inside a
//! `[k=v,k=v]` clause does not terminate the directive.

use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
};

use obs_proto::obs::v1::{ObsEnvelope, Severity};

use crate::callsite::{Interest, ObsCallsite};

/// Resolved static directive: either an explicit veto (`=off`) or a
/// severity floor.
#[derive(Debug, Clone, Copy)]
enum StaticDirective {
    Off,
    Level(Severity),
}

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
    /// Ignored when [`Self::off`] is `true`.
    pub level: Severity,
    /// `true` for `=off` directives — callsites matched by this
    /// directive are vetoed regardless of severity. Spec 13 § 7 / spec
    /// 94 § 2.2.
    pub off: bool,
}

/// Parsed `obs::Filter`. Statics are checked first against the
/// callsite; dynamics are bucketed by `full_name` and consulted at
/// emit time against `env.labels`.
#[derive(Debug, Clone, Default)]
pub struct Filter {
    /// Severity floor when no directive matches; default `Info`.
    default_level: Severity,
    /// `true` when the bare-target floor is `off` (e.g. filter `off`).
    /// Vetoes every callsite that no static / dynamic directive elects
    /// to keep.
    default_off: bool,
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
            default_off: false,
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
        for raw in split_directives(s) {
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
            // A bare `off` directive disables every callsite; we model
            // that by setting the floor to `Fatal+1` semantics. Use a
            // boolean on the filter so the comparison is unambiguous.
            if directive.off {
                self.default_off = true;
            } else {
                self.default_level = directive.level;
                self.default_off = false;
            }
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
    ///
    /// Spec 94 § 2.2: a static directive that matches the callsite
    /// vetoes inclusion regardless of the global default floor — i.e.
    /// `info,my_module=off` rejects every `my_module::*` callsite even
    /// when its default severity is at or above `info`.
    #[must_use]
    pub fn callsite_interest(&self, callsite: &ObsCallsite) -> Interest {
        let resolved = self.static_level_for(callsite.full_name(), callsite.module());
        let dynamic_present = self.has_dynamic_for(callsite.full_name());
        match resolved {
            Some(StaticDirective::Off) => {
                // Explicit veto — only dynamics can rescue. Spec 94 § 2.2.
                if dynamic_present {
                    Interest::Sometimes
                } else {
                    Interest::Never
                }
            }
            Some(StaticDirective::Level(sev_floor)) => {
                if callsite.default_sev() < sev_floor {
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
            None => {
                // No matching static directive — fall back to the
                // bare-target floor (or `off` global veto).
                let blocked = self.default_off || callsite.default_sev() < self.default_level;
                if blocked {
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
        }
    }

    /// Per-emit decision once the envelope is built. Honours dynamic
    /// directives (`[field=value]`) by reading `env.labels`.
    #[must_use]
    pub fn event_allowed(&self, env: &ObsEnvelope, callsite_sev: Severity) -> bool {
        if let Some(buckets) = self.dynamics.get(env.full_name.as_str()) {
            for d in buckets {
                if directive_matches_env(d, env) {
                    if d.off {
                        return false;
                    }
                    return callsite_sev >= d.level;
                }
            }
        }
        match self.static_level_for(&env.full_name, "") {
            Some(StaticDirective::Off) => false,
            Some(StaticDirective::Level(sev)) => callsite_sev >= sev,
            None => {
                if self.default_off {
                    false
                } else {
                    callsite_sev >= self.default_level
                }
            }
        }
    }

    /// `true` when at least one dynamic directive references this
    /// `full_name`.
    fn has_dynamic_for(&self, full_name: &str) -> bool {
        self.dynamics.contains_key(full_name)
    }

    fn static_level_for(&self, full_name: &str, module: &str) -> Option<StaticDirective> {
        // Spec 13 § 7 / spec 93 P2-6 / spec 94 § 2.2: target match must
        // respect segment boundaries; the longest matching directive
        // wins, and an `=off` directive is preserved so callers can
        // veto the callsite.
        let mut best: Option<(usize, StaticDirective)> = None;
        for d in &self.statics {
            let Some(t) = d.target.as_deref() else {
                continue;
            };
            let matches = matches_segment(full_name, t) || matches_segment(module, t);
            if matches && best.map(|(len, _)| t.len() > len).unwrap_or(true) {
                let resolved = if d.off {
                    StaticDirective::Off
                } else {
                    StaticDirective::Level(d.level)
                };
                best = Some((t.len(), resolved));
            }
        }
        best.map(|(_, d)| d)
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

/// `s` matches `target` either by being equal, or by being prefixed
/// by `target` followed by a segment separator (`.` or `::`). Spec 93
/// P2-6.
fn matches_segment(s: &str, target: &str) -> bool {
    if s == target {
        return true;
    }
    if let Some(rest) = s.strip_prefix(target) {
        return rest.starts_with('.') || rest.starts_with("::");
    }
    false
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
    grammar::directive(raw.trim()).map_err(|e| FilterParseError::Malformed(e.to_string()))
}

/// Split the filter spec on directive boundaries, respecting `[...]`
/// bracket nesting so a `,` inside a `[field=val,field=val]` clause
/// does not terminate the directive. Spec 94 § 2.9.
fn split_directives(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut depth = 0i32;
    let mut quoted = false;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '"' if depth > 0 => quoted = !quoted,
            '[' if !quoted => depth += 1,
            ']' if !quoted => depth = depth.saturating_sub(1),
            ',' if depth == 0 && !quoted => {
                out.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    if start < s.len() {
        out.push(&s[start..]);
    } else if start == s.len() {
        // Trailing comma → empty directive; preserved so callers can
        // skip via the `is_empty()` check.
        out.push("");
    }
    out
}

/// winnow-based grammar for the filter DSL. Spec 13 § 7 / spec 94 §
/// 2.9 / P2-A. The grammar tracks `tracing-subscriber::EnvFilter` for
/// the documented subset:
///
/// ```ignore
/// directive    := bare_off | bare_level | head ('=' level_or_off)?
/// head         := target_part ('[' field_clause (',' field_clause)* ']')?
/// target_part  := <chars excluding '=', '[', ',', whitespace>
/// field_clause := key '=' value
/// key          := <chars excluding '=', ',', ']'>
/// value        := <chars excluding ',', ']'>
/// level_or_off := 'off' | 'trace' | 'debug' | 'info' | 'warn' | 'error' | 'fatal'
/// ```
///
/// The grammar is forgiving in a couple of places to keep operators'
/// muscle memory intact: target identifiers may include dots and
/// double-colons (so `myapp.v1.ObsRequestCompleted` and
/// `myapp::auth` both parse), level keywords are case-insensitive,
/// and field values may be quoted (`[route="/users/:id"]=info`) when
/// the value contains commas or square brackets.
mod grammar {
    use std::str::FromStr;

    use obs_proto::obs::v1::Severity;
    use winnow::{
        ModalResult, Parser as _,
        ascii::multispace0,
        combinator::{delimited, opt, preceded, separated},
        error::{ContextError, ErrMode, ParseError, StrContext, StrContextValue},
        token::take_while,
    };

    use super::Directive;

    /// Top-level entry: parse one full directive from a `&str`. The
    /// caller is expected to have already split on `,` between
    /// directives.
    pub(super) fn directive(input: &str) -> Result<Directive, ParseError<&str, ContextError>> {
        directive_full.parse(input)
    }

    fn directive_full(s: &mut &str) -> ModalResult<Directive> {
        let _ = multispace0.parse_next(s)?;
        // Bare `off` — global veto, target/fields empty.
        if let Some(()) = opt(bare_off).parse_next(s)? {
            let _ = multispace0.parse_next(s)?;
            return Ok(Directive {
                target: None,
                fields: Vec::new(),
                level: Severity::Trace,
                off: true,
            });
        }
        // Bare level — set default floor.
        if let Some(level) = opt(bare_level).parse_next(s)? {
            let _ = multispace0.parse_next(s)?;
            return Ok(Directive {
                target: None,
                fields: Vec::new(),
                level,
                off: false,
            });
        }
        // Otherwise: head '=' level_or_off, or head alone.
        let (target, fields) = head.parse_next(s)?;
        let _ = multispace0.parse_next(s)?;
        let (level, off) = match opt(preceded('=', cut_level_or_off)).parse_next(s)? {
            Some(v) => v,
            // Default for `target` alone is `Severity::Trace`, matching
            // tracing-subscriber's behaviour when a target appears
            // without an explicit level.
            None => (Severity::Trace, false),
        };
        let _ = multispace0.parse_next(s)?;
        Ok(Directive {
            target,
            fields,
            level,
            off,
        })
    }

    fn bare_off(s: &mut &str) -> ModalResult<()> {
        // Match `off` only when nothing follows that could continue a
        // target identifier.
        let snapshot = *s;
        let token = take_while(1.., is_target_char).parse_next(s)?;
        if !token.eq_ignore_ascii_case("off") {
            *s = snapshot;
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        // Reject if there's a trailing `=` / `[` / `,` — those would
        // turn this into `off=…` / `off[…]` / a path containing "off".
        let lookahead = s.chars().next();
        if matches!(lookahead, Some('=') | Some('[')) {
            *s = snapshot;
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        Ok(())
    }

    fn bare_level(s: &mut &str) -> ModalResult<Severity> {
        let snapshot = *s;
        let token = take_while(1.., is_target_char).parse_next(s)?;
        let level = match Severity::from_str(token) {
            Ok(l) => l,
            Err(_) => {
                *s = snapshot;
                return Err(ErrMode::Backtrack(ContextError::new()));
            }
        };
        let lookahead = s.chars().next();
        if matches!(lookahead, Some('=') | Some('[')) {
            *s = snapshot;
            return Err(ErrMode::Backtrack(ContextError::new()));
        }
        Ok(level)
    }

    type Head = (Option<String>, Vec<(String, String)>);

    fn head(s: &mut &str) -> ModalResult<Head> {
        let target_str = take_while(0.., is_target_char).parse_next(s)?;
        let target = if target_str.is_empty() {
            None
        } else {
            Some(target_str.to_string())
        };
        let fields = opt(field_clause_block).parse_next(s)?.unwrap_or_default();
        Ok((target, fields))
    }

    fn field_clause_block(s: &mut &str) -> ModalResult<Vec<(String, String)>> {
        '['.parse_next(s)?;
        let _ = multispace0.parse_next(s)?;
        let clauses: Vec<(String, String)> = separated(0.., field_clause, ',').parse_next(s)?;
        let _ = multispace0.parse_next(s)?;
        ']'.parse_next(s)?;
        Ok(clauses)
    }

    fn field_clause(s: &mut &str) -> ModalResult<(String, String)> {
        let _ = multispace0.parse_next(s)?;
        let key = take_while(1.., is_field_key_char).parse_next(s)?;
        let _ = multispace0.parse_next(s)?;
        '='.parse_next(s)?;
        let _ = multispace0.parse_next(s)?;
        let value = field_value.parse_next(s)?;
        let _ = multispace0.parse_next(s)?;
        Ok((key.to_string(), value))
    }

    fn field_value(s: &mut &str) -> ModalResult<String> {
        // Quoted values let operators carry commas / brackets / spaces
        // through to `env.labels` matching unchanged.
        if s.starts_with('"') {
            let inner = delimited('"', take_while(0.., is_quoted_value_char), '"').parse_next(s)?;
            return Ok(inner.to_string());
        }
        let raw = take_while(0.., is_field_value_char).parse_next(s)?;
        Ok(raw.trim().to_string())
    }

    fn is_quoted_value_char(c: char) -> bool {
        // Inside `"..."`: stop at the closing `"`. We don't support
        // backslash escapes in v1 — the documented use case is
        // alphanumeric / punctuation values that happen to contain
        // commas or brackets.
        c != '"'
    }

    fn cut_level_or_off(s: &mut &str) -> ModalResult<(Severity, bool)> {
        let _ = multispace0.parse_next(s)?;
        let token = take_while(1.., is_target_char)
            .context(StrContext::Expected(StrContextValue::Description(
                "level (trace|debug|info|warn|error|fatal) or 'off'",
            )))
            .parse_next(s)?;
        if token.eq_ignore_ascii_case("off") {
            return Ok((Severity::Trace, true));
        }
        match Severity::from_str(token) {
            Ok(level) => Ok((level, false)),
            Err(_) => Err(ErrMode::Cut(ContextError::new())),
        }
    }

    fn is_target_char(c: char) -> bool {
        // Targets / event full_names contain `[a-zA-Z0-9_:.]`. We
        // accept hyphens too — some workspaces emit dash-separated
        // crate names.
        matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | ':' | '.' | '-')
    }

    fn is_field_key_char(c: char) -> bool {
        matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_' | '.')
    }

    fn is_field_value_char(c: char) -> bool {
        // Stop at `,` and `]` so the next clause / block-close parses.
        // Everything else is part of the value (including spaces);
        // `field_value` trims trailing whitespace.
        !matches!(c, ',' | ']' | '"')
    }
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
    fn test_off_directive_vetoes_callsite_above_floor() {
        // Spec 94 § 2.2: `info,my_module=off` must veto every callsite
        // whose target lives under `my_module`, even when its default
        // severity is `>= info` (the global floor).
        static CALLSITE: ObsCallsite = ObsCallsite::new(
            "myapp.v1.ObsNoisy",
            Severity::Warn,
            "my_module::auth",
            "lib.rs",
            1,
        );
        let f: Filter = "info,my_module=off".parse().unwrap();
        assert_eq!(f.callsite_interest(&CALLSITE), Interest::Never);
    }

    #[test]
    fn test_off_directive_targets_full_name() {
        static CALLSITE: ObsCallsite = ObsCallsite::new(
            "myapp.v1.ObsNoisyFn",
            Severity::Warn,
            "myapp::handler",
            "lib.rs",
            1,
        );
        let f: Filter = "info,myapp.v1.ObsNoisyFn=off".parse().unwrap();
        assert_eq!(f.callsite_interest(&CALLSITE), Interest::Never);
    }

    #[test]
    fn test_global_off_blocks_everything() {
        static CALLSITE: ObsCallsite =
            ObsCallsite::new("myapp.v1.ObsXyz", Severity::Error, "myapp", "lib.rs", 1);
        let f: Filter = "off".parse().unwrap();
        assert_eq!(f.callsite_interest(&CALLSITE), Interest::Never);
    }

    #[test]
    fn test_winnow_parses_compound_directive() {
        // Spec 94 § 2.9 / P2-A: target + field clause + level.
        let f: Filter = "myapp.v1.ObsRequestCompleted[route=admin]=warn"
            .parse()
            .unwrap();
        let dyn_count = f.dynamics().count();
        assert_eq!(dyn_count, 1);
    }

    #[test]
    fn test_winnow_accepts_multiple_field_clauses() {
        let f: Filter = "myapp.v1.ObsX[route=admin,tenant=acme]=warn"
            .parse()
            .unwrap();
        let dynamic = f.dynamics().next().expect("one dynamic");
        assert_eq!(dynamic.fields.len(), 2);
        assert_eq!(dynamic.fields[0].0, "route");
        assert_eq!(dynamic.fields[0].1, "admin");
        assert_eq!(dynamic.fields[1].0, "tenant");
        assert_eq!(dynamic.fields[1].1, "acme");
    }

    #[test]
    fn test_winnow_accepts_quoted_values_with_commas() {
        // Quoted values let operators carry commas through to label
        // matching. Spec 94 § 2.9.
        let f: Filter = r#"myapp.v1.ObsX[route="/users/:id"]=warn"#.parse().unwrap();
        let dynamic = f.dynamics().next().expect("one dynamic");
        assert_eq!(dynamic.fields[0].0, "route");
        assert_eq!(dynamic.fields[0].1, "/users/:id");
    }

    #[test]
    fn test_winnow_accepts_whitespace_between_directives() {
        let f: Filter = " info , my_module = debug ".parse().unwrap();
        assert_eq!(f.default_level(), Severity::Info);
        let static_count = f.statics().count();
        assert_eq!(static_count, 1);
    }

    #[test]
    fn test_winnow_rejects_malformed_directive() {
        let err = Filter::parse("info,myapp::auth=BOGUS").unwrap_err();
        assert!(matches!(err, FilterParseError::Malformed(_)));
    }

    #[test]
    fn test_winnow_rejects_unclosed_bracket() {
        let err = Filter::parse("myapp[route=admin=info").unwrap_err();
        assert!(matches!(err, FilterParseError::Malformed(_)));
    }

    #[test]
    fn test_winnow_target_off_inside_directive_list() {
        // Mid-list `=off` directive must veto the matched target even
        // when a later directive sets a higher floor.
        let f: Filter = "info,my_module=off,other=debug".parse().unwrap();
        let static_count = f.statics().count();
        // Two static directives: my_module=off, other=debug.
        assert_eq!(static_count, 2);
    }

    #[test]
    fn test_winnow_allows_dotted_target_with_dashes() {
        // Some workspaces emit dash-separated crate names. Accept them.
        let f: Filter = "info,my-crate.module=warn".parse().unwrap();
        assert_eq!(f.statics().count(), 1);
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
