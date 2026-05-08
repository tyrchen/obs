//! Prometheus 0.0.4 and OpenMetrics 1.0 renderers.

use std::{collections::BTreeMap, io::Write};

use crate::{
    PromError,
    config::RenderFormat,
    registry::Inner,
    series::{SeriesKey, SeriesKind, SeriesState},
};

pub(crate) fn render<W: Write>(
    inner: &Inner,
    out: &mut W,
    format: RenderFormat,
    emit_eof: bool,
) -> Result<(), PromError> {
    // Group series by metric name so the `# HELP` / `# TYPE` preamble
    // precedes every matching line. DashMap iteration order is
    // implementation-defined — sort into a BTreeMap for determinism.
    let mut grouped: BTreeMap<String, Vec<(SeriesKey, std::sync::Arc<SeriesState>)>> =
        BTreeMap::new();
    for entry in inner.series.iter() {
        grouped
            .entry(entry.key().name.clone())
            .or_default()
            .push((entry.key().clone(), std::sync::Arc::clone(entry.value())));
    }

    for (name, mut series) in grouped {
        series.sort_by(|a, b| a.0.labels.cmp(&b.0.labels));
        // Every series in a group must share the same kind — we
        // freeze kind at insertion, so the first is representative.
        let Some((_, state)) = series.first() else {
            continue;
        };
        let kind = state.kind;
        writeln!(out, "# HELP {name} obs-prom series")?;
        writeln!(out, "# TYPE {name} {}", kind.text_type())?;
        if matches!(format, RenderFormat::OpenMetricsText)
            && let Some(unit) = state.unit
        {
            writeln!(out, "# UNIT {name} {unit}")?;
        }
        for (key, state) in &series {
            render_series(out, &name, key, state, format)?;
        }
    }

    // OpenMetrics marks the end-of-stream with `# EOF`. Callers that
    // need to concatenate with another producer pass `emit_eof=false`
    // and emit the terminator themselves.
    if emit_eof && matches!(format, RenderFormat::OpenMetricsText) {
        writeln!(out, "# EOF")?;
    }
    Ok(())
}

fn render_series<W: Write>(
    out: &mut W,
    name: &str,
    key: &SeriesKey,
    state: &SeriesState,
    format: RenderFormat,
) -> Result<(), PromError> {
    let labels = render_labels(&key.labels);
    match state.kind {
        SeriesKind::Counter => {
            let v = *state.counter.read();
            writeln!(out, "{name}{labels} {v}")?;
        }
        SeriesKind::GaugeU64 => {
            let v = *state.gauge_u64.read();
            writeln!(out, "{name}{labels} {v}")?;
        }
        SeriesKind::GaugeF64 => {
            let v = *state.gauge_f64.read();
            writeln!(out, "{name}{labels} {}", format_float(v))?;
        }
        SeriesKind::Histogram => {
            let h = state.histogram.read();
            for (i, bound) in h.bounds.iter().enumerate() {
                let bucket_count = h.bucket_counts.get(i).copied().unwrap_or(0);
                let bucket_labels = append_label(&key.labels, "le", &format_float(*bound));
                writeln!(
                    out,
                    "{name}_bucket{} {bucket_count}",
                    render_labels(&bucket_labels)
                )?;
            }
            // +Inf bucket: OpenMetrics requires it, Prometheus accepts it.
            let inf_labels = append_label(&key.labels, "le", "+Inf");
            writeln!(
                out,
                "{name}_bucket{} {}",
                render_labels(&inf_labels),
                h.count
            )?;
            writeln!(out, "{name}_sum{labels} {}", format_float(h.sum))?;
            writeln!(out, "{name}_count{labels} {}", h.count)?;
        }
    }
    let _ = format;
    Ok(())
}

fn render_labels(pairs: &[(String, String)]) -> String {
    if pairs.is_empty() {
        return String::new();
    }
    let mut out = String::with_capacity(32);
    out.push('{');
    for (i, (k, v)) in pairs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(k);
        out.push('=');
        out.push('"');
        escape_into(v, &mut out);
        out.push('"');
    }
    out.push('}');
    out
}

fn append_label(src: &[(String, String)], k: &str, v: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = src.iter().filter(|(lk, _)| lk != k).cloned().collect();
    out.push((k.to_string(), v.to_string()));
    out.sort();
    out
}

fn escape_into(raw: &str, out: &mut String) {
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
}

fn format_float(v: f64) -> String {
    if v.is_nan() {
        "NaN".to_string()
    } else if v.is_infinite() {
        if v.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else if v == v.trunc() && v.abs() < 1e16 {
        format!("{v}")
    } else {
        format!("{v:?}")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, atomic::AtomicU64};

    use dashmap::DashMap;

    use super::*;
    use crate::{
        config::PromConfig,
        registry::Inner,
        series::{SeriesKey, SeriesKind},
    };

    fn empty_inner() -> Arc<Inner> {
        let cfg = PromConfig::default();
        let default_bounds: &'static [f64] =
            Box::leak(cfg.default_histogram_bounds.clone().into_boxed_slice());
        Arc::new(Inner {
            config: cfg,
            default_bounds,
            series: DashMap::new(),
            dropped: AtomicU64::new(0),
            evicted_idle: AtomicU64::new(0),
            cap_breach_notified: DashMap::new(),
        })
    }

    #[test]
    fn test_counter_renders_in_prom_text() {
        let inner = empty_inner();
        let state = inner
            .get_or_insert(
                SeriesKey {
                    name: "foo_total".into(),
                    labels: vec![("route".into(), "/x".into())],
                },
                SeriesKind::Counter,
                None,
            )
            .expect("insert");
        state.record_counter(7);

        let mut out = Vec::<u8>::new();
        render(&inner, &mut out, RenderFormat::PrometheusText, true).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("# TYPE foo_total counter"), "{s}");
        assert!(s.contains("foo_total{route=\"/x\"} 7"), "{s}");
    }

    #[test]
    fn test_openmetrics_emits_eof_marker() {
        let inner = empty_inner();
        let mut out = Vec::<u8>::new();
        render(&inner, &mut out, RenderFormat::OpenMetricsText, true).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.ends_with("# EOF\n"), "{s:?}");
    }

    #[test]
    fn test_histogram_emits_bucket_sum_count() {
        let inner = empty_inner();
        let state = inner
            .get_or_insert(
                SeriesKey {
                    name: "latency".into(),
                    labels: vec![],
                },
                SeriesKind::Histogram,
                None,
            )
            .expect("insert");
        static BOUNDS: &[f64] = &[0.01, 0.1, 1.0];
        state.record_histogram(0.005, BOUNDS);
        state.record_histogram(0.2, BOUNDS);
        state.record_histogram(2.0, BOUNDS);

        let mut out = Vec::<u8>::new();
        render(&inner, &mut out, RenderFormat::PrometheusText, true).unwrap();
        let s = String::from_utf8(out).unwrap();
        assert!(s.contains("# TYPE latency histogram"), "{s}");
        assert!(s.contains("latency_bucket{le=\"0.01\"} 1"), "{s}");
        assert!(s.contains("latency_bucket{le=\"0.1\"} 1"), "{s}");
        assert!(s.contains("latency_bucket{le=\"1\"} 2"), "{s}");
        assert!(s.contains("latency_bucket{le=\"+Inf\"} 3"), "{s}");
        assert!(s.contains("latency_sum"), "{s}");
        assert!(s.contains("latency_count"), "{s}");
    }

    #[test]
    fn test_cap_enforcement_caps_series_count() {
        let cfg = PromConfig {
            max_series: 2,
            ..PromConfig::default()
        };
        let default_bounds: &'static [f64] =
            Box::leak(cfg.default_histogram_bounds.clone().into_boxed_slice());
        let inner = Arc::new(Inner {
            config: cfg,
            default_bounds,
            series: DashMap::new(),
            dropped: AtomicU64::new(0),
            evicted_idle: AtomicU64::new(0),
            cap_breach_notified: DashMap::new(),
        });
        let _ = inner
            .get_or_insert(
                SeriesKey {
                    name: "m".into(),
                    labels: vec![("x".into(), "1".into())],
                },
                SeriesKind::Counter,
                None,
            )
            .expect("insert1");
        let _ = inner
            .get_or_insert(
                SeriesKey {
                    name: "m".into(),
                    labels: vec![("x".into(), "2".into())],
                },
                SeriesKind::Counter,
                None,
            )
            .expect("insert2");
        let overflow = inner.get_or_insert(
            SeriesKey {
                name: "m".into(),
                labels: vec![("x".into(), "3".into())],
            },
            SeriesKind::Counter,
            None,
        );
        assert!(overflow.is_none(), "cap must reject the third series");
        assert_eq!(inner.dropped.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
