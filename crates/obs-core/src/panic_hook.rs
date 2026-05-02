//! `install_panic_hook` — emits one `ObsPanicked` event then calls
//! `Observer::shutdown_blocking(2s)` so the inflight sinks have a
//! chance to flush before the prior hook (potentially `panic = abort`)
//! takes the process down.
//!
//! Spec 11 § 6.1.

use std::{
    panic::{self, PanicHookInfo},
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use obs_proto::obs::v1::{ObsEnvelope, Severity as PSeverity, Tier as PTier};

static INSTALLED: AtomicBool = AtomicBool::new(false);

/// Install the obs panic hook. Idempotent — calling more than once is
/// a no-op.
///
/// The hook chains the previously-installed hook (typically the
/// default), so `panic!` still produces the standard backtrace; the
/// added behaviour is one `ObsPanicked` envelope plus a 2-second
/// best-effort sink flush before the chained hook runs.
pub fn install_panic_hook() {
    if INSTALLED.swap(true, Ordering::SeqCst) {
        return;
    }
    let prev = panic::take_hook();
    panic::set_hook(Box::new(move |info: &PanicHookInfo<'_>| {
        emit_panicked(info);
        // Best-effort flush before the chain continues.
        let observer = crate::observer::observer();
        observer.shutdown_blocking(Duration::from_secs(2));
        prev(info);
    }));
}

fn emit_panicked(info: &PanicHookInfo<'_>) {
    let message = panic_message(info);
    let location = info
        .location()
        .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
        .unwrap_or_default();

    let mut env = ObsEnvelope {
        full_name: "obs.runtime.v1.ObsPanicked".to_string(),
        tier: ::buffa::EnumValue::Known(PTier::TIER_LOG),
        sev: ::buffa::EnumValue::Known(PSeverity::SEVERITY_FATAL),
        ts_ns: now_ns(),
        sampling_reason: ::buffa::EnumValue::Known(
            obs_proto::obs::v1::SamplingReason::SAMPLING_REASON_OVERRIDE,
        ),
        ..Default::default()
    };
    env.labels
        .insert("message".to_string(), truncate(&message, 1024));
    env.labels.insert("location".to_string(), location);

    let observer = crate::observer::observer();
    observer.emit_envelope(env);
}

fn panic_message(info: &PanicHookInfo<'_>) -> String {
    if let Some(s) = info.payload().downcast_ref::<&'static str>() {
        return (*s).to_string();
    }
    if let Some(s) = info.payload().downcast_ref::<String>() {
        return s.clone();
    }
    "panic with non-string payload".to_string()
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let mut t = s[..max].to_string();
        t.push('…');
        t
    }
}

fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}
