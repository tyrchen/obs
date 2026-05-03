//! `notify`-driven file watcher for `obs.yaml` reload. Spec 15 § 5.3
//! / spec 93 P0-9.
//!
//! [`ConfigWatcher::spawn`] launches a background OS-level watcher
//! that debounces file change events and invokes a user-supplied
//! callback with the freshly-reloaded [`EventsConfig`]. The callback
//! is the integration point — typically it calls
//! `Observer::reload_config` plus emits an `ObsConfigReloaded` /
//! `ObsConfigReloadFailed` self-event so operators can confirm the
//! reload landed.
//!
//! The watcher debounces with a 200 ms window so a save that triggers
//! `Modify(Data) + Modify(Metadata)` only fires the callback once.
//! Drop the [`ConfigWatcher`] handle to stop watching.

#![allow(clippy::disallowed_methods, clippy::disallowed_types)]

use std::{
    path::{Path, PathBuf},
    sync::{Arc, mpsc::channel},
    thread,
    time::{Duration, Instant},
};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::Mutex;

use crate::config::{ConfigError, EventsConfig};

/// Active file watcher. Drop to stop.
pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
    _join: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl std::fmt::Debug for ConfigWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConfigWatcher").finish()
    }
}

/// Default debounce window between consecutive change events.
pub const DEFAULT_DEBOUNCE: Duration = Duration::from_millis(200);

impl ConfigWatcher {
    /// Watch `path` for changes; on each debounced event, reload the
    /// file and invoke `on_change` with the parsed result. Errors from
    /// the loader are passed to `on_change` as `Err` so callers can
    /// emit `ObsConfigReloadFailed` self-events without their own
    /// retry loop.
    ///
    /// # Errors
    ///
    /// Returns the underlying notify error if the watcher cannot be
    /// installed (e.g. path does not exist, EMFILE, permission).
    pub fn spawn<F>(path: impl AsRef<Path>, on_change: F) -> Result<Self, notify::Error>
    where
        F: Fn(Result<EventsConfig, ConfigError>) + Send + 'static,
    {
        let path: PathBuf = path.as_ref().to_path_buf();
        let (tx, rx) = channel::<notify::Result<Event>>();

        let mut watcher: RecommendedWatcher = notify::recommended_watcher(move |res| {
            let _ = tx.send(res);
        })?;
        let watch_dir = path.parent().map(Path::to_path_buf).unwrap_or(path.clone());
        watcher.watch(&watch_dir, RecursiveMode::NonRecursive)?;

        let watch_path = path.clone();
        let join = thread::Builder::new()
            .name("obs-config-watcher".to_string())
            .spawn(move || {
                let mut last_fire: Option<Instant> = None;
                while let Ok(event) = rx.recv() {
                    let Ok(ev) = event else { continue };
                    if !is_relevant(&ev, &watch_path) {
                        continue;
                    }
                    if let Some(prev) = last_fire
                        && prev.elapsed() < DEFAULT_DEBOUNCE
                    {
                        continue;
                    }
                    last_fire = Some(Instant::now());
                    let cfg = EventsConfig::from_yaml_path(&watch_path);
                    on_change(cfg);
                }
            })
            .map_err(|e| notify::Error::generic(&format!("spawn watcher thread: {e}")))?;

        Ok(Self {
            _watcher: watcher,
            _join: Arc::new(Mutex::new(Some(join))),
        })
    }
}

fn is_relevant(ev: &Event, target: &Path) -> bool {
    if !matches!(
        ev.kind,
        EventKind::Modify(_) | EventKind::Create(_) | EventKind::Remove(_)
    ) {
        return false;
    }
    // macOS FSEvents reports paths via `/private/tmp/...` while the
    // test passes `/tmp/...`; ext4 inotify reports the full canonical
    // path; Windows reports backslash-separated. Match on file name +
    // canonicalized parent so any of these resolves correctly.
    let target_name = target.file_name();
    let target_parent = target.parent().and_then(|p| p.canonicalize().ok());
    ev.paths.iter().any(|p| {
        if p == target {
            return true;
        }
        if p.file_name() != target_name {
            return false;
        }
        let p_parent = p.parent().and_then(|q| q.canonicalize().ok());
        match (p_parent, target_parent.as_ref()) {
            (Some(a), Some(b)) => &a == b,
            _ => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn test_should_reload_on_file_change() {
        // FSEvents (default macOS backend) coalesces events but the
        // initial subscribe can take a few hundred ms to land. Give
        // the watcher more wall-clock budget on the slowest backend.
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("obs.yaml");
        std::fs::write(&path, "filter: info\n").expect("write initial");

        let calls: Arc<AtomicUsize> = Arc::new(AtomicUsize::new(0));
        let last_filter: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let calls_c = Arc::clone(&calls);
        let last_filter_c = Arc::clone(&last_filter);
        let _w = ConfigWatcher::spawn(&path, move |res| {
            if let Ok(cfg) = res {
                *last_filter_c.lock() = cfg.filter.clone();
                calls_c.fetch_add(1, Ordering::SeqCst);
            }
        })
        .expect("spawn");

        // Allow the watcher to subscribe before issuing any writes.
        std::thread::sleep(Duration::from_millis(500));
        let mut f = std::fs::File::create(&path).expect("recreate");
        writeln!(f, "filter: warn").expect("write");
        f.sync_all().ok();
        drop(f);

        // Poll for up to 4 s for the event to land + reload to run.
        for _ in 0..40 {
            if calls.load(Ordering::SeqCst) > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        let n = calls.load(Ordering::SeqCst);
        assert!(n >= 1, "expected at least one reload, got {n}");
        assert_eq!(last_filter.lock().as_deref(), Some("warn"));
    }
}
