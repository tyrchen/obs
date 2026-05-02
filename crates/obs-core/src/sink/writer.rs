//! `MakeWriter` and the writer family (Stdout/Stderr/LevelSplit/Tee/
//! `RollingFile` / `NonBlocking`). Spec 20 §§ 3.3–3.5.
//!
//! `RollingFileWriter` deliberately uses `std::fs` synchronously
//! because it implements `std::io::Write`; sinks run in the per-tier
//! tokio worker that already owns the write call, so converting
//! everything to `tokio::fs` would force a `block_on` round-trip per
//! batch. The blanket `disallowed-{methods,types}` guidance is
//! file-overridden here.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::{
    io::{self, Write},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, sync_channel},
    },
    thread::{self, JoinHandle},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use obs_types::Severity;
use parking_lot::Mutex;

/// A factory that yields one `io::Write` per batch. Cheap to call.
/// Spec 20 § 3.3.
pub trait MakeWriter: Send + Sync + 'static {
    /// The writer type produced by [`Self::make_writer`]; usually
    /// `Stdout`, `Stderr`, or a guard around a file handle.
    type Writer: Write + Send + 'static;

    /// Yield a writer for this batch.
    fn make_writer(&self) -> Self::Writer;

    /// Yield a severity-specific writer; defaults to
    /// [`Self::make_writer`].
    fn make_writer_for(&self, _sev: Severity) -> Self::Writer {
        self.make_writer()
    }
}

/// Writes to stdout.
#[derive(Debug, Default, Clone, Copy)]
pub struct StdoutWriter;

impl MakeWriter for StdoutWriter {
    type Writer = io::Stdout;
    fn make_writer(&self) -> io::Stdout {
        io::stdout()
    }
}

/// Writes to stderr.
#[derive(Debug, Default, Clone, Copy)]
pub struct StderrWriter;

impl MakeWriter for StderrWriter {
    type Writer = io::Stderr;
    fn make_writer(&self) -> io::Stderr {
        io::stderr()
    }
}

/// Composes two writers — INFO+ goes through `low`, WARN+ through
/// `high`. The conventional shape for cargo binaries.
#[derive(Debug, Clone)]
pub struct LevelSplitWriter<L, H> {
    low: L,
    high: H,
    threshold: Severity,
}

impl<L: MakeWriter, H: MakeWriter> LevelSplitWriter<L, H> {
    /// New split writer with default `WARN` threshold.
    #[must_use]
    pub fn new(low: L, high: H) -> Self {
        Self {
            low,
            high,
            threshold: Severity::Warn,
        }
    }

    /// Override the threshold.
    #[must_use]
    pub fn threshold(mut self, threshold: Severity) -> Self {
        self.threshold = threshold;
        self
    }
}

/// Erased `Box<dyn Write + Send>` so `LevelSplitWriter::Writer` can
/// be a single concrete type (the trait associated type is fixed,
/// can't be `match`-ed at the type level).
pub struct ErasedWriter(Box<dyn Write + Send + 'static>);

impl ErasedWriter {
    /// Construct an erased writer.
    #[must_use]
    pub fn new<W: Write + Send + 'static>(w: W) -> Self {
        Self(Box::new(w))
    }
}

impl std::fmt::Debug for ErasedWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErasedWriter").finish_non_exhaustive()
    }
}

impl Write for ErasedWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.0.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl<L: MakeWriter, H: MakeWriter> MakeWriter for LevelSplitWriter<L, H> {
    type Writer = ErasedWriter;

    fn make_writer(&self) -> ErasedWriter {
        ErasedWriter::new(self.low.make_writer())
    }

    fn make_writer_for(&self, sev: Severity) -> ErasedWriter {
        if sev >= self.threshold {
            ErasedWriter::new(self.high.make_writer_for(sev))
        } else {
            ErasedWriter::new(self.low.make_writer_for(sev))
        }
    }
}

/// Tee writer — writes to both branches.
#[derive(Debug, Clone)]
pub struct TeeWriter<A, B> {
    a: A,
    b: B,
}

impl<A: MakeWriter, B: MakeWriter> TeeWriter<A, B> {
    /// New tee writer.
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

/// Concrete writer returned by `TeeWriter::make_writer` — writes
/// every byte to both inner writers; if either errors the call
/// returns the first error.
pub struct TeeWriterImpl<WA: Write, WB: Write> {
    a: WA,
    b: WB,
}

impl<WA: Write, WB: Write> Write for TeeWriterImpl<WA, WB> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.a.write_all(buf)?;
        self.b.write_all(buf)?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.a.flush()?;
        self.b.flush()
    }
}

impl<WA: Write, WB: Write> std::fmt::Debug for TeeWriterImpl<WA, WB> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TeeWriterImpl").finish_non_exhaustive()
    }
}

impl<A: MakeWriter, B: MakeWriter> MakeWriter for TeeWriter<A, B> {
    type Writer = TeeWriterImpl<A::Writer, B::Writer>;

    fn make_writer(&self) -> Self::Writer {
        TeeWriterImpl {
            a: self.a.make_writer(),
            b: self.b.make_writer(),
        }
    }
}

// ─── RollingFileWriter ────────────────────────────────────────────────

/// Rolling policy. Spec 20 § 3.4.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum RollingPolicy {
    /// Never rotate.
    Never,
    /// Rotate when file size hits `max_bytes`.
    SizeBased {
        /// Per-file size cap.
        max_bytes: u64,
    },
    /// Rotate at midnight UTC.
    Daily,
    /// Rotate at the top of every hour UTC.
    Hourly,
    /// Rotate on size or age, whichever first.
    SizeOrAge {
        /// Per-file size cap.
        max_bytes: u64,
        /// Per-file age cap.
        max_age: Duration,
    },
}

/// Builder for [`RollingFileWriter`].
#[derive(Debug, Clone)]
pub struct RollingFileWriterBuilder {
    directory: Option<PathBuf>,
    prefix: Option<String>,
    suffix: String,
    policy: RollingPolicy,
    keep: Option<usize>,
}

impl Default for RollingFileWriterBuilder {
    fn default() -> Self {
        Self {
            directory: None,
            prefix: None,
            suffix: ".ndjson".to_string(),
            policy: RollingPolicy::Daily,
            keep: None,
        }
    }
}

impl RollingFileWriterBuilder {
    /// Output directory (created if absent).
    #[must_use]
    pub fn directory(mut self, dir: impl Into<PathBuf>) -> Self {
        self.directory = Some(dir.into());
        self
    }

    /// Filename prefix (e.g. `obs`). Required.
    #[must_use]
    pub fn filename_prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = Some(p.into());
        self
    }

    /// Filename suffix (default `.ndjson`).
    #[must_use]
    pub fn filename_suffix(mut self, s: impl Into<String>) -> Self {
        self.suffix = s.into();
        self
    }

    /// Set the rolling policy.
    #[must_use]
    pub fn policy(mut self, p: RollingPolicy) -> Self {
        self.policy = p;
        self
    }

    /// Retain the last `n` rolled files. Older files are deleted at
    /// rotation time.
    #[must_use]
    pub fn keep(mut self, n: usize) -> Self {
        self.keep = Some(n);
        self
    }

    /// Build the writer. Creates the directory if absent.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` if the directory cannot be created or the
    /// initial file cannot be opened.
    pub fn build(self) -> io::Result<RollingFileWriter> {
        let dir = self
            .directory
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "directory is required"))?;
        let prefix = self.prefix.unwrap_or_else(|| "obs".to_string());
        std::fs::create_dir_all(&dir)?;
        let inner = RollingInner {
            directory: dir,
            prefix,
            suffix: self.suffix,
            policy: self.policy,
            keep: self.keep,
            current: Mutex::new(None),
        };
        Ok(RollingFileWriter {
            inner: Arc::new(inner),
        })
    }
}

/// Rolling file writer with size + time policies. Spec 20 § 3.4.
#[derive(Clone)]
pub struct RollingFileWriter {
    inner: Arc<RollingInner>,
}

impl std::fmt::Debug for RollingFileWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RollingFileWriter")
            .field("directory", &self.inner.directory)
            .field("prefix", &self.inner.prefix)
            .field("policy", &self.inner.policy)
            .finish()
    }
}

struct RollingInner {
    directory: PathBuf,
    prefix: String,
    suffix: String,
    policy: RollingPolicy,
    keep: Option<usize>,
    current: Mutex<Option<RollingState>>,
}

struct RollingState {
    file: std::fs::File,
    bytes: u64,
    opened_at: SystemTime,
}

impl RollingFileWriter {
    /// Builder entry.
    #[must_use]
    pub fn builder() -> RollingFileWriterBuilder {
        RollingFileWriterBuilder::default()
    }
}

impl MakeWriter for RollingFileWriter {
    type Writer = RollingFileHandle;
    fn make_writer(&self) -> RollingFileHandle {
        RollingFileHandle {
            inner: Arc::clone(&self.inner),
        }
    }
}

/// Handle returned per-batch by `RollingFileWriter`. Each `write_all`
/// rotates if the policy demands it.
pub struct RollingFileHandle {
    inner: Arc<RollingInner>,
}

impl std::fmt::Debug for RollingFileHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RollingFileHandle").finish_non_exhaustive()
    }
}

impl Write for RollingFileHandle {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.inner.with_state(|state| state.file.write_all(buf))?;
        self.inner.note_bytes(buf.len() as u64);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.with_state(|state| state.file.flush())
    }
}

impl RollingInner {
    fn with_state<F, R>(&self, f: F) -> io::Result<R>
    where
        F: FnOnce(&mut RollingState) -> io::Result<R>,
    {
        let mut guard = self.current.lock();
        if guard.is_none() {
            *guard = Some(self.open_new()?);
        }
        let needs_rotate = match guard.as_ref() {
            Some(state) => self.should_rotate(state),
            None => false,
        };
        if needs_rotate {
            *guard = Some(self.open_new()?);
            self.maybe_evict_old();
        }
        let state = guard
            .as_mut()
            .ok_or_else(|| io::Error::other("rolling state missing after open"))?;
        f(state)
    }

    fn note_bytes(&self, n: u64) {
        if let Some(state) = self.current.lock().as_mut() {
            state.bytes += n;
        }
    }

    fn should_rotate(&self, state: &RollingState) -> bool {
        match self.policy {
            RollingPolicy::Never => false,
            RollingPolicy::SizeBased { max_bytes } => state.bytes >= max_bytes,
            RollingPolicy::Daily => {
                let opened = state
                    .opened_at
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() / 86_400)
                    .ok();
                opened != now_unix_secs().map(|s| s / 86_400)
            }
            RollingPolicy::Hourly => {
                let opened = state
                    .opened_at
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_secs() / 3600)
                    .ok();
                opened != now_unix_secs().map(|s| s / 3600)
            }
            RollingPolicy::SizeOrAge { max_bytes, max_age } => {
                if state.bytes >= max_bytes {
                    return true;
                }
                state.opened_at.elapsed().unwrap_or_default() >= max_age
            }
        }
    }

    fn open_new(&self) -> io::Result<RollingState> {
        let now = now_unix_secs().unwrap_or(0);
        let counter = ROLL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let stamp = format!("{now}-{counter}");
        let filename = format!("{}.{stamp}{}", self.prefix, self.suffix);
        let path = self.directory.join(&filename);
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        Ok(RollingState {
            file,
            bytes: 0,
            opened_at: SystemTime::now(),
        })
    }

    fn maybe_evict_old(&self) {
        let Some(keep) = self.keep else { return };
        let Ok(read_dir) = std::fs::read_dir(&self.directory) else {
            return;
        };
        let mut entries: Vec<_> = read_dir
            .filter_map(Result::ok)
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with(&self.prefix) && n.ends_with(&self.suffix))
            })
            .collect();
        entries.sort_by_key(|e| {
            e.metadata()
                .and_then(|m| m.modified())
                .unwrap_or(SystemTime::UNIX_EPOCH)
        });
        if entries.len() > keep {
            let extras = entries.len() - keep;
            for entry in entries.into_iter().take(extras) {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
}

static ROLL_COUNTER: AtomicU64 = AtomicU64::new(0);

fn now_unix_secs() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .ok()
}

// ─── NonBlockingWriter ────────────────────────────────────────────────

const NON_BLOCKING_DEFAULT_CAPACITY: usize = 8192;

/// Wraps a `MakeWriter` with a background thread + bounded
/// `mpsc::SyncSender` channel. Overflow drops the line and increments
/// the dropped counter. Spec 20 § 3.5.
#[derive(Debug, Clone)]
pub struct NonBlockingWriter {
    sender: SyncSender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
}

/// Returned alongside `NonBlockingWriter`; flushes + joins the bg
/// thread on drop.
pub struct WorkerGuard {
    shutdown: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for WorkerGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerGuard").finish_non_exhaustive()
    }
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(j) = self.join.take() {
            let _ = j.join();
        }
    }
}

impl NonBlockingWriter {
    /// Wrap `inner` with a background thread. Returns the
    /// non-blocking writer and a `WorkerGuard` whose `Drop` flushes +
    /// joins.
    pub fn new<M>(inner: M, capacity: usize) -> (Self, WorkerGuard)
    where
        M: MakeWriter,
    {
        let cap = if capacity == 0 {
            NON_BLOCKING_DEFAULT_CAPACITY
        } else {
            capacity
        };
        let (tx, rx) = sync_channel::<Vec<u8>>(cap);
        let dropped = Arc::new(AtomicU64::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_in_thread = Arc::clone(&shutdown);
        let inner = Arc::new(inner);
        let join = thread::spawn(move || run_loop(inner, rx, shutdown_in_thread));
        (
            Self {
                sender: tx,
                dropped,
            },
            WorkerGuard {
                shutdown,
                join: Some(join),
            },
        )
    }

    /// Total bytes dropped due to channel pressure.
    #[must_use]
    pub fn dropped_total(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

fn run_loop<M: MakeWriter>(inner: Arc<M>, rx: mpsc::Receiver<Vec<u8>>, shutdown: Arc<AtomicBool>) {
    while let Ok(buf) = rx.recv_timeout(Duration::from_millis(200)) {
        let mut w = inner.make_writer();
        let _ = w.write_all(&buf);
        let _ = w.flush();
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
    }
    // Drain remaining queued buffers on shutdown.
    while let Ok(buf) = rx.try_recv() {
        let mut w = inner.make_writer();
        let _ = w.write_all(&buf);
        let _ = w.flush();
    }
}

impl MakeWriter for NonBlockingWriter {
    type Writer = NonBlockingHandle;
    fn make_writer(&self) -> NonBlockingHandle {
        NonBlockingHandle {
            sender: self.sender.clone(),
            dropped: Arc::clone(&self.dropped),
            buf: Vec::with_capacity(256),
        }
    }
}

/// Per-batch handle. Buffers bytes, flushes to the bg sender on
/// `flush()` / `Drop`.
pub struct NonBlockingHandle {
    sender: SyncSender<Vec<u8>>,
    dropped: Arc<AtomicU64>,
    buf: Vec<u8>,
}

impl std::fmt::Debug for NonBlockingHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NonBlockingHandle")
            .field("buffered", &self.buf.len())
            .finish()
    }
}

impl Write for NonBlockingHandle {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let buf = std::mem::take(&mut self.buf);
        match self.sender.try_send(buf) {
            Ok(()) => Ok(()),
            Err(mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
    }
}

impl Drop for NonBlockingHandle {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_rotate_size_based() {
        let dir = tempfile::tempdir().unwrap();
        let writer = RollingFileWriter::builder()
            .directory(dir.path())
            .filename_prefix("test")
            .policy(RollingPolicy::SizeBased { max_bytes: 16 })
            .build()
            .unwrap();
        for _ in 0..5 {
            let mut h = writer.make_writer();
            h.write_all(b"hello world!").unwrap();
            h.flush().unwrap();
        }
        let entries: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(Result::ok)
            .collect();
        assert!(
            entries.len() >= 2,
            "expected size-based rotation to produce >1 file"
        );
    }

    #[test]
    fn test_non_blocking_writer_should_flush_on_drop() {
        let captured = Arc::new(parking_lot::Mutex::new(Vec::<u8>::new()));
        struct FakeWriter(Arc<parking_lot::Mutex<Vec<u8>>>);
        impl MakeWriter for FakeWriter {
            type Writer = FakeHandle;
            fn make_writer(&self) -> FakeHandle {
                FakeHandle(Arc::clone(&self.0))
            }
        }
        struct FakeHandle(Arc<parking_lot::Mutex<Vec<u8>>>);
        impl Write for FakeHandle {
            fn write(&mut self, b: &[u8]) -> io::Result<usize> {
                self.0.lock().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let (writer, _guard) = NonBlockingWriter::new(FakeWriter(Arc::clone(&captured)), 16);
        {
            let mut h = writer.make_writer();
            h.write_all(b"hello\n").unwrap();
            h.flush().unwrap();
        }
        // Allow the bg thread to drain.
        std::thread::sleep(Duration::from_millis(50));
        assert!(captured.lock().starts_with(b"hello\n"));
    }
}
