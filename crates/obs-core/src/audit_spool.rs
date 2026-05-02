//! AUDIT spool — binary length-prefixed envelope file with CRC32C tail
//! integrity. Spec 11 § 6.4.
//!
//! Format:
//!
//! ```text
//! audit-spool-record := u32_le_length || ObsEnvelope_buffa_bytes
//! audit-spool-file   := record* (no header)
//! audit-spool-crc    := u32_le crc per record (parallel `.crc` file)
//! ```
//!
//! `std::fs` is used synchronously here because the AUDIT path runs
//! on the emit thread (spec 11 § 6.4 documents the blocking trade-off);
//! switching to `tokio::fs` would require a `block_on` round-trip per
//! envelope, defeating the latency budget.
#![allow(clippy::disallowed_types, clippy::disallowed_methods)]

use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use obs_proto::{__private::Message, obs::v1::ObsEnvelope};
use parking_lot::Mutex;

use crate::config::AuditFailureMode;

/// Polynomial used by CRC-32C / Castagnoli.
const CRC32C_POLY: u32 = 0x82F63B78;

/// Compute CRC32C (Castagnoli). Software implementation, fast enough
/// for the AUDIT path's bounded throughput.
#[must_use]
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &b in data {
        crc ^= u32::from(b);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (CRC32C_POLY & mask);
        }
    }
    !crc
}

/// One spool batch is bounded by record count or wall-clock time;
/// larger of the two is kept simple here.
#[derive(Debug)]
pub struct SpoolWriter {
    inner: Arc<Mutex<SpoolInner>>,
    on_failure: AuditFailureMode,
}

#[derive(Debug)]
struct SpoolInner {
    dir: PathBuf,
    bin: Option<File>,
    crc: Option<File>,
    bin_path: PathBuf,
    crc_path: PathBuf,
    bytes_written: u64,
    max_bytes: u64,
}

impl SpoolWriter {
    /// Open a fresh batch in `dir`. Files are named
    /// `<batch_id>.audit.bin` / `<batch_id>.audit.bin.crc`.
    ///
    /// # Errors
    ///
    /// Returns `io::Error` when the directory cannot be created or the
    /// files cannot be opened for append.
    pub fn open(
        dir: impl Into<PathBuf>,
        max_bytes: u64,
        on_failure: AuditFailureMode,
    ) -> io::Result<Self> {
        let dir: PathBuf = dir.into();
        std::fs::create_dir_all(&dir)?;
        let stamp = batch_stamp();
        let bin_path = dir.join(format!("{stamp}.audit.bin"));
        let crc_path = dir.join(format!("{stamp}.audit.bin.crc"));
        let bin = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&bin_path)?;
        let crc = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&crc_path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(SpoolInner {
                dir,
                bin: Some(bin),
                crc: Some(crc),
                bin_path,
                crc_path,
                bytes_written: 0,
                max_bytes,
            })),
            on_failure,
        })
    }

    /// Append one envelope to the spool. Returns `Err` only when the
    /// underlying write or flush fails.
    ///
    /// # Errors
    ///
    /// I/O errors propagate from the underlying file writes.
    pub fn append(&self, env: &ObsEnvelope) -> io::Result<()> {
        let mut buf = Vec::with_capacity(64 + env.encoded_len() as usize);
        env.encode(&mut buf);
        let len = buf.len() as u32;
        let crc = crc32c(&buf);
        let mut inner = self.inner.lock();
        if inner.bytes_written.saturating_add(buf.len() as u64 + 4) > inner.max_bytes {
            // Surface as a write error; caller decides how to react
            // per `audit.on_failure`.
            return Err(io::Error::other("audit spool full"));
        }
        let bin = inner
            .bin
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "spool bin file missing"))?;
        bin.write_all(&len.to_le_bytes())?;
        bin.write_all(&buf)?;
        bin.flush()?;
        let crc_file = inner
            .crc
            .as_mut()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "spool crc file missing"))?;
        crc_file.write_all(&crc.to_le_bytes())?;
        crc_file.flush()?;
        inner.bytes_written += buf.len() as u64 + 4;
        Ok(())
    }

    /// Close the current batch (the `.audit.bin` is left intact for
    /// the drainer / a later process to recover).
    pub fn close(&self) {
        let mut inner = self.inner.lock();
        inner.bin.take();
        inner.crc.take();
    }

    /// Configured failure mode (used by the AUDIT path to decide
    /// `panic` / `abort` / `warn_only` on append failure).
    #[must_use]
    pub fn on_failure(&self) -> AuditFailureMode {
        self.on_failure
    }

    /// Spool dir (used by tests and the drainer).
    pub fn dir(&self) -> PathBuf {
        self.inner.lock().dir.clone()
    }

    /// Path of the active `.audit.bin` file (test helper).
    pub fn bin_path(&self) -> PathBuf {
        self.inner.lock().bin_path.clone()
    }

    /// Path of the active `.audit.bin.crc` file (test helper).
    pub fn crc_path(&self) -> PathBuf {
        self.inner.lock().crc_path.clone()
    }
}

/// Outcome of recovering one spool file.
#[derive(Debug)]
pub struct RecoveryReport {
    /// Path that was recovered.
    pub path: PathBuf,
    /// Number of valid records.
    pub records: usize,
    /// Number of records dropped due to CRC mismatch / truncation.
    pub dropped: usize,
}

/// Walk `dir` for any `*.audit.bin` files, validate each record's
/// CRC32C, and feed valid records to `consume`. CRC-mismatched tails
/// are discarded; the `.audit.bin` and `.crc` files are deleted only
/// after `consume` returns `Ok(())` for every valid record.
///
/// # Errors
///
/// I/O errors propagate from the underlying directory + file reads.
pub fn recover<F>(dir: &Path, mut consume: F) -> io::Result<Vec<RecoveryReport>>
where
    F: FnMut(ObsEnvelope) -> io::Result<()>,
{
    let mut reports = Vec::new();
    if !dir.exists() {
        return Ok(reports);
    }
    let entries = std::fs::read_dir(dir)?;
    let mut bin_files: Vec<_> = entries
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.ends_with(".audit.bin"))
        })
        .collect();
    bin_files.sort_by_key(|e| {
        e.metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    });
    for entry in bin_files {
        let bin_path = entry.path();
        let crc_path = with_crc_suffix(&bin_path);
        let report = recover_one(&bin_path, &crc_path, &mut consume)?;
        let _ = std::fs::remove_file(&bin_path);
        let _ = std::fs::remove_file(&crc_path);
        reports.push(report);
    }
    Ok(reports)
}

fn with_crc_suffix(bin: &Path) -> PathBuf {
    let mut s = bin.as_os_str().to_os_string();
    s.push(".crc");
    PathBuf::from(s)
}

fn recover_one<F>(bin_path: &Path, crc_path: &Path, consume: &mut F) -> io::Result<RecoveryReport>
where
    F: FnMut(ObsEnvelope) -> io::Result<()>,
{
    let mut bin = File::open(bin_path)?;
    let mut crc = match File::open(crc_path) {
        Ok(f) => Some(f),
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => return Err(e),
    };
    let mut records = 0;
    let mut dropped = 0;
    loop {
        let pos = bin.stream_position()?;
        let mut len_buf = [0u8; 4];
        match bin.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut record = vec![0u8; len];
        match bin.read_exact(&mut record) {
            Ok(()) => {}
            Err(_) => {
                dropped += 1;
                bin.seek(SeekFrom::Start(pos))?;
                break;
            }
        }
        let mut sidecar_buf = [0u8; 4];
        let sidecar = if let Some(c) = crc.as_mut() {
            match c.read_exact(&mut sidecar_buf) {
                Ok(()) => Some(u32::from_le_bytes(sidecar_buf)),
                Err(_) => None,
            }
        } else {
            None
        };
        let actual = crc32c(&record);
        if let Some(expected) = sidecar
            && expected != actual
        {
            dropped += 1;
            continue;
        }
        match ObsEnvelope::decode_from_slice(&record) {
            Ok(env) => {
                consume(env)?;
                records += 1;
            }
            Err(_) => {
                dropped += 1;
            }
        }
    }
    Ok(RecoveryReport {
        path: bin_path.to_path_buf(),
        records,
        dropped,
    })
}

fn batch_stamp() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let pid = std::process::id();
    format!("{nanos:020}-{pid}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_with_name(name: &str) -> ObsEnvelope {
        ObsEnvelope {
            full_name: name.to_string(),
            ts_ns: 1_700_000_000_000_000_000,
            ..Default::default()
        }
    }

    #[test]
    fn test_crc32c_canonical_vector() {
        assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn test_round_trip_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SpoolWriter::open(dir.path(), 1 << 20, AuditFailureMode::WarnOnly).unwrap();
        let envs = (0..5)
            .map(|i| env_with_name(&format!("test.v1.Audit{i}")))
            .collect::<Vec<_>>();
        for env in &envs {
            writer.append(env).unwrap();
        }
        writer.close();
        let mut recovered = Vec::new();
        let reports = recover(dir.path(), |env| {
            recovered.push(env);
            Ok(())
        })
        .unwrap();
        assert_eq!(recovered.len(), 5);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].records, 5);
        assert_eq!(reports[0].dropped, 0);
    }

    #[test]
    fn test_truncated_tail_is_discarded() {
        let dir = tempfile::tempdir().unwrap();
        let writer = SpoolWriter::open(dir.path(), 1 << 20, AuditFailureMode::WarnOnly).unwrap();
        for i in 0..3 {
            writer
                .append(&env_with_name(&format!("test.v1.Trunc{i}")))
                .unwrap();
        }
        let bin_path = writer.bin_path();
        writer.close();
        // Truncate the last record by chopping off the last 8 bytes —
        // simulates a kill -9 between buffa.encode and fsync.
        let mut data = std::fs::read(&bin_path).unwrap();
        data.truncate(data.len() - 8);
        std::fs::write(&bin_path, data).unwrap();
        let mut recovered = Vec::new();
        let _ = recover(dir.path(), |env| {
            recovered.push(env);
            Ok(())
        })
        .unwrap();
        assert!(
            recovered.len() < 3,
            "truncation should drop the partial tail"
        );
    }
}
