//! Envelope-framed spool used by [`BatchingSink`](crate::BatchingSink).
//!
//! The spool holds pre-encoding envelopes. Each spool file is a
//! sequence of length-prefixed frames (see
//! [`obs_core::wire::envelope_codec`]). Recovery walks the file and
//! re-admits each envelope through the normal ingress path.
//!
//! Per-partition file layout:
//!
//! ```text
//! {spool_root}/{backend_id}/{partition_hash_hex}/{ts_ms}-{uuid}.spool
//! ```
//!
//! A `failed/` subtree under `{spool_root}/failed/{backend_id}/…`
//! holds records that exceeded `escalate_after`. The framework never
//! re-reads those; operators drain them via external tooling.

use std::{
    io::{self, ErrorKind},
    path::{Path, PathBuf},
};

use buffa::Message;
use obs_core::wire::envelope_codec;
use obs_proto::obs::v1::ObsEnvelope;
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncWriteExt},
};

use crate::config::{FsyncMode, SpoolConfig};

/// One envelope persisted to the spool. The file it was recovered
/// from is carried alongside so the caller can delete or escalate it.
#[derive(Debug)]
pub struct SpoolRecord {
    /// Absolute file path the record was read from.
    pub path: PathBuf,
    /// Envelopes recovered from that file, in order.
    pub envelopes: Vec<ObsEnvelope>,
    /// Millis since the Unix epoch from the filename prefix. Used by
    /// the framework to compute age when escalating.
    pub first_failed_at_ms: i64,
    /// Partition hash hex (the containing subdirectory name).
    pub partition_hex: String,
}

/// Errors surfaced by the spool subsystem.
#[derive(Debug, Error)]
pub enum SpoolError {
    /// Underlying filesystem failure.
    #[error("spool io: {0}")]
    Io(#[from] io::Error),
    /// Envelope encoding exceeded the 4 GiB frame limit.
    #[error("envelope too large: {size} bytes")]
    EnvelopeTooLarge {
        /// Envelope encoded size.
        size: u64,
    },
    /// Envelope decode failed during recovery (malformed frame).
    #[error("spool decode: {0}")]
    Decode(String),
}

/// Bounded envelope-framed spool for one [`BatchingSink`](crate::BatchingSink).
#[derive(Debug)]
pub(crate) struct Spool {
    backend_id: &'static str,
    root: PathBuf,
    max_bytes: u64,
    fsync_mode: FsyncMode,
}

impl Spool {
    /// Open the spool rooted at `{config.root}/{backend_id}`. Creates
    /// the directory if missing.
    pub(crate) async fn open(
        backend_id: &'static str,
        config: &SpoolConfig,
    ) -> Result<Self, SpoolError> {
        let root = config.root.join(backend_id);
        fs::create_dir_all(&root).await?;
        Ok(Self {
            backend_id,
            root,
            max_bytes: config.max_bytes,
            fsync_mode: config.fsync_mode,
        })
    }

    /// Write one partition's batch of envelopes to a new spool file.
    /// Returns the path on success.
    pub(crate) async fn write(
        &self,
        partition_hex: &str,
        envelopes: &[ObsEnvelope],
    ) -> Result<PathBuf, SpoolError> {
        let dir = self.root.join(partition_hex);
        fs::create_dir_all(&dir).await?;
        let ts_ms = now_ms();
        let uuid = uuid::Uuid::new_v4().simple().to_string();
        let fname = format!("{ts_ms}-{uuid}.spool");
        let path = dir.join(&fname);

        let mut buf: Vec<u8> = Vec::new();
        let mut cache = buffa::SizeCache::new();
        for env in envelopes {
            // Reject envelopes whose encoded length won't fit in a
            // u32 frame header. The framework itself guards this at
            // admit time; belt-and-braces here so a malformed caller
            // cannot write a file we cannot recover.
            let size = u64::from(env.compute_size(&mut cache));
            if size > u64::from(u32::MAX) {
                return Err(SpoolError::EnvelopeTooLarge { size });
            }
            envelope_codec::encode_into_with_cache(env, &mut buf, &mut cache);
        }

        let mut f = fs::File::create(&path).await?;
        f.write_all(&buf).await?;
        if matches!(self.fsync_mode, FsyncMode::Fsync) {
            f.sync_all().await?;
        }
        drop(f);

        // Enforce the cap after each write so a burst cannot balloon
        // the spool beyond a short overshoot.
        let _ = self.evict_to_cap().await;

        Ok(path)
    }

    /// Walk every spool file on disk (oldest first) and return the
    /// decoded contents. Directory scan is O(total files); used at
    /// startup and by the background retry task.
    pub(crate) async fn list(&self) -> Result<Vec<SpoolRecord>, SpoolError> {
        let mut entries: Vec<(std::time::SystemTime, PathBuf, String)> = Vec::new();
        self.collect_entries(&self.root, &mut entries).await?;
        entries.sort_by_key(|(t, _, _)| *t);

        let mut out = Vec::with_capacity(entries.len());
        for (_, path, partition_hex) in entries {
            match Self::read_file(&path, &partition_hex).await {
                Ok(rec) => out.push(rec),
                Err(e) => {
                    eprintln!(
                        "obs-sink-batch WARN: dropping corrupt spool file path={} err={e}",
                        path.display()
                    );
                    let _ = fs::remove_file(&path).await;
                }
            }
        }
        Ok(out)
    }

    /// Remove one spool file (after successful re-ship).
    pub(crate) async fn remove(&self, path: &Path) -> Result<(), SpoolError> {
        match fs::remove_file(path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Move a stuck spool file into the `failed/` subtree so the
    /// retry loop stops trying to upload it. Returns the destination
    /// path when the move succeeded.
    pub(crate) async fn move_to_failed(&self, path: &Path) -> Result<Option<PathBuf>, SpoolError> {
        // `{spool_root}/failed/{backend_id}/{partition_hex}/<filename>`.
        // The partition subtree is preserved so operators can tell
        // whose batches keep failing.
        let failed_root = self
            .root
            .parent()
            .unwrap_or(Path::new("."))
            .join("failed")
            .join(self.backend_id);
        let partition = path
            .parent()
            .and_then(|p| p.file_name())
            .map(Path::new)
            .unwrap_or(Path::new(""));
        let dir = failed_root.join(partition);
        fs::create_dir_all(&dir).await?;
        let Some(file) = path.file_name() else {
            return Ok(None);
        };
        let dest = dir.join(file);
        match fs::rename(path, &dest).await {
            Ok(()) => Ok(Some(dest)),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Current on-disk usage across the entire backend subtree.
    pub(crate) async fn size_bytes(&self) -> u64 {
        measure_dir(&self.root).await.unwrap_or(0)
    }

    async fn evict_to_cap(&self) -> Result<(), SpoolError> {
        let mut size = self.size_bytes().await;
        if size <= self.max_bytes {
            return Ok(());
        }
        let mut entries: Vec<(std::time::SystemTime, PathBuf, String)> = Vec::new();
        self.collect_entries(&self.root, &mut entries).await?;
        entries.sort_by_key(|(t, _, _)| *t);
        for (_, path, _) in entries {
            if size <= self.max_bytes {
                break;
            }
            if let Ok(meta) = fs::metadata(&path).await {
                size = size.saturating_sub(meta.len());
            }
            let _ = fs::remove_file(&path).await;
        }
        Ok(())
    }

    async fn collect_entries(
        &self,
        root: &Path,
        out: &mut Vec<(std::time::SystemTime, PathBuf, String)>,
    ) -> Result<(), SpoolError> {
        let mut rd = match fs::read_dir(root).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();
            if path.is_dir() {
                // Skip the sibling `failed/` tree — owned by
                // `move_to_failed`, not the retry loop. The partition
                // subtrees are plain `{hex}/` — recurse one level.
                if name_str == "failed" {
                    continue;
                }
                self.collect_dir_files(&path, &name_str, out).await?;
            }
        }
        Ok(())
    }

    async fn collect_dir_files(
        &self,
        dir: &Path,
        partition_hex: &str,
        out: &mut Vec<(std::time::SystemTime, PathBuf, String)>,
    ) -> Result<(), SpoolError> {
        let mut rd = fs::read_dir(dir).await?;
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("spool") {
                continue;
            }
            let meta = entry.metadata().await.ok();
            let ts = meta
                .and_then(|m| m.created().or_else(|_| m.modified()).ok())
                .unwrap_or(std::time::UNIX_EPOCH);
            out.push((ts, path, partition_hex.to_string()));
        }
        Ok(())
    }

    async fn read_file(path: &Path, partition_hex: &str) -> Result<SpoolRecord, SpoolError> {
        let mut f = fs::File::open(path).await?;
        let mut buf = Vec::with_capacity(64 * 1024);
        f.read_to_end(&mut buf).await?;
        let mut envelopes = Vec::new();
        let mut offset = 0;
        while offset < buf.len() {
            let Some(remainder) = buf.get(offset..) else {
                break;
            };
            match envelope_codec::decode_frame(remainder, 1 << 30) {
                Ok(Some((env, consumed))) => {
                    envelopes.push(env);
                    offset += consumed;
                }
                Ok(None) => {
                    return Err(SpoolError::Decode(format!(
                        "short frame at offset {offset} (file={})",
                        path.display()
                    )));
                }
                Err(e) => return Err(SpoolError::Decode(e.to_string())),
            }
        }
        let first_failed_at_ms = parse_timestamp(path);
        Ok(SpoolRecord {
            path: path.to_path_buf(),
            envelopes,
            first_failed_at_ms,
            partition_hex: partition_hex.to_string(),
        })
    }
}

/// Parse the `{ts_ms}-{uuid}.spool` filename back into millis since
/// the Unix epoch. Falls back to `0` when the filename doesn't match
/// the expected shape — recovery should not fail for a missing
/// timestamp.
fn parse_timestamp(path: &Path) -> i64 {
    path.file_stem()
        .and_then(|s| s.to_str())
        .and_then(|s| s.split_once('-'))
        .and_then(|(ts, _)| ts.parse::<i64>().ok())
        .unwrap_or(0)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

async fn measure_dir(root: &Path) -> Result<u64, SpoolError> {
    let mut total = 0u64;
    let mut stack: Vec<PathBuf> = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut rd = match fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = rd.next_entry().await? {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if let Ok(meta) = entry.metadata().await {
                total += meta.len();
            }
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use buffa::EnumValue;
    use obs_proto::obs::v1::{Severity as PSeverity, Tier as PTier};
    use tempfile::tempdir;

    use super::*;

    fn sample_env(name: &str) -> ObsEnvelope {
        ObsEnvelope {
            full_name: name.to_string(),
            tier: EnumValue::Known(PTier::TIER_LOG),
            sev: EnumValue::Known(PSeverity::SEVERITY_INFO),
            ts_ns: 1,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_write_and_list_roundtrip() {
        let dir = tempdir().unwrap();
        let cfg = SpoolConfig {
            root: dir.path().to_path_buf(),
            max_bytes: 1 << 20,
            ..SpoolConfig::default()
        };
        let spool = Spool::open("testbackend", &cfg).await.unwrap();

        let envs = vec![sample_env("Evt.A"), sample_env("Evt.B")];
        let path = spool.write("deadbeef", &envs).await.unwrap();
        assert!(path.exists());

        let records = spool.list().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].envelopes.len(), 2);
        assert_eq!(records[0].envelopes[0].full_name, "Evt.A");
        assert_eq!(records[0].envelopes[1].full_name, "Evt.B");
        assert_eq!(records[0].partition_hex, "deadbeef");

        spool.remove(&path).await.unwrap();
        let after = spool.list().await.unwrap();
        assert!(after.is_empty());
    }

    #[tokio::test]
    async fn test_move_to_failed_moves_file() {
        let dir = tempdir().unwrap();
        let cfg = SpoolConfig {
            root: dir.path().to_path_buf(),
            max_bytes: 1 << 20,
            ..SpoolConfig::default()
        };
        let spool = Spool::open("backend", &cfg).await.unwrap();
        let path = spool.write("cafebabe", &[sample_env("X")]).await.unwrap();

        let failed = spool.move_to_failed(&path).await.unwrap().expect("moved");
        assert!(failed.exists());
        assert!(!path.exists());
        let after = spool.list().await.unwrap();
        assert!(after.is_empty(), "failed/ tree is excluded from list()");
    }

    #[tokio::test]
    async fn test_evict_to_cap_keeps_newest() {
        let dir = tempdir().unwrap();
        let cfg = SpoolConfig {
            root: dir.path().to_path_buf(),
            // Tiny cap — each envelope is >10 B encoded, so writing
            // three forces at least one eviction.
            max_bytes: 64,
            ..SpoolConfig::default()
        };
        let spool = Spool::open("b", &cfg).await.unwrap();
        let _ = spool.write("p1", &[sample_env("AAAAA")]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let _ = spool.write("p1", &[sample_env("BBBBB")]).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let _ = spool.write("p1", &[sample_env("CCCCC")]).await.unwrap();

        let size = spool.size_bytes().await;
        assert!(size <= cfg.max_bytes * 2, "size={size}");
    }
}
