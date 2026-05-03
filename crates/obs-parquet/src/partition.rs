//! Partitioning logic for Parquet output paths. Spec 22 § 2 file path
//! convention: `base_dir/service=<svc>/date=YYYY-MM-DD/hour=HH/obs_events-{batch_id}.parquet`.

use std::{path::PathBuf, time::SystemTime};

use obs_proto::obs::v1::ObsEnvelope;

/// Partition key derived from one envelope.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PartitionKey {
    pub service: String,
    pub date: String,
    pub hour: u32,
}

impl PartitionKey {
    pub(crate) fn from_envelope(env: &ObsEnvelope, default_service: &str) -> Self {
        let service = if env.service.is_empty() {
            default_service.to_string()
        } else {
            env.service.clone()
        };
        let secs = (env.ts_ns / 1_000_000_000) as i64;
        let (date, hour) = format_date_hour(secs);
        Self {
            service,
            date,
            hour,
        }
    }

    pub(crate) fn dir(&self, base: &std::path::Path, fields: &[&str]) -> PathBuf {
        let mut p = base.to_path_buf();
        for f in fields {
            match *f {
                "service" => p.push(format!("service={}", self.service)),
                "date" => p.push(format!("date={}", self.date)),
                "hour" => p.push(format!("hour={:02}", self.hour)),
                _ => p.push(format!("{f}=_")),
            }
        }
        p
    }
}

/// `secs` is the unix epoch seconds for the envelope.
/// Returns `(YYYY-MM-DD, hour-in-day)`.
pub(crate) fn format_date_hour(secs: i64) -> (String, u32) {
    let secs = if secs >= 0 { secs as u64 } else { 0 };
    let total_days = secs / 86_400;
    let secs_today = (secs % 86_400) as u32;
    let hour = secs_today / 3_600;
    let (year, month, day) = days_to_ymd(total_days);
    (format!("{year:04}-{month:02}-{day:02}"), hour)
}

/// Convert days-since-1970-01-01 to (year, month, day). Implementation
/// avoids pulling in `chrono`; works for dates 1970..9999.
fn days_to_ymd(mut days: u64) -> (i32, u32, u32) {
    // Algorithm: Howard Hinnant "civil_from_days".
    days += 719_468;
    let era = days / 146_097;
    let doe = days % 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = (yoe + era * 400) as i32;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

/// Wall-clock helper used when an envelope's `ts_ns` is zero (event
/// produced before the runtime stamped a timestamp).
pub(crate) fn now_seconds() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_date_hour_should_match_known_dates() {
        // 1_777_731_600 unix seconds = 2026-05-02 14:20:00 UTC.
        let (date, hour) = format_date_hour(1_777_731_600);
        assert_eq!(date, "2026-05-02");
        assert_eq!(hour, 14);
    }

    #[test]
    fn test_partition_dir_should_render_path() {
        let key = PartitionKey {
            service: "api".to_string(),
            date: "2026-05-02".to_string(),
            hour: 14,
        };
        let p = key.dir(std::path::Path::new("/data"), &["service", "date", "hour"]);
        assert_eq!(
            p.to_string_lossy(),
            "/data/service=api/date=2026-05-02/hour=14"
        );
    }
}
