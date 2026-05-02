//! `test_audit_spool_recovery` (spec 72 § 7) — the AUDIT spool path
//! recovers records on next observer init.

use obs_core::audit_spool::{SpoolWriter, recover};
use obs_sdk::ObsEnvelope;

#[test]
fn test_audit_spool_round_trip_recovers_envelopes() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SpoolWriter::open(
        dir.path(),
        1 << 20,
        obs_core::config::AuditFailureMode::WarnOnly,
    )
    .unwrap();
    for i in 0..3 {
        let env = ObsEnvelope {
            full_name: format!("test.v1.AuditX{i}"),
            ts_ns: 1_700_000_000_000_000_000 + i as u64,
            ..Default::default()
        };
        writer.append(&env).unwrap();
    }
    writer.close();
    let mut recovered = Vec::new();
    let _ = recover(dir.path(), |env| {
        recovered.push(env);
        Ok(())
    })
    .unwrap();
    assert_eq!(recovered.len(), 3);
}
