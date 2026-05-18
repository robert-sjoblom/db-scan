use std::collections::HashMap;

use crate::v2::{
    analyze::{ClusterVerdict, LAG_THRESHOLD_BYTES, NodeVerdict, Verdict},
    scan::{
        AnalyzedNode, Role,
        disk_check::DiskCheckOutcome,
        health_check_primary::{PgSyncSettings, PrimaryHealthCheckResult},
    },
};

///   Check if archive command has never succeeded since this node became primary.
///
/// Returns `Some((failed_count`, `last_failed_wal`)) if:
/// - `archive_mode` = "on"
/// - `archived_count` = 0 (no successful archives)
/// - `failed_count` > 0 (there have been failures)
///
/// Also emits a Degraded `ArchiveLagging` verdict when archiving has succeeded
/// before but the most recent attempt failed (`last_failed_time >
/// last_archived_time`). WAL is retained until the next push succeeds, so this
/// is a warning, not a durability outage.
pub(super) fn check_archive(primary: &AnalyzedNode, verdict: &mut Verdict) {
    let Role::Primary { health } = &primary.role else {
        return;
    };

    let archive_mode = health.configuration.get("archive_mode").map(String::as_str);
    if let Some("on" | "always") = archive_mode {
    } else {
        verdict
            .node_verdicts
            .push((primary.node_name.clone(), NodeVerdict::ArchivingDisabled));
        return;
    }

    let archiver = &health.archiver;
    if archiver.archived_count == 0 && archiver.failed_count > 0 {
        verdict.push_node_verdict(
            primary.node_name.clone(),
            NodeVerdict::ArchiveFailure {
                failed_count: archiver.failed_count,
                last_wal: archiver.last_failed_wal.clone(),
                last_failed_at: archiver.last_failed_time,
            },
        );
        return;
    }

    if let (Some(failed), Some(archived)) = (archiver.last_failed_time, archiver.last_archived_time)
        && failed > archived
    {
        verdict.push_node_verdict(
            primary.node_name.clone(),
            NodeVerdict::ArchiveLagging {
                failed_count: archiver.failed_count,
                last_wal: archiver.last_failed_wal.clone(),
                last_failed_at: archiver.last_failed_time,
                last_archived_at: archiver.last_archived_time,
            },
        );
    }
}

/// - OR `synchronous_standby_names` is empty (even with `remote_apply`, writes won't block)
///
/// See: <https://postgresqlco.nf/doc/en/param/synchronous_commit>/.
pub(super) fn check_sync_commit(primary: &AnalyzedNode, verdict: &mut Verdict) {
    let Role::Primary { health } = &primary.role else {
        return;
    };

    if is_sync_commit_off(health) || is_standby_names_empty(health) {
        verdict.push_node_verdict(primary.node_name.clone(), NodeVerdict::SyncCommitOff);
    }
}

/// Detects whether the primary will block writes waiting for synchronous
/// replication acks that no replica can provide.
///
/// Both [`check_writes_blocked`] and [`check_writes_unprotected`] flow from the
/// same Postgres semantics. The truth table below covers the combinations of
/// `synchronous_commit`, `synchronous_standby_names`, and replica state:
///
/// | `sync_commit` | `standby_names` | streaming replicas | reachable quorum | postgres behavior        | cluster verdict       |
/// |-------------|---------------|--------------------|------------------|--------------------------|-----------------------|
/// | off / local | any           | yes                | n/a              | writes don't wait        | none (`SyncCommitOff` per node) |
/// | off / local | any           | no                 | n/a              | writes don't wait        | `WritesUnprotected`   |
/// | on / remote_*| empty        | yes                | n/a              | writes don't wait (no candidate) | none (`SyncCommitOff` per node) |
/// | on / remote_*| empty        | no                 | n/a              | writes don't wait        | `WritesUnprotected`   |
/// | on / remote_*| populated    | no                 | n/a              | writes wait + nothing acks | `WritesBlocked`     |
/// | on / remote_*| populated    | yes, all non-quorum| no               | writes wait + block      | `WritesBlocked`       |
/// | on / remote_*| populated    | yes, ≥1 quorum     | yes              | writes wait + ack        | none (OK)             |
///
/// Two properties make this check correct:
///
/// 1. Empty `synchronous_standby_names` short-circuits — postgres can't block
///    on a candidate that doesn't exist, so this scenario is unprotected, not
///    blocked.
/// 2. `sync_commit=off`/`local` is unprotected by definition; let
///    [`check_writes_unprotected`] handle it.
///
/// The vacuous-true `0 == 0` from an empty `pg_stat_replication` (no replicas)
/// is *correct* once `standby_names` is populated — postgres really will wait
/// forever for an ack from a replica that doesn't exist.
pub(super) fn check_writes_blocked(primary: &AnalyzedNode, verdict: &mut Verdict) {
    let Role::Primary { health } = &primary.role else {
        return;
    };

    if is_sync_commit_off(health) || is_standby_names_empty(health) {
        return; // sync replication effectively disabled → unprotected, not blocked
    }

    // When standby_names is populated but no replica is in quorum (whether
    // via empty replication list or all non-quorum sync_state), postgres
    // waits forever for an ack that won't come → writes block.
    if find_non_quorum_replicas(primary).len() == health.replication.len() {
        verdict.cluster_verdict = Some(ClusterVerdict::WritesBlocked);
    }
}

/// Fires when sync replication is effectively disabled at the primary AND no
/// streaming replica is observing writes. See the truth table on
/// [`check_writes_blocked`] for the full matrix.
pub(super) fn check_writes_unprotected(
    primary: &AnalyzedNode,
    replicas: &[&AnalyzedNode],
    verdict: &mut Verdict,
) {
    let Role::Primary { health } = &primary.role else {
        return;
    };

    let sync_disabled = is_sync_commit_off(health) || is_standby_names_empty(health);
    if !sync_disabled {
        return;
    }

    let any_streaming = replicas.iter().any(|r| is_replica_streaming(r));
    if any_streaming {
        return;
    }
    verdict.cluster_verdict = Some(ClusterVerdict::WritesUnprotected);
}

/// Check if this node is a failover node (not db001).
pub(super) fn check_failover(primary: &AnalyzedNode, verdict: &mut Verdict) {
    // Node naming convention: env-pg-appXXX-dbYYY.zone.example.com
    // db001 is the original primary, db002/db003 are replicas
    // If db002 or db003 is primary, failover has occurred
    if !primary.node_name.contains("-db001") {
        verdict.push_node_verdict(primary.node_name.clone(), NodeVerdict::IsFailoverNode);
    }
}

/// Check how much the replicas lag behind the primary.
pub(super) fn check_lag(primary: &AnalyzedNode, replicas: &[&AnalyzedNode], verdict: &mut Verdict) {
    let Role::Primary { health } = &primary.role else {
        return;
    };

    for r in &health.replication {
        let backup_stream = matches!(
            r.application_name.as_str(),
            "pg_basebackup" | "pg_dump" | "pg_dumpall"
        );
        if !r.state.is_streaming() || backup_stream {
            continue;
        }

        // Replay LSN is null only briefly during startup, flush_lsn is the next-best signal.
        let effective_lsn = r.replay_lsn.as_deref().or(r.flush_lsn.as_deref());
        let Some(sent) = r.sent_lsn.as_deref() else {
            continue;
        };
        let Some(replay) = effective_lsn else {
            continue;
        };
        let Some(bytes) = pg_lsn_diff(sent, replay) else {
            continue;
        };

        if bytes < LAG_THRESHOLD_BYTES {
            continue;
        }

        let Some(client_addr) = r.client_addr.as_deref() else {
            continue;
        };

        let Some(replica_node) = replicas
            .iter()
            .find(|n| n.ip_address.to_string() == client_addr)
        else {
            continue;
        };

        verdict.push_node_verdict(
            replica_node.node_name.clone(),
            NodeVerdict::HighLag { bytes },
        );
    }
}

pub(super) fn check_quorum(
    primary: &AnalyzedNode,
    replicas: &[&AnalyzedNode],
    verdict: &mut Verdict,
) {
    let Role::Primary { health } = &primary.role else {
        return;
    };

    for r in &health.replication {
        if matches!(r.sync_state, PgSyncSettings::Quorum) {
            continue;
        }
        let Some(client_addr) = r.client_addr.as_deref() else {
            continue;
        };
        let Some(replica_node) = replicas
            .iter()
            .find(|n| n.ip_address.to_string() == client_addr)
        else {
            continue;
        };

        verdict.push_node_verdict(replica_node.node_name.clone(), NodeVerdict::NotInQuorum);
    }
}

/// Detect if any replica is replicating from another replica instead of the primary.
///
/// Returns information about the first chained replica found, if any.
pub(super) fn check_chained_replication(
    primary: &AnalyzedNode,
    replicas: &[&AnalyzedNode],
    verdict: &mut Verdict,
) {
    let primary_ip = primary.ip_address.to_string();

    // Build a map of replica IPs to replica names for lookup
    let replica_ips: HashMap<String, &str> = replicas
        .iter()
        .map(|r| (r.ip_address.to_string(), r.node_name.as_str()))
        .collect();

    for replica in replicas {
        if let Role::Replica { health } = &replica.role
            && let Some(wal_receiver) = &health.wal_receiver
        {
            let sender_ip = &wal_receiver.sender_host;

            // If sender_host is not the primary's IP, check if it's another replica
            if sender_ip != &primary_ip
                && let Some(&upstream_name) = replica_ips.get(sender_ip)
            {
                verdict.push_node_verdict(
                    replica.node_name.clone(),
                    NodeVerdict::ChainedReplication {
                        upstream: upstream_name.to_owned(),
                    },
                );
            }
        }
    }
}

pub(super) fn check_disk_errors(node: &AnalyzedNode, verdict: &mut Verdict) {
    let Some(DiskCheckOutcome::Checked(result)) = &node.disk_check else {
        return;
    };

    if result.io_errors > 0 || result.block_errors > 0 {
        verdict.push_node_verdict(
            node.node_name.clone(),
            NodeVerdict::DiskIoErrors {
                io: result.io_errors,
                block: result.block_errors,
            },
        );
    }

    if result.filesystem_errors > 0 {
        verdict.push_node_verdict(
            node.node_name.clone(),
            NodeVerdict::FilesystemErrors {
                count: result.filesystem_errors,
            },
        );
    }
}

pub(super) fn check_streaming(node: &AnalyzedNode, verdict: &mut Verdict) {
    let Role::Replica { health } = &node.role else {
        return;
    };

    if health.wal_receiver.is_none() {
        verdict.push_node_verdict(node.node_name.clone(), NodeVerdict::NotStreaming);
    }
}

/// Flags a node that is in inventory but not reachable for health checks.
/// Maps to `Reason::ReducedRedundancy` at the cluster level when at least one
/// node is unreachable (Degraded; cluster still operational).
///
/// If the primary's `pg_stat_replication` shows the node actively streaming,
/// the primary's view is authoritative — the node is reachable enough for
/// redundancy purposes even if our scanner couldn't connect directly.
pub(super) fn check_unreachable(
    node: &AnalyzedNode,
    primary: &AnalyzedNode,
    verdict: &mut Verdict,
) {
    if !matches!(node.role, Role::Unknown) {
        return;
    }
    if primary_sees_streaming(primary, node) {
        return;
    }
    verdict.push_node_verdict(node.node_name.clone(), NodeVerdict::Unreachable);
}

fn primary_sees_streaming(primary: &AnalyzedNode, node: &AnalyzedNode) -> bool {
    let Role::Primary { health } = &primary.role else {
        return false;
    };
    let node_ip = node.ip_address.to_string();
    health
        .replication
        .iter()
        .any(|c| c.state.is_streaming() && c.client_addr.as_deref() == Some(node_ip.as_str()))
}

/// Calculate byte difference between two `PostgreSQL` LSNs
/// LSN format: "XXX/YYYYYYYY" where both parts are hexadecimal
/// Returns None if LSNs are invalid.
pub(super) fn pg_lsn_diff(lsn1: &str, lsn2: &str) -> Option<u64> {
    pub(super) fn parse_lsn(lsn: &str) -> Option<u64> {
        let parts: Vec<&str> = lsn.split('/').collect();
        if parts.len() != 2 {
            return None;
        }
        let high = u64::from_str_radix(parts[0], 16).ok()?;
        let low = u64::from_str_radix(parts[1], 16).ok()?;
        Some((high << 32) | low)
    }

    let pos1 = parse_lsn(lsn1)?;
    let pos2 = parse_lsn(lsn2)?;

    // Return absolute difference
    Some(pos1.abs_diff(pos2))
}

fn is_sync_commit_off(health: &PrimaryHealthCheckResult) -> bool {
    health
        .configuration
        .get("synchronous_commit")
        .is_some_and(|v| v == "off" || v == "local")
}

fn is_standby_names_empty(health: &PrimaryHealthCheckResult) -> bool {
    health
        .configuration
        .get("synchronous_standby_names")
        .is_some_and(String::is_empty)
}

/// Returns `application_names` of replicas whose `sync_state` is not Quorum.
fn find_non_quorum_replicas(primary: &AnalyzedNode) -> Vec<String> {
    let Role::Primary { health } = &primary.role else {
        return Vec::new();
    };
    health
        .replication
        .iter()
        .filter(|conn| !matches!(conn.sync_state, PgSyncSettings::Quorum))
        .map(|conn| conn.application_name.clone())
        .collect()
}

/// Check if a replica is actively streaming (has `wal_receiver`).
fn is_replica_streaming(node: &AnalyzedNode) -> bool {
    if let Role::Replica { health } = &node.role {
        health.wal_receiver.is_some()
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::v2::{
        analyze::cluster_state_tests::{
            checked, make_node, make_primary_health, make_primary_health_with_config,
            make_replica_health,
        },
        scan::{
            disk_check::DiskCheckOutcome,
            health_check_primary::{ArchiverStats, PgSyncSettings, ReplicationState},
        },
        tests_common::{PrimaryHealthBuilder, ReplicaHealthBuilder},
    };
    use chrono::Duration;
    use rstest::rstest;
    use std::net::Ipv4Addr;

    use super::*;

    fn primary_with_sync_commit(value: Option<&str>) -> AnalyzedNode {
        let mut config = HashMap::new();
        if let Some(v) = value {
            config.insert("synchronous_commit".to_owned(), v.to_owned());
        }
        make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        )
    }

    fn has_verdict(verdict: &Verdict, expected: &NodeVerdict) -> bool {
        verdict
            .node_verdicts
            .iter()
            .any(|(_, v)| std::mem::discriminant(v) == std::mem::discriminant(expected))
    }

    fn empty_archiver() -> ArchiverStats {
        ArchiverStats {
            archived_count: 0,
            failed_count: 0,
            last_archived_wal: None,
            last_archived_time: None,
            last_failed_wal: None,
            last_failed_time: None,
        }
    }

    fn primary_node(health: PrimaryHealthCheckResult) -> AnalyzedNode {
        make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(health),
            },
        )
    }

    #[rstest]
    #[case::off("off", true)]
    #[case::local("local", true)]
    #[case::on("on", false)]
    #[case::remote_write("remote_write", false)]
    #[case::remote_apply("remote_apply", false)]
    fn check_sync_commit_flags_unprotected_modes(
        #[case] sync_commit: &str,
        #[case] should_push: bool,
    ) {
        let mut verdict = Verdict::default();
        let node = primary_with_sync_commit(Some(sync_commit));

        check_sync_commit(&node, &mut verdict);

        assert_eq!(
            has_verdict(&verdict, &NodeVerdict::SyncCommitOff),
            should_push,
        );
    }

    #[test]
    fn check_sync_commit_silent_when_config_missing() {
        let mut verdict = Verdict::default();
        let node = primary_with_sync_commit(None);

        check_sync_commit(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_sync_commit_silent_for_replica() {
        let mut verdict = Verdict::default();
        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Replica {
                health: Box::new(make_replica_health()),
            },
        );

        check_sync_commit(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_sync_commit_flags_empty_standby_names() {
        let mut config = HashMap::new();
        config.insert("synchronous_commit".to_owned(), "on".to_owned());
        config.insert("synchronous_standby_names".to_owned(), String::new());
        let mut verdict = Verdict::default();
        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health_with_config(0, None, config)),
            },
        );

        check_sync_commit(&node, &mut verdict);

        assert!(has_verdict(&verdict, &NodeVerdict::SyncCommitOff));
    }

    #[rstest]
    #[case::dev_db001("dev-pg-app001-db001.sto1.example.com", false)]
    #[case::prod_db001("prod-pg-app007-db001.sto2.example.com", false)]
    #[case::dev_db002("dev-pg-app001-db002.sto1.example.com", true)]
    #[case::prod_db002("prod-pg-app007-db002.sto2.example.com", true)]
    #[case::dev_db003("dev-pg-app001-db003.sto1.example.com", true)]
    #[case::prod_db003("prod-pg-app007-db003.sto3.example.com", true)]
    fn check_failover_flags_non_db001(#[case] node_name: &str, #[case] should_push: bool) {
        let mut verdict = Verdict::default();
        let node = make_node(
            1,
            node_name,
            Role::Primary {
                health: Box::new(make_primary_health(0, None)),
            },
        );

        check_failover(&node, &mut verdict);

        assert_eq!(
            has_verdict(&verdict, &NodeVerdict::IsFailoverNode),
            should_push,
        );
    }

    #[rstest]
    #[case::on_mode("on", false)]
    #[case::always_mode("always", false)]
    #[case::off_mode("off", true)]
    fn check_archive_flags_disabled_for_non_on_modes(
        #[case] mode: &str,
        #[case] should_flag: bool,
    ) {
        let mut h = PrimaryHealthBuilder::new().build();
        h.configuration
            .insert("archive_mode".to_owned(), mode.to_owned());
        let mut verdict = Verdict::default();

        check_archive(&primary_node(h), &mut verdict);

        assert_eq!(
            has_verdict(&verdict, &NodeVerdict::ArchivingDisabled),
            should_flag,
        );
    }

    #[test]
    fn check_archive_flags_disabled_when_mode_missing() {
        let mut h = PrimaryHealthBuilder::new().build();
        h.configuration.remove("archive_mode");
        let mut verdict = Verdict::default();

        check_archive(&primary_node(h), &mut verdict);

        assert!(has_verdict(&verdict, &NodeVerdict::ArchivingDisabled));
    }

    #[test]
    fn check_archive_flags_failure_when_archives_failing() {
        let archiver = ArchiverStats {
            failed_count: 5,
            last_failed_wal: Some("000000010000000000000042".to_owned()),
            ..empty_archiver()
        };
        let h = PrimaryHealthBuilder::new().with_archiver(archiver).build();
        let mut verdict = Verdict::default();

        check_archive(&primary_node(h), &mut verdict);

        assert!(has_verdict(
            &verdict,
            &NodeVerdict::ArchiveFailure {
                failed_count: 0,
                last_wal: None,
                last_failed_at: None,
            },
        ));
    }

    #[test]
    fn check_archive_silent_when_archiving_succeeded_at_least_once() {
        let archiver = ArchiverStats {
            archived_count: 100,
            failed_count: 5, // recent failures, but archiving has succeeded before
            ..empty_archiver()
        };
        let h = PrimaryHealthBuilder::new().with_archiver(archiver).build();
        let mut verdict = Verdict::default();

        check_archive(&primary_node(h), &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_archive_flags_lagging_when_last_failure_newer_than_last_archive() {
        let archived_at = chrono::Utc::now() - Duration::minutes(20);
        let failed_at = chrono::Utc::now() - Duration::minutes(2);
        let stats = ArchiverStats {
            archived_count: 100,
            failed_count: 3,
            last_archived_wal: Some("000000010000000000000040".to_owned()),
            last_archived_time: Some(archived_at),
            last_failed_wal: Some("000000010000000000000042".to_owned()),
            last_failed_time: Some(failed_at),
        };
        let h = PrimaryHealthBuilder::new().with_archiver(stats).build();
        let mut verdict = Verdict::default();

        check_archive(&primary_node(h), &mut verdict);

        assert!(has_verdict(
            &verdict,
            &NodeVerdict::ArchiveLagging {
                failed_count: 0,
                last_wal: None,
                last_failed_at: None,
                last_archived_at: None,
            },
        ));
    }

    #[test]
    fn check_archive_silent_when_last_archive_newer_than_last_failure() {
        let failed_at = chrono::Utc::now() - Duration::hours(2);
        let archived_at = chrono::Utc::now() - Duration::minutes(2);
        let stats = ArchiverStats {
            archived_count: 100,
            failed_count: 3,
            last_archived_wal: Some("000000010000000000000042".to_owned()),
            last_archived_time: Some(archived_at),
            last_failed_wal: Some("000000010000000000000040".to_owned()),
            last_failed_time: Some(failed_at),
        };
        let h = PrimaryHealthBuilder::new().with_archiver(stats).build();
        let mut verdict = Verdict::default();

        check_archive(&primary_node(h), &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_archive_silent_for_replica() {
        let mut verdict = Verdict::default();
        let node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Replica {
                health: Box::new(make_replica_health()),
            },
        );

        check_archive(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    fn primary_with(
        sync_commit: Option<&str>,
        standby_names: Option<&str>,
        replication_count: usize,
        sync_state: PgSyncSettings,
    ) -> AnalyzedNode {
        let mut config = HashMap::new();
        if let Some(s) = sync_commit {
            config.insert("synchronous_commit".to_owned(), s.to_owned());
        }
        if let Some(n) = standby_names {
            config.insert("synchronous_standby_names".to_owned(), n.to_owned());
        }
        let h = PrimaryHealthBuilder::new()
            .with_replication(replication_count)
            .with_config(config)
            .with_sync_state(sync_state)
            .build();
        primary_node(h)
    }

    fn streaming_replica(id: u32, name: &str) -> AnalyzedNode {
        make_node(
            id,
            name,
            Role::Replica {
                health: Box::new(make_replica_health()),
            },
        )
    }

    #[test]
    fn check_writes_blocked_fires_when_sync_on_and_no_quorum() {
        let primary = primary_with(Some("on"), Some("ANY 1 (a)"), 2, PgSyncSettings::Async);
        let mut verdict = Verdict::default();

        check_writes_blocked(&primary, &mut verdict);

        assert_eq!(verdict.cluster_verdict, Some(ClusterVerdict::WritesBlocked),);
    }

    #[test]
    fn check_writes_blocked_fires_when_sync_on_and_no_replicas() {
        // Vacuous all-non-quorum (0 == 0) is correct: postgres waits forever.
        let primary = primary_with(Some("on"), Some("ANY 1 (a)"), 0, PgSyncSettings::Quorum);
        let mut verdict = Verdict::default();

        check_writes_blocked(&primary, &mut verdict);

        assert_eq!(verdict.cluster_verdict, Some(ClusterVerdict::WritesBlocked),);
    }

    #[test]
    fn check_writes_blocked_silent_when_quorum_present() {
        let primary = primary_with(Some("on"), Some("ANY 1 (a)"), 2, PgSyncSettings::Quorum);
        let mut verdict = Verdict::default();

        check_writes_blocked(&primary, &mut verdict);

        assert_eq!(verdict.cluster_verdict, None);
    }

    #[test]
    fn check_writes_blocked_silent_when_standby_names_empty() {
        let primary = primary_with(Some("on"), Some(""), 2, PgSyncSettings::Async);
        let mut verdict = Verdict::default();

        check_writes_blocked(&primary, &mut verdict);

        assert_eq!(verdict.cluster_verdict, None);
    }

    #[test]
    fn check_writes_blocked_silent_when_sync_commit_off() {
        let primary = primary_with(Some("off"), Some("ANY 1 (a)"), 2, PgSyncSettings::Async);
        let mut verdict = Verdict::default();

        check_writes_blocked(&primary, &mut verdict);

        assert_eq!(verdict.cluster_verdict, None);
    }

    #[test]
    fn check_writes_unprotected_fires_when_sync_off_and_no_streaming() {
        let primary = primary_with(Some("off"), None, 0, PgSyncSettings::Quorum);
        let mut verdict = Verdict::default();

        check_writes_unprotected(&primary, &[], &mut verdict);

        assert_eq!(
            verdict.cluster_verdict,
            Some(ClusterVerdict::WritesUnprotected),
        );
    }

    #[test]
    fn check_writes_unprotected_fires_when_standby_names_empty_and_no_streaming() {
        let primary = primary_with(Some("on"), Some(""), 0, PgSyncSettings::Quorum);
        let mut verdict = Verdict::default();

        check_writes_unprotected(&primary, &[], &mut verdict);

        assert_eq!(
            verdict.cluster_verdict,
            Some(ClusterVerdict::WritesUnprotected),
        );
    }

    #[test]
    fn check_writes_unprotected_silent_when_streaming_replica_present() {
        let primary = primary_with(Some("off"), None, 1, PgSyncSettings::Quorum);
        let replica = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let mut verdict = Verdict::default();

        check_writes_unprotected(&primary, &[&replica], &mut verdict);

        assert_eq!(verdict.cluster_verdict, None);
    }

    #[test]
    fn check_writes_unprotected_silent_when_sync_replication_active() {
        let primary = primary_with(Some("on"), Some("ANY 1 (a)"), 1, PgSyncSettings::Quorum);
        let replica = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let mut verdict = Verdict::default();

        check_writes_unprotected(&primary, &[&replica], &mut verdict);

        assert_eq!(verdict.cluster_verdict, None);
    }

    #[test]
    fn check_lag_fires_for_replica_above_threshold() {
        // 10s lag * 16MB/s = 160MB > LAG_THRESHOLD_BYTES (80MB).
        let primary = primary_node(make_primary_health(2, Some("00:00:10.000000")));
        let r1 = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let r2 = streaming_replica(3, "dev-pg-app001-db003.sto3.example.com");
        let mut verdict = Verdict::default();

        check_lag(&primary, &[&r1, &r2], &mut verdict);

        assert!(
            verdict
                .node_verdicts
                .iter()
                .any(|(name, v)| name == &r1.node_name && matches!(v, NodeVerdict::HighLag { .. }))
        );
    }

    #[test]
    fn check_lag_silent_below_threshold() {
        // No lag in fixture → sent_lsn == replay_lsn → 0 bytes < threshold.
        let primary = primary_node(make_primary_health(2, None));
        let r1 = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let r2 = streaming_replica(3, "dev-pg-app001-db003.sto3.example.com");
        let mut verdict = Verdict::default();

        check_lag(&primary, &[&r1, &r2], &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_lag_silent_when_replica_ip_does_not_match() {
        // Replica IP doesn't match primary's pg_stat_replication.client_addr.
        let primary = primary_node(make_primary_health(2, Some("00:00:10.000000")));
        let mut r1 = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        r1.ip_address = Ipv4Addr::new(10, 99, 99, 99);
        let mut verdict = Verdict::default();

        check_lag(&primary, &[&r1], &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_quorum_fires_for_non_quorum_replica() {
        let primary = primary_node(
            PrimaryHealthBuilder::new()
                .with_replication(1)
                .with_sync_state(PgSyncSettings::Async)
                .build(),
        );
        let r1 = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let mut verdict = Verdict::default();

        check_quorum(&primary, &[&r1], &mut verdict);

        assert!(has_verdict(&verdict, &NodeVerdict::NotInQuorum));
    }

    #[test]
    fn check_quorum_silent_for_quorum_replica() {
        let primary = primary_node(
            PrimaryHealthBuilder::new()
                .with_replication(1)
                .with_sync_state(PgSyncSettings::Quorum)
                .build(),
        );
        let r1 = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let mut verdict = Verdict::default();

        check_quorum(&primary, &[&r1], &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_chained_replication_fires_when_replica_follows_replica() {
        // db003 streams from db002 (a replica) instead of db001 (the primary).
        let primary = primary_node(make_primary_health(2, None));
        let primary_ip = primary.ip_address.to_string();
        let r1 = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let r1_ip = r1.ip_address.to_string();

        let r2_health = ReplicaHealthBuilder::new().with_sender_host(&r1_ip).build();
        let r2 = make_node(
            3,
            "dev-pg-app001-db003.sto3.example.com",
            Role::Replica {
                health: Box::new(r2_health),
            },
        );
        // sanity: r1 streams from primary, not chained
        let _ = primary_ip;
        let mut verdict = Verdict::default();

        check_chained_replication(&primary, &[&r1, &r2], &mut verdict);

        assert!(verdict.node_verdicts.iter().any(|(name, v)| {
            name == &r2.node_name && matches!(v, NodeVerdict::ChainedReplication { .. })
        }));
    }

    #[test]
    fn check_chained_replication_silent_when_replicas_follow_primary() {
        let primary = primary_node(make_primary_health(2, None));
        let primary_ip = primary.ip_address.to_string();

        let make_repl = |id: u32, name: &str| {
            let h = ReplicaHealthBuilder::new()
                .with_sender_host(&primary_ip)
                .build();
            make_node(
                id,
                name,
                Role::Replica {
                    health: Box::new(h),
                },
            )
        };
        let r1 = make_repl(2, "dev-pg-app001-db002.sto2.example.com");
        let r2 = make_repl(3, "dev-pg-app001-db003.sto3.example.com");
        let mut verdict = Verdict::default();

        check_chained_replication(&primary, &[&r1, &r2], &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_streaming_fires_when_wal_receiver_missing() {
        let h = ReplicaHealthBuilder::new().without_wal_receiver().build();
        let node = make_node(
            2,
            "dev-pg-app001-db002.sto2.example.com",
            Role::Replica {
                health: Box::new(h),
            },
        );
        let mut verdict = Verdict::default();

        check_streaming(&node, &mut verdict);

        assert!(has_verdict(&verdict, &NodeVerdict::NotStreaming));
    }

    #[test]
    fn check_streaming_silent_when_wal_receiver_present() {
        let node = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let mut verdict = Verdict::default();

        check_streaming(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_streaming_silent_for_primary() {
        let node = primary_node(make_primary_health(0, None));
        let mut verdict = Verdict::default();

        check_streaming(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    fn node_with_disk(disk: Option<DiskCheckOutcome>) -> AnalyzedNode {
        let mut node = make_node(
            1,
            "dev-pg-app001-db001.sto1.example.com",
            Role::Primary {
                health: Box::new(make_primary_health(0, None)),
            },
        );
        node.disk_check = disk;
        node
    }

    #[test]
    fn check_disk_errors_fires_disk_io_errors() {
        let node = node_with_disk(Some(checked(2, 0, 1)));
        let mut verdict = Verdict::default();

        check_disk_errors(&node, &mut verdict);

        assert!(has_verdict(
            &verdict,
            &NodeVerdict::DiskIoErrors { io: 0, block: 0 },
        ));
        assert!(!has_verdict(
            &verdict,
            &NodeVerdict::FilesystemErrors { count: 0 },
        ));
    }

    #[test]
    fn check_disk_errors_fires_filesystem_errors() {
        let node = node_with_disk(Some(checked(0, 3, 0)));
        let mut verdict = Verdict::default();

        check_disk_errors(&node, &mut verdict);

        assert!(has_verdict(
            &verdict,
            &NodeVerdict::FilesystemErrors { count: 0 },
        ));
        assert!(!has_verdict(
            &verdict,
            &NodeVerdict::DiskIoErrors { io: 0, block: 0 },
        ));
    }

    #[test]
    fn check_disk_errors_fires_both_when_both_present() {
        let node = node_with_disk(Some(checked(1, 2, 0)));
        let mut verdict = Verdict::default();

        check_disk_errors(&node, &mut verdict);

        assert!(has_verdict(
            &verdict,
            &NodeVerdict::DiskIoErrors { io: 0, block: 0 },
        ));
        assert!(has_verdict(
            &verdict,
            &NodeVerdict::FilesystemErrors { count: 0 },
        ));
    }

    #[test]
    fn check_disk_errors_silent_for_failed_outcome() {
        let node = node_with_disk(Some(DiskCheckOutcome::Failed {
            reason: "ssh failed".to_owned(),
        }));
        let mut verdict = Verdict::default();

        check_disk_errors(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_disk_errors_silent_when_no_disk_check() {
        let node = node_with_disk(None);
        let mut verdict = Verdict::default();

        check_disk_errors(&node, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[rstest]
    #[case::unknown_role(Role::Unknown, true)]
    fn check_unreachable_fires_for_unknown_role(#[case] role: Role, #[case] should_fire: bool) {
        let node = make_node(1, "dev-pg-app001-db001.sto1.example.com", role);
        let primary = primary_node(make_primary_health(0, None));
        let mut verdict = Verdict::default();

        check_unreachable(&node, &primary, &mut verdict);

        assert_eq!(
            has_verdict(&verdict, &NodeVerdict::Unreachable),
            should_fire,
        );
    }

    #[test]
    fn check_unreachable_silent_for_primary() {
        let node = primary_node(make_primary_health(0, None));
        let primary = primary_node(make_primary_health(0, None));
        let mut verdict = Verdict::default();

        check_unreachable(&node, &primary, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_unreachable_silent_for_replica() {
        let node = streaming_replica(2, "dev-pg-app001-db002.sto2.example.com");
        let primary = primary_node(make_primary_health(0, None));
        let mut verdict = Verdict::default();

        check_unreachable(&node, &primary, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_unreachable_silent_when_primary_sees_streaming() {
        // Scanner couldn't connect to db002 (Role::Unknown), but the primary's
        // pg_stat_replication shows db002 streaming — redundancy is intact.
        let unreachable_replica =
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown);
        let primary_health = PrimaryHealthBuilder::new().with_replication(1).build();
        let mut primary = primary_node(primary_health);
        if let Role::Primary { health } = &mut primary.role {
            health.replication[0].client_addr = Some(unreachable_replica.ip_address.to_string());
            health.replication[0].state = ReplicationState::Streaming;
        }
        let mut verdict = Verdict::default();

        check_unreachable(&unreachable_replica, &primary, &mut verdict);

        assert!(verdict.node_verdicts.is_empty());
    }

    #[test]
    fn check_unreachable_fires_when_primary_does_not_see_streaming() {
        let unreachable_replica =
            make_node(2, "dev-pg-app001-db002.sto2.example.com", Role::Unknown);
        let primary = primary_node(make_primary_health(0, None));
        let mut verdict = Verdict::default();

        check_unreachable(&unreachable_replica, &primary, &mut verdict);

        assert!(has_verdict(&verdict, &NodeVerdict::Unreachable));
    }
}
