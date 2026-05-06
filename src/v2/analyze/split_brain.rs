use std::collections::HashMap;

use crate::v2::{
    analyze::get_timeline,
    scan::{AnalyzedNode, Role},
};

/// How split-brain was resolved.
#[derive(Debug, Eq, PartialEq)]
pub enum SplitBrainResolution {
    /// Higher timeline indicates the true primary (most recent promotion).
    HigherTimeline {
        true_primary_timeline: i32,
        stale_timeline: i32,
    },
    /// Replicas are streaming from the true primary.
    ReplicaFollowing {
        replicas_following_true: Vec<String>,
    },
    /// Both timeline and replica evidence agree.
    Both {
        true_primary_timeline: i32,
        stale_timeline: i32,
        replicas_following_true: Vec<String>,
    },
    /// Replica evidence overrides timeline - replicas are following a lower-timeline primary
    /// This indicates the higher-timeline primary was likely isolated after promotion.
    ReplicaOverridesTimeline {
        true_primary_timeline: i32,
        stale_timeline: i32,
        replicas_following_true: Vec<String>,
    },
    /// Cannot determine true primary - timelines equal, no replica evidence.
    Indeterminate,
}

/// Information extracted from timeline analysis of multiple primaries.
///
/// Used during split-brain resolution to categorize primaries by their timeline.
struct TimelineInfo<'a> {
    /// The highest timeline ID found among primaries.
    highest_timeline: i32,
    /// The primary node with the highest timeline (first one if multiple).
    highest_timeline_node: &'a AnalyzedNode,
    /// Primaries that share the highest timeline (could be multiple if equal).
    primaries_with_highest_timeline: Vec<(&'a AnalyzedNode, i32)>,
    /// Primaries with timelines lower than the highest (stale primaries).
    primaries_with_lower_timeline: Vec<(&'a AnalyzedNode, i32)>,
}

/// Information about a split-brain scenario and its resolution.
#[derive(Debug, Eq, PartialEq)]
pub struct SplitBrainInfo {
    /// The node determined to be the true primary based on timeline analysis.
    pub true_primary: String,
    /// The node(s) that are stale primaries (should be demoted).
    pub stale_primaries: Vec<String>,
    /// How the true primary was determined.
    pub resolution: SplitBrainResolution,
}

/// Resolve a split-brain scenario by determining the true primary.
///
/// Resolution strategy:
/// 1. Compare timelines - higher timeline = more recent promotion
/// 2. Check which primary the replicas are streaming from (`received_tli`)
/// 3. If both agree, high confidence. If they disagree, prefer replica evidence.
pub(super) fn resolve_split_brain(
    primaries: &[&AnalyzedNode],
    replicas: &[&AnalyzedNode],
) -> SplitBrainInfo {
    assert!(
        primaries.len() >= 2,
        "resolve_split_brain requires at least 2 primaries"
    );

    let timeline_info = extract_timeline_info(primaries);
    let replicas_following = build_replica_following_map(&timeline_info, replicas);
    determine_true_primary(&timeline_info, &replicas_following)
}

/// Extract and categorize timeline information from primary nodes.
///
/// Sorts primaries by timeline (highest first) and partitions them into
/// those with the highest timeline and those with lower (stale) timelines.
fn extract_timeline_info<'a>(primaries: &[&'a AnalyzedNode]) -> TimelineInfo<'a> {
    // Extract timeline info from primaries
    let mut primary_timelines: Vec<(&AnalyzedNode, i32)> = primaries
        .iter()
        .filter_map(|p| get_timeline(p).map(|tl| (*p, tl)))
        .collect();

    // Sort by timeline descending (highest first)
    primary_timelines.sort_by_key(|b| std::cmp::Reverse(b.1));

    let highest_timeline = primary_timelines[0].1;
    let highest_timeline_node = primary_timelines[0].0;

    // Find primaries with the highest timeline (could be multiple if equal)
    let primaries_with_highest_timeline: Vec<_> = primary_timelines
        .iter()
        .filter(|(_, tl)| *tl == highest_timeline)
        .copied()
        .collect();

    // Find primaries with lower timelines
    let primaries_with_lower_timeline: Vec<_> = primary_timelines
        .iter()
        .filter(|(_, tl)| *tl < highest_timeline)
        .copied()
        .collect();

    TimelineInfo {
        highest_timeline,
        highest_timeline_node,
        primaries_with_highest_timeline,
        primaries_with_lower_timeline,
    }
}

/// Build a map of which replicas are following which primary.
///
/// For each primary, checks if any replica's WAL receiver is connected to that
/// primary's IP address (on port 5432). Returns a map from primary node name
/// to the list of replica node names following it.
fn build_replica_following_map(
    timeline_info: &TimelineInfo<'_>,
    replicas: &[&AnalyzedNode],
) -> HashMap<String, Vec<String>> {
    // First, build a map of sender (host:port) -> replica names
    let mut replicas_by_sender: HashMap<String, Vec<String>> = HashMap::new();

    for replica in replicas {
        if let Role::Replica { health } = &replica.role
            && let Some(wal_receiver) = &health.wal_receiver
        {
            let sender_key = format!("{}:{}", wal_receiver.sender_host, wal_receiver.sender_port);
            replicas_by_sender
                .entry(sender_key)
                .or_default()
                .push(replica.node_name.clone());
        }
    }

    // Match sender IPs to primaries (combine both highest and lower timeline primaries)
    let all_primaries: Vec<_> = timeline_info
        .primaries_with_highest_timeline
        .iter()
        .chain(timeline_info.primaries_with_lower_timeline.iter())
        .collect();

    let mut replicas_following = HashMap::new();

    for (primary, _) in all_primaries {
        let primary_ip = primary.ip_address.to_string();
        // Check for connections on standard PostgreSQL port
        let key_5432 = format!("{}:5432", primary_ip);
        if let Some(followers) = replicas_by_sender.get(&key_5432) {
            replicas_following.insert(primary.node_name.clone(), followers.clone());
        }
    }

    replicas_following
}

/// Determine the true primary based on timeline and replica evidence.
///
/// Resolution strategy:
/// 1. If one primary has a higher timeline, it's likely the true primary
/// 2. Check which primary the replicas are streaming from
/// 3. Replica evidence can override timeline if replicas follow a lower-timeline primary
///    (indicates the higher-timeline primary was isolated after promotion)
fn determine_true_primary(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<String, Vec<String>>,
) -> SplitBrainInfo {
    if timeline_info.primaries_with_highest_timeline.len() == 1
        && !timeline_info.primaries_with_lower_timeline.is_empty()
    {
        resolve_with_different_timelines(timeline_info, replicas_following)
    } else if timeline_info.primaries_with_highest_timeline.len() > 1 {
        resolve_with_equal_timelines(timeline_info, replicas_following)
    } else {
        // Single primary with highest timeline, no stale ones (shouldn't happen with >= 2 primaries)
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .skip(1)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: timeline_info.highest_timeline_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Indeterminate,
        }
    }
}

/// Resolve split-brain when primaries have different timelines.
///
/// One primary has a higher timeline - but we still need to check if replicas disagree.
/// Replica evidence can override timeline analysis.
fn resolve_with_different_timelines(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<String, Vec<String>>,
) -> SplitBrainInfo {
    let stale_tl = timeline_info.primaries_with_lower_timeline[0].1;
    let highest_tl_node = timeline_info.highest_timeline_node;
    let highest_tl = timeline_info.highest_timeline;

    let replicas_following_highest = replicas_following
        .get(&highest_tl_node.node_name)
        .cloned()
        .unwrap_or_default();

    // Check if any replicas are following a lower-timeline primary instead
    let mut replicas_following_stale: Vec<String> = vec![];
    let mut stale_with_followers: Option<&AnalyzedNode> = None;

    for (stale_node, _) in &timeline_info.primaries_with_lower_timeline {
        if let Some(followers) = replicas_following.get(&stale_node.node_name)
            && !followers.is_empty()
        {
            replicas_following_stale.clone_from(followers);
            stale_with_followers = Some(*stale_node);
            break;
        }
    }

    if !replicas_following_stale.is_empty() && replicas_following_highest.is_empty() {
        // Replicas are following the lower-timeline primary - it's the real primary
        // The higher-timeline was likely isolated after a failed promotion
        let stale_node = stale_with_followers.unwrap();

        SplitBrainInfo {
            true_primary: stale_node.node_name.clone(),
            stale_primaries: vec![highest_tl_node.node_name.clone()],
            resolution: SplitBrainResolution::ReplicaOverridesTimeline {
                true_primary_timeline: stale_tl,
                stale_timeline: highest_tl,
                replicas_following_true: replicas_following_stale,
            },
        }
    } else if !replicas_following_highest.is_empty() {
        // Both timeline and replica evidence agree
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_lower_timeline
            .iter()
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: highest_tl_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Both {
                true_primary_timeline: highest_tl,
                stale_timeline: stale_tl,
                replicas_following_true: replicas_following_highest,
            },
        }
    } else {
        // No replica evidence - trust timeline
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_lower_timeline
            .iter()
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: highest_tl_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::HigherTimeline {
                true_primary_timeline: highest_tl,
                stale_timeline: stale_tl,
            },
        }
    }
}

/// Resolve split-brain when primaries have equal timelines.
///
/// When timelines are equal, we need replica evidence to determine the true primary.
fn resolve_with_equal_timelines(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<String, Vec<String>>,
) -> SplitBrainInfo {
    // Find which primary has replicas following it
    let mut primary_with_followers: Option<&AnalyzedNode> = None;
    let mut followers_list: Vec<String> = vec![];

    for (primary, _) in &timeline_info.primaries_with_highest_timeline {
        if let Some(followers) = replicas_following.get(&primary.node_name)
            && !followers.is_empty()
        {
            primary_with_followers = Some(*primary);
            followers_list.clone_from(followers);
            break;
        }
    }

    if let Some(true_primary_node) = primary_with_followers {
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .filter(|(n, _)| n.node_name != true_primary_node.node_name)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: true_primary_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::ReplicaFollowing {
                replicas_following_true: followers_list,
            },
        }
    } else {
        // Cannot determine - mark first as "true" but resolution is indeterminate
        let first = timeline_info.primaries_with_highest_timeline[0].0;
        let stale_primaries: Vec<String> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .skip(1)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: first.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Indeterminate,
        }
    }
}

#[cfg(test)]
mod tests {

    use std::net::Ipv4Addr;

    use chrono::Utc;

    use crate::v2::{
        analyze::{
            AnalyzedCluster, ClusterHealth, Reason, analyze,
            cluster_state_tests::{
                make_cluster, make_node, make_node_with_ip, make_primary_health,
                make_primary_health_with_timeline, make_replica_health,
            },
        },
        scan::health_check_replica::{LagInfo, ReplicaHealthCheckResult, WalReceiverInfo},
    };

    use super::*;

    #[test]
    fn test_critical_when_split_brain_two_primaries_same_timeline() {
        // Both primaries have same timeline (11), replica is following db001 (sender_host: 127.1.12.151)
        let cluster = make_cluster(vec![
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(1, Some("00:00:00.001"))),
                },
                Ipv4Addr::new(127, 1, 12, 151),
            ),
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(1, Some("00:00:00.001"))),
                },
                Ipv4Addr::new(127, 2, 12, 151),
            ),
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()), // sender_host: 127.1.12.151
                },
                Ipv4Addr::new(127, 3, 12, 151),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // Both primaries have same timeline, replica's sender_host matches db001's IP
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db001.sto1.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db002.sto2.example.com".to_owned()],
                resolution: SplitBrainResolution::ReplicaFollowing {
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_owned(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_no_replica_evidence() {
        // Scenario: 2 primaries detected (split brain), no replica to determine true primary
        // Both have same timeline, no replica evidence - resolution is indeterminate
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
                },
            ),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                // db001 is first in iteration order, but resolution is indeterminate
                true_primary: "dev-pg-app001-db001.sto1.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db002.sto2.example.com".to_owned()],
                resolution: SplitBrainResolution::Indeterminate,
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_higher_timeline_wins() {
        // Scenario: db001 has timeline 11, db002 has timeline 12 (more recent promotion)
        // db002 should be identified as true primary
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 11)),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 12)),
                },
            ),
            make_node(3, "dev-pg-app001-db003.sto3.example.com", Role::Unknown),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_owned()],
                resolution: SplitBrainResolution::HigherTimeline {
                    true_primary_timeline: 12,
                    stale_timeline: 11,
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_both_timeline_and_replica_evidence() {
        // Scenario: db002 has higher timeline AND replica is following db002
        // Use different IPs so we can match replica to the correct primary
        let replica_health = ReplicaHealthCheckResult {
            timeline_id: 12,
            wal_receiver: Some(WalReceiverInfo {
                pid: 4_053_449,
                status: "streaming".to_owned(),
                receive_start_lsn: "47F/67000000".to_owned(),
                receive_start_tli: 12,
                written_lsn: "48F/6957B540".to_owned(),
                flushed_lsn: "48F/6957B540".to_owned(),
                received_tli: 12,
                last_msg_send_time: Some(Utc::now()),
                last_msg_receipt_time: Some(Utc::now()),
                latest_end_lsn: "48F/6957B540".to_owned(),
                latest_end_time: Some(Utc::now()),
                slot_name: None,
                sender_host: "127.2.12.151".to_owned(), // db002's IP
                sender_port: 5432,
                conninfo: "user=replicator host=127.2.12.151".to_owned(),
            }),
            lag: LagInfo {
                apply_lag_bytes: Some(0),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 11)),
                },
                Ipv4Addr::new(127, 1, 12, 151),
            ),
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(1, None, 12)),
                },
                Ipv4Addr::new(127, 2, 12, 151),
            ),
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(replica_health),
                },
                Ipv4Addr::new(127, 3, 12, 151),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_owned()],
                resolution: SplitBrainResolution::Both {
                    true_primary_timeline: 12,
                    stale_timeline: 11,
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_owned(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_replica_overrides_higher_timeline() {
        // Scenario: db002 has higher timeline (12) but replica is following db001 (timeline 11)
        // This happens when db002 was promoted but then isolated, while db001 continued serving
        // The replica following db001 is the authoritative evidence of the true primary
        let replica_health = ReplicaHealthCheckResult {
            timeline_id: 11,
            wal_receiver: Some(WalReceiverInfo {
                pid: 4_053_449,
                status: "streaming".to_owned(),
                receive_start_lsn: "47F/67000000".to_owned(),
                receive_start_tli: 11, // Following timeline 11 (db001)
                written_lsn: "48F/6957B540".to_owned(),
                flushed_lsn: "48F/6957B540".to_owned(),
                received_tli: 11,
                last_msg_send_time: Some(Utc::now()),
                last_msg_receipt_time: Some(Utc::now()),
                latest_end_lsn: "48F/6957B540".to_owned(),
                latest_end_time: Some(Utc::now()),
                slot_name: None,
                sender_host: "127.1.12.151".to_owned(), // db001's IP
                sender_port: 5432,
                conninfo: "user=replicator host=127.1.12.151".to_owned(),
            }),
            lag: LagInfo {
                apply_lag_bytes: Some(0),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(1, None, 11)), // Lower timeline but has replica
                },
                Ipv4Addr::new(127, 1, 12, 151),
            ),
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 12)), // Higher timeline but isolated
                },
                Ipv4Addr::new(127, 2, 12, 151),
            ),
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(replica_health),
                },
                Ipv4Addr::new(127, 3, 12, 151),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // db001 is the true primary because the replica is following it,
        // even though db002 has a higher timeline
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db001.sto1.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db002.sto2.example.com".to_owned()],
                resolution: SplitBrainResolution::ReplicaOverridesTimeline {
                    true_primary_timeline: 11,
                    stale_timeline: 12,
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_owned(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_split_brain_after_hard_failover_replica_follows_new_primary() {
        // Scenario: Hard failover occurred, db002 was promoted to primary.
        // db001 came back online as a stale primary (same timeline, no replicas).
        // db003 correctly reconnected to db002 (the new primary).
        // Both primaries have the same timeline, so we rely on replica evidence.
        // db002 should be identified as the true primary because db003 follows it.
        let replica_health = ReplicaHealthCheckResult {
            timeline_id: 13,
            wal_receiver: Some(WalReceiverInfo {
                pid: 2_727_816,
                status: "streaming".to_owned(),
                receive_start_lsn: "281/7D000000".to_owned(),
                receive_start_tli: 13,
                written_lsn: "281/BAAA6510".to_owned(),
                flushed_lsn: "281/BAAA6510".to_owned(),
                received_tli: 13,
                last_msg_send_time: Some(Utc::now()),
                last_msg_receipt_time: Some(Utc::now()),
                latest_end_lsn: "281/BAAA6510".to_owned(),
                latest_end_time: Some(Utc::now()),
                slot_name: None,
                sender_host: "127.2.12.162".to_owned(), // db002's IP - following new primary
                sender_port: 5432,
                conninfo: "user=replicator host=127.2.12.162".to_owned(),
            }),
            lag: LagInfo {
                apply_lag_bytes: Some(0),
                last_transaction_replay_at: Some(Utc::now()),
            },
            conflicts_by_db: HashMap::new(),
            configuration: HashMap::new(),
        };

        let cluster = make_cluster(vec![
            // db001: Stale primary that came back after failover (no replicas connected)
            make_node_with_ip(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(0, None, 13)),
                },
                Ipv4Addr::new(127, 1, 12, 162),
            ),
            // db002: New primary (promoted during failover, has db003 streaming)
            make_node_with_ip(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health_with_timeline(1, None, 13)),
                },
                Ipv4Addr::new(127, 2, 12, 162),
            ),
            // db003: Replica correctly following db002
            make_node_with_ip(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::Replica {
                    health: Box::new(replica_health),
                },
                Ipv4Addr::new(127, 3, 12, 162),
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // db002 is the true primary because the replica is following it
        let expected = ClusterHealth::Critical {
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::SplitBrain(SplitBrainInfo {
                true_primary: "dev-pg-app001-db002.sto2.example.com".to_owned(),
                stale_primaries: vec!["dev-pg-app001-db001.sto1.example.com".to_owned()],
                resolution: SplitBrainResolution::ReplicaFollowing {
                    replicas_following_true: vec![
                        "dev-pg-app001-db003.sto3.example.com".to_owned(),
                    ],
                },
            }),
        };

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_critical_primary_with_two_unreachable_replicas() {
        // Primary reachable + one replica reachable (but replica count is 0)
        let cluster = make_cluster(vec![
            make_node(
                1,
                "dev-pg-app001-db001.sto1.example.com",
                Role::Primary {
                    health: Box::new(make_primary_health(0, None)),
                },
            ),
            make_node(
                2,
                "dev-pg-app001-db002.sto2.example.com",
                Role::Replica {
                    health: Box::new(make_replica_health()),
                },
            ),
            make_node(
                3,
                "dev-pg-app001-db003.sto3.example.com",
                Role::UnknownReplica,
            ),
        ]);

        let actual = analyze(cluster.clone(), HashMap::new());
        // 1 primary + 1 replica = Degraded with OneReplicaDown
        let expected = ClusterHealth::Degraded {
            lag: 0,
            cluster: AnalyzedCluster {
                cluster,
                backup_progress: HashMap::new(),
            },
            reason: Reason::OneReplicaDown,
        };

        assert_eq!(actual, expected);
    }
}
