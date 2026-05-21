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
    LowerTimelineHasQuorum {
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
            resolution: SplitBrainResolution::LowerTimelineHasQuorum {
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

    use super::*;
    use crate::v2::tests_common::{NodeBuilder, PrimaryHealthBuilder, ReplicaHealthBuilder};

    use pretty_assertions::assert_eq;

    fn primary(id: u32, name: &str, ip: Ipv4Addr, timeline: i32) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_ip(ip)
            .with_primary(PrimaryHealthBuilder::new().with_timeline(timeline).build())
            .build()
    }

    fn replica_following(id: u32, name: &str, sender_ip: Ipv4Addr, timeline: i32) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(timeline)
                    .with_sender_host(&sender_ip.to_string())
                    .build(),
            )
            .build()
    }

    const IP_DB1: Ipv4Addr = Ipv4Addr::new(127, 1, 12, 151);
    const IP_DB2: Ipv4Addr = Ipv4Addr::new(127, 2, 12, 151);
    const IP_DB3: Ipv4Addr = Ipv4Addr::new(127, 3, 12, 151);

    #[test]
    fn higher_timeline_wins_when_no_replica_evidence() {
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let primaries = vec![&db1, &db2];
        let replicas: Vec<&AnalyzedNode> = vec![];

        let info = resolve_split_brain(&primaries, &replicas);

        assert_eq!(
            info,
            SplitBrainInfo {
                true_primary: "db002".to_owned(),
                stale_primaries: vec!["db001".to_owned()],
                resolution: SplitBrainResolution::HigherTimeline {
                    true_primary_timeline: 12,
                    stale_timeline: 11,
                },
            }
        );
    }

    #[test]
    fn timeline_and_replica_evidence_agree() {
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = replica_following(3, "db003", IP_DB2, 12);
        let primaries = vec![&db1, &db2];
        let replicas = vec![&db3];

        let info = resolve_split_brain(&primaries, &replicas);

        assert_eq!(
            info,
            SplitBrainInfo {
                true_primary: "db002".to_owned(),
                stale_primaries: vec!["db001".to_owned()],
                resolution: SplitBrainResolution::Both {
                    true_primary_timeline: 12,
                    stale_timeline: 11,
                    replicas_following_true: vec!["db003".to_owned()],
                },
            }
        );
    }

    #[test]
    fn replica_following_lower_timeline_overrides() {
        // db002 has higher timeline (isolated after promotion);
        // replica still streams from db001 — db001 is the true primary.
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = replica_following(3, "db003", IP_DB1, 11);
        let primaries = vec![&db1, &db2];
        let replicas = vec![&db3];

        let info = resolve_split_brain(&primaries, &replicas);

        assert_eq!(
            info,
            SplitBrainInfo {
                true_primary: "db001".to_owned(),
                stale_primaries: vec!["db002".to_owned()],
                resolution: SplitBrainResolution::LowerTimelineHasQuorum {
                    true_primary_timeline: 11,
                    stale_timeline: 12,
                    replicas_following_true: vec!["db003".to_owned()],
                },
            }
        );
    }

    #[test]
    fn equal_timelines_resolved_by_replica_following() {
        let db1 = primary(1, "db001", IP_DB1, 13);
        let db2 = primary(2, "db002", IP_DB2, 13);
        let db3 = replica_following(3, "db003", IP_DB2, 13);
        let primaries = vec![&db1, &db2];
        let replicas = vec![&db3];

        let info = resolve_split_brain(&primaries, &replicas);

        assert_eq!(
            info,
            SplitBrainInfo {
                true_primary: "db002".to_owned(),
                stale_primaries: vec!["db001".to_owned()],
                resolution: SplitBrainResolution::ReplicaFollowing {
                    replicas_following_true: vec!["db003".to_owned()],
                },
            }
        );
    }

    #[test]
    fn equal_timelines_three_primaries_one_has_follower() {
        // Three-way split with equal timelines; kills the `len > 1 -> ==` mutant
        // (which would only match exactly-2 cases).
        let db1 = primary(1, "db001", IP_DB1, 13);
        let db2 = primary(2, "db002", IP_DB2, 13);
        let db3 = primary(3, "db003", IP_DB3, 13);
        let db4 = replica_following(4, "db004", IP_DB2, 13);
        let primaries = vec![&db1, &db2, &db3];
        let replicas = vec![&db4];

        let info = resolve_split_brain(&primaries, &replicas);

        assert_eq!(info.true_primary, "db002");
        assert_eq!(info.stale_primaries.len(), 2);
        assert!(info.stale_primaries.contains(&"db001".to_owned()));
        assert!(info.stale_primaries.contains(&"db003".to_owned()));
        assert_eq!(
            info.resolution,
            SplitBrainResolution::ReplicaFollowing {
                replicas_following_true: vec!["db004".to_owned()],
            }
        );
    }

    #[test]
    fn equal_timelines_no_replica_evidence_is_indeterminate() {
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 11);
        let primaries = vec![&db1, &db2];
        let replicas: Vec<&AnalyzedNode> = vec![];

        let info = resolve_split_brain(&primaries, &replicas);

        assert_eq!(
            info,
            SplitBrainInfo {
                true_primary: "db001".to_owned(),
                stale_primaries: vec!["db002".to_owned()],
                resolution: SplitBrainResolution::Indeterminate,
            }
        );
    }

    #[test]
    fn build_replica_following_map_groups_by_sender_ip() {
        let db1 = primary(1, "db001", IP_DB1, 12);
        let db2 = primary(2, "db002", IP_DB2, 11);
        let r1 = replica_following(3, "r1", IP_DB1, 12);
        let r2 = replica_following(4, "r2", IP_DB1, 12);
        let r3 = replica_following(5, "r3", IP_DB2, 11);
        let primaries = vec![&db1, &db2];
        let replicas = vec![&r1, &r2, &r3];

        let info = extract_timeline_info(&primaries);
        let map = build_replica_following_map(&info, &replicas);

        assert_eq!(map.get("db001").unwrap().len(), 2);
        assert!(map.get("db001").unwrap().contains(&"r1".to_owned()));
        assert!(map.get("db001").unwrap().contains(&"r2".to_owned()));
        assert_eq!(map.get("db002").unwrap(), &vec!["r3".to_owned()]);
    }

    #[test]
    fn extract_timeline_info_partitions_by_highest() {
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = primary(3, "db003", IP_DB3, 12);
        let primaries = vec![&db1, &db2, &db3];

        let info = extract_timeline_info(&primaries);

        assert_eq!(info.highest_timeline, 12);
        assert_eq!(info.primaries_with_highest_timeline.len(), 2);
        assert_eq!(info.primaries_with_lower_timeline.len(), 1);
        assert_eq!(info.primaries_with_lower_timeline[0].0.node_name, "db001");
    }
}
