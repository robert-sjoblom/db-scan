use std::collections::HashMap;

use serde::Serialize;

use crate::v2::{
    analyze::{
        NodeName, get_timeline,
        sync_standby_names::{Quorum, parse},
    },
    scan::{AnalyzedNode, Role, health_check_primary::ReplicationState},
};

const WEAKENED_SYNCHRONOUS_COMMIT: [&str; 4] = ["local", "off", "remote_write", ""];

/// How split-brain was resolved.
#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub enum SplitBrainResolution {
    /// Higher timeline indicates the true primary (most recent promotion).
    HigherTimeline {
        true_primary_timeline: i32,
        stale_timeline: i32,
    },
    /// Replicas are streaming from the true primary.
    ReplicaFollowing {
        replicas_following_true: Vec<NodeName>,
    },
    /// Both timeline and replica evidence agree.
    Both {
        true_primary_timeline: i32,
        stale_timeline: i32,
        replicas_following_true: Vec<NodeName>,
    },
    /// Replica evidence overrides timeline - replicas are following a lower-timeline primary
    /// This indicates the higher-timeline primary was likely isolated after promotion.
    LowerTimelineHasQuorum {
        true_primary_timeline: i32,
        stale_timeline: i32,
        replicas_following_true: Vec<NodeName>,
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
#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct SplitBrainInfo {
    /// The node determined to be the true primary based on timeline analysis.
    pub true_primary: NodeName,
    /// The node(s) that are stale primaries (should be demoted).
    pub stale_primaries: Vec<NodeName>,
    /// How the true primary was determined.
    pub resolution: SplitBrainResolution,
    pub confidence: Confidence,
    pub findings: Vec<SplitBrainFinding>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Copy, Clone, Serialize)]
#[cfg_attr(test, derive(strum::EnumIter))]
pub enum Confidence {
    Refuse,
    Conflicting,
    BestEffort,
}

/// Structured finding attached to a split-brain resolution.
/// Categories defined in ADR-002 §4.
#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub enum SplitBrainFinding {
    SystemIdentifierMismatch {
        nodes: Vec<NodeName>,
    },
    SynchronousCommitWeakened {
        primary: NodeName,
        value: String,
    },
    SyncStandbyNamesDiverged {
        primaries: Vec<NodeName>,
    },
    ReplicaWalReceiverStale {
        replica: NodeName,
        claimed_sender: NodeName,
    },
    PrimaryDoesNotSeeReplica(ReplicationLink),
    BidirectionalFlushingConfirmed(ReplicationLink),
    ReplicaInCatchup(ReplicationLink),
    PrimaryQuorumUnsatisfied {
        primary: NodeName,
        required: u32,
        observed: u32,
    },
    DivergentReplicaWal {
        replica_node: NodeName,
        replica_received_tli: i32,
        replica_flushed_lsn: String,
        fork_tli: i32,
        fork_lsn: String,
    },
}

#[derive(Debug, Serialize, Clone, Eq, PartialEq)]
pub struct ReplicationLink {
    pub primary: NodeName,
    pub replica: NodeName,
}

impl ReplicationLink {
    pub fn new<T: Into<NodeName>>(primary: T, replica: T) -> Self {
        Self {
            primary: primary.into(),
            replica: replica.into(),
        }
    }
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

    let reference_sysid = reference_sysid(primaries);
    let mismatched_nodes = mismatched_sysid_nodes(primaries, replicas, reference_sysid.as_deref());

    let filtered_replicas = replicas
        .iter()
        .filter(|r| !mismatched_nodes.contains(&r.node_name))
        .copied()
        .collect::<Vec<_>>();

    let mut findings = if mismatched_nodes.is_empty() {
        Vec::new()
    } else {
        vec![SplitBrainFinding::SystemIdentifierMismatch {
            nodes: mismatched_nodes,
        }]
    };

    for p in primaries {
        let Some(h) = p.role.as_primary() else {
            continue;
        };
        let v = h
            .configuration
            .get("synchronous_commit")
            .map_or("", String::as_str);

        if WEAKENED_SYNCHRONOUS_COMMIT.contains(&v) {
            findings.push(SplitBrainFinding::SynchronousCommitWeakened {
                primary: p.node_name.clone(),
                value: v.to_owned(),
            });
        }
    }

    let (replicas_following, following_findings) =
        build_replica_following_map(&timeline_info, filtered_replicas.as_slice());
    findings.extend(following_findings);
    findings.extend(emit_quorum_findings(primaries, &replicas_following));

    determine_true_primary(&timeline_info, &replicas_following, &findings)
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

/// Build a map of which replicas are bidirectionally streaming for each primary.
///
/// A replica counts as following a primary only when both sides of the streaming
/// connection look fresh.
///
///   - Replica side (authoritative): `wal_receiver` names this primary, status is
///     `streaming`/`catchup` and `last_msg_receipt_time` is within the freshness
///     threshold.
///   - Primary side (corroborating): `pg_stat_replication` has a row with a non-empty
///     `application_name` matching the replica, stat is `streaming`/`catchup`, and
///     `reply_time` is fresh
///
/// Outcomes:
///   - replica-side fails -> not following (silent).
///   - replica-side passes but primary-side fails -> not following + `PrimaryDoesNotSeeReplica`
///   - Both pass -> following + `BidirectionalFlushingConfirmed` (plus `ReplicaInCatchup`
///     if the primary-side state is `catchup`
///
/// Freshness uses each node's own `current_time` against that same node's recorded
/// timestamps, so the comparison is intra-node and immune to scanner<->db clock skew
/// that would otherwise eat into a tight threshold.
fn build_replica_following_map(
    timeline_info: &TimelineInfo<'_>,
    replicas: &[&AnalyzedNode],
) -> (HashMap<NodeName, Vec<NodeName>>, Vec<SplitBrainFinding>) {
    let mut findings = Vec::new();
    let mut following: HashMap<String, Vec<String>> = HashMap::new();

    // Iterate both highest- and lower-timeline primaries. A lower-TL primary with
    // a flushing replica is the `LowerTimelineHasQuorum` signal this module exist
    // to detect.
    let all_primaries = timeline_info
        .primaries_with_highest_timeline
        .iter()
        .chain(timeline_info.primaries_with_lower_timeline.iter())
        .map(|(p, _)| *p);

    for primary in all_primaries {
        let Some(p_health) = primary.role.as_primary() else {
            continue;
        };
        // `wal_sender_timeout` can differ between primaries
        let threshold_ms = (parse_wal_sender_timeout(&p_health.configuration) / 2) + 30_000;

        for replica in replicas {
            let Role::Replica { health: r_health } = &replica.role else {
                continue;
            };
            let Some(wr) = &r_health.wal_receiver else {
                continue;
            };

            // Replica-side gate (authoritative). The `last_msg_receipt_time`
            // freshness check is what separates a live stream from a stale row:
            // a dead `wal_receiver` keeps its values until `wal_sender_timeout`
            // fires, so without this check `status="streaming"` alone would pass
            // on a connection that just died.
            let replica_passes = wr.sender_host == primary.ip_address.to_string()
                && wr.sender_port == 5432
                && matches!(wr.status.as_str(), "streaming" | "catchup")
                && wr.last_msg_receipt_time.is_some_and(|t| {
                    (r_health.current_time - t).num_milliseconds() <= threshold_ms
                });

            if !replica_passes {
                // Only flag stale when the replica's `sender_host` named this
                // primary. Otherwise the replica isn't pointing here, and every
                // primary in the outer loop would emit a finding for every replica
                // that follows some *other* primary.
                if wr.sender_host == primary.ip_address.to_string() {
                    findings.push(SplitBrainFinding::ReplicaWalReceiverStale {
                        replica: replica.node_name.clone(),
                        claimed_sender: primary.node_name.clone(),
                    });
                }
                continue;
            }

            // primary-side gate (corroborating). Rejects:
            // - empty `application_name`; it's a pg default and matches indiscriminately
            // - `state=backup`: pg_basebackup clients, not replication consumers.
            let primary_row = p_health.replication.iter().find(|conn| {
                !conn.application_name.is_empty()
                    && conn.application_name == replica.node_name
                    && matches!(
                        conn.state,
                        ReplicationState::Streaming | ReplicationState::Catchup
                    )
                    && conn.reply_time.is_some_and(|t| {
                        (p_health.current_time - t).num_milliseconds() <= threshold_ms
                    })
            });

            match primary_row {
                Some(row) => {
                    following
                        .entry(primary.node_name.clone())
                        .or_default()
                        .push(replica.node_name.clone());

                    findings.push(SplitBrainFinding::BidirectionalFlushingConfirmed(
                        ReplicationLink::new(&primary.node_name, &replica.node_name),
                    ));

                    if matches!(row.state, ReplicationState::Catchup) {
                        findings.push(SplitBrainFinding::ReplicaInCatchup(ReplicationLink::new(
                            &primary.node_name,
                            &replica.node_name,
                        )));
                    }
                }
                None => findings.push(SplitBrainFinding::PrimaryDoesNotSeeReplica(
                    ReplicationLink::new(&primary.node_name, &replica.node_name),
                )),
            }
        }
    }

    (following, findings)
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
    replicas_following: &HashMap<NodeName, Vec<NodeName>>,
    findings: &[SplitBrainFinding],
) -> SplitBrainInfo {
    let mut split_brain_info = if timeline_info.primaries_with_highest_timeline.len() == 1
        && !timeline_info.primaries_with_lower_timeline.is_empty()
    {
        resolve_with_different_timelines(timeline_info, replicas_following)
    } else if timeline_info.primaries_with_highest_timeline.len() > 1 {
        resolve_with_equal_timelines(timeline_info, replicas_following)
    } else {
        // Single primary with highest timeline, no stale ones (shouldn't happen with >= 2 primaries)
        let stale_primaries: Vec<NodeName> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .skip(1)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: timeline_info.highest_timeline_node.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Indeterminate,
            confidence: Confidence::BestEffort,
            findings: vec![],
        }
    };

    split_brain_info.findings.extend_from_slice(findings);

    let confidence = split_brain_info
        .findings
        .iter()
        .map(determine_confidence_level)
        .min()
        .unwrap_or(Confidence::BestEffort);

    split_brain_info.confidence = confidence;
    split_brain_info
}

fn determine_confidence_level(finding: &SplitBrainFinding) -> Confidence {
    match finding {
        SplitBrainFinding::SystemIdentifierMismatch { .. }
        | SplitBrainFinding::SynchronousCommitWeakened { .. }
        | SplitBrainFinding::PrimaryQuorumUnsatisfied { .. }
        | SplitBrainFinding::DivergentReplicaWal { .. } => Confidence::Refuse,
        SplitBrainFinding::ReplicaWalReceiverStale { .. }
        | SplitBrainFinding::PrimaryDoesNotSeeReplica(_)
        | SplitBrainFinding::ReplicaInCatchup(_) => Confidence::Conflicting,
        SplitBrainFinding::BidirectionalFlushingConfirmed(_)
        | SplitBrainFinding::SyncStandbyNamesDiverged { .. } => Confidence::BestEffort,
    }
}

/// Resolve split-brain when primaries have different timelines.
///
/// One primary has a higher timeline - but we still need to check if replicas disagree.
/// Replica evidence can override timeline analysis.
fn resolve_with_different_timelines(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<NodeName, Vec<NodeName>>,
) -> SplitBrainInfo {
    let stale_tl = timeline_info.primaries_with_lower_timeline[0].1;
    let highest_tl_node = timeline_info.highest_timeline_node;
    let highest_tl = timeline_info.highest_timeline;

    let replicas_following_highest = replicas_following
        .get(&highest_tl_node.node_name)
        .cloned()
        .unwrap_or_default();

    // Check if any replicas are following a lower-timeline primary instead
    let mut replicas_following_stale: Vec<NodeName> = vec![];
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
            confidence: Confidence::BestEffort,
            findings: vec![],
        }
    } else if !replicas_following_highest.is_empty() {
        // Both timeline and replica evidence agree
        let stale_primaries: Vec<NodeName> = timeline_info
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
            confidence: Confidence::BestEffort,
            findings: vec![],
        }
    } else {
        // No replica evidence - trust timeline
        let stale_primaries: Vec<NodeName> = timeline_info
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
            confidence: Confidence::BestEffort,
            findings: vec![],
        }
    }
}

/// Resolve split-brain when primaries have equal timelines.
///
/// When timelines are equal, we need replica evidence to determine the true primary.
fn resolve_with_equal_timelines(
    timeline_info: &TimelineInfo<'_>,
    replicas_following: &HashMap<NodeName, Vec<NodeName>>,
) -> SplitBrainInfo {
    // Find which primary has replicas following it
    let mut primary_with_followers: Option<&AnalyzedNode> = None;
    let mut followers_list: Vec<NodeName> = vec![];

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
        let stale_primaries: Vec<NodeName> = timeline_info
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
            confidence: Confidence::BestEffort,
            findings: vec![],
        }
    } else {
        // Cannot determine - mark first as "true" but resolution is indeterminate
        let first = timeline_info.primaries_with_highest_timeline[0].0;
        let stale_primaries: Vec<NodeName> = timeline_info
            .primaries_with_highest_timeline
            .iter()
            .skip(1)
            .map(|(n, _)| n.node_name.clone())
            .collect();

        SplitBrainInfo {
            true_primary: first.node_name.clone(),
            stale_primaries,
            resolution: SplitBrainResolution::Indeterminate,
            confidence: Confidence::BestEffort,
            findings: vec![],
        }
    }
}

/// Determine the reference `system_identifier` from primaries. Returns `Some(sid)`
/// when a majority class exists (in a 3-node HA cluster); `None` when primaries
/// disagree (no majority).
fn reference_sysid(primaries: &[&AnalyzedNode]) -> Option<String> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for p in primaries {
        if let Some(sid) = p.role.as_primary().map(|h| h.system_identifier.as_str()) {
            *counts.entry(sid).or_insert(0) += 1;
        }
    }

    counts
        .into_iter()
        .filter(|(_, count)| *count >= 2)
        .max_by_key(|(_, count)| *count)
        .map(|(sid, _)| sid.to_owned())
}

/// Returns the set of node names whose sysid differs from `reference`,
/// or every node when `reference` is None.
fn mismatched_sysid_nodes(
    primaries: &[&AnalyzedNode],
    replicas: &[&AnalyzedNode],
    reference: Option<&str>,
) -> Vec<NodeName> {
    let matches = |sid: &str| reference == Some(sid);

    primaries
        .iter()
        .chain(replicas.iter())
        .filter_map(|n| {
            let sid = match &n.role {
                Role::Primary { health } => Some(health.system_identifier.as_str()),
                Role::Replica { health } => Some(health.system_identifier.as_str()),
                Role::Unknown | Role::UnknownPrimary | Role::UnknownReplica => None,
            }?;
            if matches(sid) {
                None
            } else {
                Some(n.node_name.clone())
            }
        })
        .collect()
}

/// Parses `wal_sender_timeout` from `pg_settings.setting`.
fn parse_wal_sender_timeout(cfg: &HashMap<String, String>) -> i64 {
    cfg.get("wal_sender_timeout")
        .and_then(|s| s.parse().ok())
        .unwrap_or(60_000) // pg default
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "we don't have MAXINT followers"
)]
/// A gated replica is a replica that passed through the bidirectional-streaming gate (`build_replica_following_map`).
///
/// Concretely, a replica is "gated" for a primary when *both sides* of the streaming connection looked fresh
/// (see `split_brain.rs:222`).
///
/// Replicas that fail either side don't appear in the `replicas_following` map -- they exist physically, but are not
/// counted as followers.
///
/// This matters for quorum because `synchronous_standby_names` lists _names_ of potential sync standbys. Postgres only
/// acks a write when enough of them respond. A replica that is listed but isn't flushing cannot ack writes, so doesn't
/// contribute to quorum _right now_.
///
/// So the count in this function is:
/// observed = | members(SSN) ∩ gated(primary) | .
///
/// That is, how many SSN-listed standbys are _also_ currently live followers of this primary. If it's below `required`,
/// the primary can't be acking sync writes.
fn emit_quorum_findings(
    primaries: &[&AnalyzedNode],
    replicas_following: &HashMap<NodeName, Vec<NodeName>>,
) -> Vec<SplitBrainFinding> {
    let mut findings = Vec::new();

    for p in primaries {
        let Some(h) = p.role.as_primary() else {
            continue;
        };
        let synchronous_standby_names = h
            .configuration
            .get("synchronous_standby_names")
            .map_or("", String::as_str);
        let Some(Quorum { count, members, .. }) = parse(synchronous_standby_names) else {
            continue;
        };

        // A replica is "gated" for a primary when both sides of the
        // streaming connection looked fresh
        let gated = replicas_following
            .get(&p.node_name)
            .cloned()
            .unwrap_or_default();
        let observed = members
            .iter()
            .filter(|m| gated.iter().any(|g| g == *m))
            .count() as u32;

        if observed < count {
            findings.push(SplitBrainFinding::PrimaryQuorumUnsatisfied {
                primary: p.node_name.clone(),
                required: count,
                observed,
            });
        }
    }

    findings
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;

    use super::*;
    use crate::v2::{
        scan::health_check_primary::PrimaryHealthCheckResult,
        tests_common::{NodeBuilder, PrimaryHealthBuilder, ReplicaHealthBuilder},
    };

    use chrono::{DateTime, Utc};
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    fn primary(id: u32, name: &str, ip: Ipv4Addr, timeline: i32) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_ip(ip)
            .with_primary(PrimaryHealthBuilder::new().with_timeline(timeline).build())
            .build()
    }

    fn primary_with_health(
        id: u32,
        name: &str,
        ip: Ipv4Addr,
        health: PrimaryHealthCheckResult,
    ) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_ip(ip)
            .with_primary(health)
            .build()
    }

    fn primary_with_followers(
        id: u32,
        name: &str,
        ip: Ipv4Addr,
        timeline: i32,
        followers: &[&str],
    ) -> AnalyzedNode {
        NodeBuilder::new(name)
            .with_id(id)
            .with_ip(ip)
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_timeline(timeline)
                    .with_followers(followers)
                    .build(),
            )
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

    /// Confidence is picked via `min()` in `determine_confidence_level`, so `PartialOrd` defines
    /// the severity hierarchy. When adding a new Confidence, add a pair here so
    /// a future re-ordering can't silently demote it.
    fn severity_rank(c: Confidence) -> u8 {
        match c {
            Confidence::Refuse => 0,
            Confidence::Conflicting => 5,
            Confidence::BestEffort => 10,
        }
    }

    #[rstest]
    #[case::system_identifier_mismatch(
        SplitBrainFinding::SystemIdentifierMismatch { nodes: vec!["db003".to_owned()] },
        Confidence::Refuse
    )]
    #[case::synchronous_commit_weakened(
        SplitBrainFinding::SynchronousCommitWeakened { primary: "db001".to_owned(), value: "remote_write".to_owned() },
        Confidence::Refuse
    )]
    #[case::primary_quorum_unsatisfied(
        SplitBrainFinding::PrimaryQuorumUnsatisfied { primary: "db001".to_owned(), required: 1, observed: 0 },
        Confidence::Refuse
    )]
    #[case::divergent_replica_wal(
        SplitBrainFinding::DivergentReplicaWal {
            replica_node: "db003".to_owned(),
            replica_received_tli: 2,
            replica_flushed_lsn: "0/3000100".to_owned(),
            fork_tli: 2,
            fork_lsn: "0/3000000".to_owned(),
        },
        Confidence::Refuse
    )]
    #[case::replica_wal_receiver_stale(
        SplitBrainFinding::ReplicaWalReceiverStale { replica: "db003".to_owned(), claimed_sender: "db001".to_owned() },
        Confidence::Conflicting
    )]
    #[case::primary_does_not_see_replica(
        SplitBrainFinding::PrimaryDoesNotSeeReplica(ReplicationLink::new("db001", "db003")),
        Confidence::Conflicting
    )]
    #[case::replica_in_catchup(
        SplitBrainFinding::ReplicaInCatchup(ReplicationLink::new("db001", "db003")),
        Confidence::Conflicting
    )]
    #[case::bidirectional_flushing_confirmed(
        SplitBrainFinding::BidirectionalFlushingConfirmed(ReplicationLink::new("db001", "db003")),
        Confidence::BestEffort
    )]
    #[case::sync_standby_names_diverged(
        SplitBrainFinding::SyncStandbyNamesDiverged { primaries: vec!["db001".to_owned(), "db002".to_owned()] },
        Confidence::BestEffort
    )]
    fn finding_to_confidence(#[case] input: SplitBrainFinding, #[case] expected: Confidence) {
        let actual = determine_confidence_level(&input);
        assert_eq!(actual, expected);
    }

    #[test]
    fn confidence_ordering_matches_severity_rank() {
        use strum::IntoEnumIterator as _;

        for a in Confidence::iter() {
            for b in Confidence::iter() {
                let ra = severity_rank(a);
                let rb = severity_rank(b);
                assert_eq!(
                    a.cmp(&b),
                    ra.cmp(&rb),
                    "PartialOrd disagrees with severity_rank: \
                     {a:?} (rank {ra}) vs {b:?} (rank {rb})",
                );
            }
        }
    }

    #[test]
    fn reference_sysid_none_when_primaries_disagree() {
        let db1 = NodeBuilder::new("db001")
            .with_primary(PrimaryHealthBuilder::new().build())
            .build();
        let db2 = NodeBuilder::new("db002")
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_system_identifier("1234")
                    .build(),
            )
            .build();

        assert!(reference_sysid(&[&db1, &db2]).is_none());
    }

    #[test]
    fn reference_sysid_some_when_primaries_agree() {
        let db1 = NodeBuilder::new("db001")
            .with_primary(PrimaryHealthBuilder::new().build())
            .build();
        let db2 = NodeBuilder::new("db002")
            .with_primary(PrimaryHealthBuilder::new().build())
            .build();

        assert!(reference_sysid(&[&db1, &db2]).is_some());
    }

    #[test]
    fn mismatched_sysid_nodes_flags_only_divergent() {
        let db1 = NodeBuilder::new("db001")
            .with_primary(PrimaryHealthBuilder::new().build())
            .build();
        let db2 = NodeBuilder::new("db002")
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_system_identifier("foreign")
                    .build(),
            )
            .build();
        let db3 = NodeBuilder::new("db003")
            .with_replica(ReplicaHealthBuilder::new().build())
            .build();

        let mismatched =
            mismatched_sysid_nodes(&[&db1, &db2], &[&db3], Some("6968745321024393216"));

        assert_eq!(mismatched, vec!["db002".to_owned()]);
    }

    #[test]
    fn mismatched_sysid_nodes_none_reference_flags_all() {
        let db1 = NodeBuilder::new("db001")
            .with_primary(PrimaryHealthBuilder::new().build())
            .build();
        let db2 = NodeBuilder::new("db002")
            .with_replica(ReplicaHealthBuilder::new().build())
            .build();

        let mismatched = mismatched_sysid_nodes(&[&db1], &[&db2], None);

        assert_eq!(mismatched, vec!["db001".to_owned(), "db002".to_owned()]);
    }

    #[test]
    fn mismatched_sysid_nodes_flags_replica_with_foreign_sysid() {
        let db1 = NodeBuilder::new("db001")
            .with_primary(PrimaryHealthBuilder::new().build())
            .build();
        let db2 = NodeBuilder::new("db002")
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_system_identifier("foreign")
                    .build(),
            )
            .build();

        let mismatched = mismatched_sysid_nodes(&[&db1], &[&db2], Some("6968745321024393216"));

        assert_eq!(mismatched, vec!["db002".to_owned()]);
    }

    #[test]
    fn sysid_mismatch_sets_refuse_and_emits_finding() {
        // db001 and db002 share the default sysid; db003 has a foreign sysid.
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .with_sender_host(&IP_DB1.to_string())
                    .with_system_identifier("foreign")
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.confidence, Confidence::Refuse);
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::SystemIdentifierMismatch { nodes }
                if nodes == &vec!["db003".to_owned()]
        )));
        assert_eq!(info.true_primary, "db002");
        assert!(matches!(
            info.resolution,
            SplitBrainResolution::HigherTimeline { .. }
        ));
    }

    #[test]
    fn primaries_disagree_on_sysid_excludes_all_replicas() {
        // Two primaries hold different sysids → no reference exists → all nodes
        // flagged; replica is filtered out and resolution falls back to timeline-only.
        let db1 = NodeBuilder::new("db001")
            .with_id(1)
            .with_ip(IP_DB1)
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_timeline(11)
                    .with_system_identifier("X")
                    .build(),
            )
            .build();
        let db2 = NodeBuilder::new("db002")
            .with_id(2)
            .with_ip(IP_DB2)
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_timeline(12)
                    .with_system_identifier("Y")
                    .build(),
            )
            .build();
        let db3 = replica_following(3, "db003", IP_DB1, 11);

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.confidence, Confidence::Refuse);
        assert_eq!(info.true_primary, "db002");
        assert!(matches!(
            info.resolution,
            SplitBrainResolution::HigherTimeline { .. }
        ));
    }

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
                confidence: Confidence::BestEffort,
                findings: vec![],
            }
        );
    }

    #[test]
    fn timeline_and_replica_evidence_agree() {
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary_with_followers(2, "db002", IP_DB2, 12, &["db003"]);
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
                confidence: Confidence::BestEffort,
                findings: vec![SplitBrainFinding::BidirectionalFlushingConfirmed(
                    ReplicationLink::new("db002", "db003"),
                )],
            }
        );
    }

    #[test]
    fn replica_following_lower_timeline_overrides() {
        // db002 has higher timeline (isolated after promotion);
        // replica still streams from db001 — db001 is the true primary.
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &["db003"]);
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
                confidence: Confidence::BestEffort,
                findings: vec![SplitBrainFinding::BidirectionalFlushingConfirmed(
                    ReplicationLink::new("db001", "db003"),
                )],
            }
        );
    }

    #[test]
    fn equal_timelines_resolved_by_replica_following() {
        let db1 = primary(1, "db001", IP_DB1, 13);
        let db2 = primary_with_followers(2, "db002", IP_DB2, 13, &["db003"]);
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
                confidence: Confidence::BestEffort,
                findings: vec![SplitBrainFinding::BidirectionalFlushingConfirmed(
                    ReplicationLink::new("db002", "db003"),
                )],
            }
        );
    }

    #[test]
    fn equal_timelines_three_primaries_one_has_follower() {
        // Three-way split with equal timelines; kills the `len > 1 -> ==` mutant
        // (which would only match exactly-2 cases).
        let db1 = primary(1, "db001", IP_DB1, 13);
        let db2 = primary_with_followers(2, "db002", IP_DB2, 13, &["db004"]);
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
                confidence: Confidence::BestEffort,
                findings: vec![],
            },
        );
    }

    #[test]
    fn build_replica_following_map_groups_by_sender_ip() {
        let db1 = primary_with_followers(1, "db001", IP_DB1, 12, &["r1", "r2"]);
        let db2 = primary_with_followers(2, "db002", IP_DB2, 11, &["r3"]);
        let r1 = replica_following(3, "r1", IP_DB1, 12);
        let r2 = replica_following(4, "r2", IP_DB1, 12);
        let r3 = replica_following(5, "r3", IP_DB2, 11);
        let primaries = vec![&db1, &db2];
        let replicas = vec![&r1, &r2, &r3];

        let info = extract_timeline_info(&primaries);
        let (map, _findings) = build_replica_following_map(&info, &replicas);

        assert_eq!(map.get("db001").unwrap().len(), 2);
        assert!(map.get("db001").unwrap().contains(&"r1".to_owned()));
        assert!(map.get("db001").unwrap().contains(&"r2".to_owned()));
        assert_eq!(map.get("db002").unwrap(), &vec!["r3".to_owned()]);
    }

    #[test]
    fn gate_rejects_stale_last_msg_receipt() {
        // Replica's wal_receiver row is stale (last_msg_receipt_time aged out
        // past freshness threshold). Gate must reject it as a follower of db001.
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &["db003"]);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let stale = DateTime::<Utc>::UNIX_EPOCH - chrono::Duration::seconds(600);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .with_sender_host(&IP_DB1.to_string())
                    .with_last_msg_receipt_time(Some(stale))
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(matches!(
            info.resolution,
            SplitBrainResolution::HigherTimeline { .. }
        ));
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::ReplicaWalReceiverStale { replica, claimed_sender }
                if replica == "db003" && claimed_sender == "db001"
        )));
    }

    #[test]
    fn gate_rejects_status_not_streaming() {
        // wal_receiver.status="stopped" — not actively streaming.
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &["db003"]);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .with_sender_host(&IP_DB1.to_string())
                    .with_wal_receiver_status("stopped")
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::ReplicaWalReceiverStale { replica, claimed_sender }
                if replica == "db003" && claimed_sender == "db001"
        )));
    }

    #[test]
    fn gate_rejects_wrong_sender_port() {
        // wal_receiver.sender_port != 5432 — defends against weird configs.
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &["db003"]);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .with_sender_host(&IP_DB1.to_string())
                    .with_sender_port(5433)
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::ReplicaWalReceiverStale { replica, claimed_sender }
                if replica == "db003" && claimed_sender == "db001"
        )));
    }

    #[test]
    fn gate_silent_when_wal_receiver_missing() {
        // Replica has no wal_receiver at all — gate skips silently. No
        // ReplicaWalReceiverStale should be emitted (no sender_host to compare).
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &["db003"]);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .without_wal_receiver()
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(
            !info
                .findings
                .iter()
                .any(|f| matches!(f, SplitBrainFinding::ReplicaWalReceiverStale { .. }))
        );
    }

    #[test]
    fn gate_rejects_none_last_msg_receipt_time() {
        // last_msg_receipt_time is None — can't prove freshness, must reject.
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &["db003"]);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .with_sender_host(&IP_DB1.to_string())
                    .with_last_msg_receipt_time(None)
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::ReplicaWalReceiverStale { replica, claimed_sender }
                if replica == "db003" && claimed_sender == "db001"
        )));
    }

    #[test]
    fn gate_accepts_catchup_status_and_emits_replica_in_catchup() {
        // status="catchup" on both sides — legitimately following mid-recovery.
        // Gate passes; ReplicaInCatchup surfaces alongside BidirectionalFlushingConfirmed.
        let db1 = NodeBuilder::new("db001")
            .with_id(1)
            .with_ip(IP_DB1)
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_timeline(11)
                    .with_followers(&["db003"])
                    .with_follower_state(ReplicationState::Catchup)
                    .build(),
            )
            .build();
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = NodeBuilder::new("db003")
            .with_id(3)
            .with_replica(
                ReplicaHealthBuilder::new()
                    .with_timeline(11)
                    .with_sender_host(&IP_DB1.to_string())
                    .with_wal_receiver_status("catchup")
                    .build(),
            )
            .build();

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db001");
        assert!(matches!(
            info.resolution,
            SplitBrainResolution::LowerTimelineHasQuorum { .. }
        ));
        assert!(
            info.findings
                .iter()
                .any(|f| matches!(f, SplitBrainFinding::BidirectionalFlushingConfirmed(_)))
        );
        assert!(
            info.findings
                .iter()
                .any(|f| matches!(f, SplitBrainFinding::ReplicaInCatchup(_)))
        );
    }

    #[test]
    fn gate_rejects_empty_application_name() {
        // Primary's pg_stat_replication has a row with empty application_name.
        // Replica-side gate passes, but no row matches "db003" → PrimaryDoesNotSeeReplica.
        let db1 = primary_with_followers(1, "db001", IP_DB1, 11, &[""]);
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = replica_following(3, "db003", IP_DB1, 11);

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::PrimaryDoesNotSeeReplica(link)
                if link.primary == "db001" && link.replica == "db003"
        )));
    }

    #[test]
    fn gate_rejects_state_backup() {
        // pg_stat_replication row has state=Backup (pg_basebackup client),
        // not a flushing replica. Primary-side gate rejects.
        let db1 = NodeBuilder::new("db001")
            .with_id(1)
            .with_ip(IP_DB1)
            .with_primary(
                PrimaryHealthBuilder::new()
                    .with_timeline(11)
                    .with_followers(&["db003"])
                    .with_follower_state(ReplicationState::Backup)
                    .build(),
            )
            .build();
        let db2 = primary(2, "db002", IP_DB2, 12);
        let db3 = replica_following(3, "db003", IP_DB1, 11);

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert_eq!(info.true_primary, "db002");
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::PrimaryDoesNotSeeReplica(link)
                if link.primary == "db001" && link.replica == "db003"
        )));
    }

    #[test]
    fn gate_silent_when_sender_host_differs_no_stale_finding() {
        // Replica follows db002 (sender_host=IP_DB2). When the gate iterates
        // db001 as the candidate primary, the replica's sender_host doesn't
        // match — no ReplicaWalReceiverStale should be emitted for db001.
        // That finding is reserved for replicas that *claim* to follow this
        // primary but fail a downstream check.
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary_with_followers(2, "db002", IP_DB2, 12, &["db003"]);
        let db3 = replica_following(3, "db003", IP_DB2, 12);

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        assert!(!info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::ReplicaWalReceiverStale { claimed_sender, .. }
                if claimed_sender == "db001"
        )));
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::BidirectionalFlushingConfirmed(link)
                if link.primary == "db002" && link.replica == "db003"
        )));
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

    #[test]
    fn synchronous_commit_weakened_refuses() {
        let db1 = primary_with_health(
            1,
            "db001",
            IP_DB1,
            PrimaryHealthBuilder::new()
                .with_synchronous_commit("remote_write")
                .build(),
        );
        let db2 = primary(2, "db002", IP_DB2, 12);

        let info = resolve_split_brain(&[&db1, &db2], &[]);

        assert_eq!(info.confidence, Confidence::Refuse);
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::SynchronousCommitWeakened { primary, value }
            if primary == "db001" && value == "remote_write"
        )));
    }

    #[test]
    fn synchronous_commit_on_does_not_refuse() {
        // Both default to synchronous_commit = "on"
        let db1 = primary(1, "db001", IP_DB1, 11);
        let db2 = primary(2, "db002", IP_DB2, 12);

        let info = resolve_split_brain(&[&db1, &db2], &[]);
        assert_ne!(info.confidence, Confidence::Refuse);
    }

    #[test]
    fn primary_with_no_followers_emits_quorum_unsatisfied() {
        let db1 = primary_with_health(
            1,
            "db001",
            IP_DB1,
            PrimaryHealthBuilder::new()
                .with_synchronous_standby_names("ANY 1 (db002, db003")
                .build(),
        );

        let db2 = primary_with_health(
            2,
            "db002",
            IP_DB2,
            PrimaryHealthBuilder::new()
                .with_synchronous_standby_names("ANY 1 (db002, db003)")
                .build(),
        );

        let db3 = replica_following(3, "db003", IP_DB1, 11);

        let info = resolve_split_brain(&[&db1, &db2], &[&db3]);

        // db002 has no followers -> quorum unsatisfied
        assert!(info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::PrimaryQuorumUnsatisfied { primary, required: 1, observed: 0 }
                if primary == "db002"
        )));

        // db001 has db003 following -> quorum satisfied; no finding for db001
        assert!(!info.findings.iter().any(|f| matches!(
            f,
            SplitBrainFinding::PrimaryQuorumUnsatisfied { primary, .. }
                if primary == "db001"
        )));
    }
}
