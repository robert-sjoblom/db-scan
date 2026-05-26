use crate::v2::analyze::{
    AnalyzedCluster, ClusterHealth, ClusterVerdict, NodeVerdict, Reason, Tier,
};

pub(super) fn classify(analyzed: AnalyzedCluster) -> ClusterHealth {
    let cluster_reason = analyzed.verdict.cluster_verdict.as_ref().map(Reason::from);
    let node_reasons = analyzed.verdict.node_reasons();

    let has_failover = analyzed.verdict.has_failover();

    let max_lag = analyzed.verdict.max_lag();

    let worst = cluster_reason.into_iter().chain(node_reasons).max();

    match worst {
        Some(reason) => match Tier::from(&reason) {
            Tier::Degraded => ClusterHealth::Degraded {
                lag: max_lag,
                cluster: analyzed,
                reason,
            },
            Tier::Critical => ClusterHealth::Critical {
                cluster: analyzed,
                reason,
            },
            Tier::Unknown => ClusterHealth::Unknown {
                reachable_nodes: analyzed.cluster.primaries().count()
                    + analyzed.cluster.replicas().count(),
                cluster: analyzed,
                reason,
            },
        },
        None => ClusterHealth::Healthy {
            failover: has_failover,
            cluster: analyzed,
        },
    }
}

impl From<&ClusterVerdict> for Reason {
    fn from(value: &ClusterVerdict) -> Self {
        match value {
            ClusterVerdict::SplitBrain(..) => Self::SplitBrain,
            ClusterVerdict::WritesBlocked => Self::WritesBlocked,
            ClusterVerdict::WritesUnprotected => Self::WritesUnprotected,
            ClusterVerdict::NoPrimary => Self::NoPrimary,
            ClusterVerdict::NoNodesReachable => Self::NoNodesReachable,
            ClusterVerdict::UnexpectedTopology { .. } => Self::UnexpectedTopology,
        }
    }
}

impl From<&NodeVerdict> for Option<Reason> {
    fn from(value: &NodeVerdict) -> Self {
        match value {
            NodeVerdict::ArchiveFailure { .. } => Some(Reason::ArchiveFailure),
            NodeVerdict::ArchiveLagging { .. } => Some(Reason::ArchiveLagging),
            NodeVerdict::ArchivingDisabled => Some(Reason::ArchivingDisabled),
            NodeVerdict::IsFailoverNode => None,
            NodeVerdict::HighLag { .. } => Some(Reason::HighReplicationLag),
            NodeVerdict::DiskIoErrors { .. } => Some(Reason::DiskIoErrors),
            NodeVerdict::FilesystemErrors { .. } => Some(Reason::FilesystemErrors),
            NodeVerdict::ChainedReplication { .. } => Some(Reason::ChainedReplica),
            NodeVerdict::NotStreaming => Some(Reason::RebuildingReplica), //?
            NodeVerdict::NotInQuorum => Some(Reason::NotInQuorum),
            NodeVerdict::SyncCommitOff => Some(Reason::SyncCommitOff),
            NodeVerdict::Unreachable => Some(Reason::ReducedRedundancy),
        }
    }
}

impl From<&Reason> for Tier {
    fn from(value: &Reason) -> Self {
        match value {
            Reason::ReducedRedundancy
            | Reason::HighReplicationLag
            | Reason::ArchiveLagging
            | Reason::RebuildingReplica
            | Reason::ChainedReplica
            | Reason::NotInQuorum
            | Reason::DiskIoErrors => Self::Degraded,
            Reason::SyncCommitOff
            | Reason::NoPrimary
            | Reason::SplitBrain
            | Reason::WritesBlocked
            | Reason::WritesUnprotected
            | Reason::ArchiveFailure
            | Reason::ArchivingDisabled
            | Reason::FilesystemErrors => Self::Critical,
            Reason::NoNodesReachable | Reason::UnexpectedTopology => Self::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::v2::analyze::{
        ClusterVerdict, NodeVerdict, Reason, SplitBrainResolution, Tier,
        split_brain::{Confidence, SplitBrainInfo},
    };

    // Reasons are picked via `max()` in classify, so PartialOrd defines the
    // severity hierarchy. Each `higher > lower` pair locks one rung of the
    // ladder, from most severe (top) to least severe (bottom). When adding a
    // new Reason, add a pair here so a future re-ordering can't silently
    // demote it.
    /// Canonical severity rank used by `classify`'s `max()` selection.
    /// Higher number = more severe.
    ///
    /// The match is exhaustive, so adding a new `Reason` variant without
    /// assigning it a rank here is a compile error. `Unknown`-tier reasons
    /// are never compared against `Critical`/`Degraded` ones by `classify`,
    /// so their ranks are arbitrary.
    fn severity_rank(r: Reason) -> u8 {
        match r {
            // Unknown tier (rank arbitrary; not compared with other tiers).
            Reason::NoNodesReachable => 0,
            Reason::UnexpectedTopology => 1,
            // Degraded tier, least → most severe.
            Reason::DiskIoErrors => 10,
            Reason::NotInQuorum => 11,
            Reason::ChainedReplica => 12,
            Reason::RebuildingReplica => 13,
            Reason::HighReplicationLag => 14,
            Reason::ArchiveLagging => 15,
            Reason::ReducedRedundancy => 16,
            // Critical tier, least → most severe.
            Reason::SyncCommitOff => 20,
            Reason::ArchivingDisabled => 21,
            Reason::ArchiveFailure => 22,
            Reason::FilesystemErrors => 23,
            Reason::WritesUnprotected => 24,
            Reason::WritesBlocked => 25,
            Reason::NoPrimary => 26,
            Reason::SplitBrain => 27,
        }
    }

    #[rstest]
    #[case::writes_blocked(ClusterVerdict::WritesBlocked, Reason::WritesBlocked)]
    #[case::writes_unprotected(ClusterVerdict::WritesUnprotected, Reason::WritesUnprotected)]
    #[case::no_primary(ClusterVerdict::NoPrimary, Reason::NoPrimary)]
    #[case::no_nodes_reachable(ClusterVerdict::NoNodesReachable, Reason::NoNodesReachable)]
    #[case::unexpected_topology(ClusterVerdict::UnexpectedTopology { replica_count: 5 }, Reason::UnexpectedTopology)]
    fn cluster_verdict_to_reason(#[case] input: ClusterVerdict, #[case] expected: Reason) {
        assert_eq!(Reason::from(&input), expected);
    }

    #[test]
    fn cluster_verdict_split_brain_to_reason() {
        let info = SplitBrainInfo {
            true_primary: "n1".into(),
            stale_primaries: vec!["n2".into()],
            resolution: SplitBrainResolution::Indeterminate,
            confidence: Confidence::BestEffort,
            findings: vec![],
        };
        assert_eq!(
            Reason::from(&ClusterVerdict::SplitBrain(info)),
            Reason::SplitBrain
        );
    }

    #[rstest]
    #[case::archive_failure(NodeVerdict::ArchiveFailure { failed_count: 1, last_wal: None, last_failed_at: None }, Some(Reason::ArchiveFailure))]
    #[case::archive_lagging(NodeVerdict::ArchiveLagging { failed_count: 1, last_wal: None, last_failed_at: None, last_archived_at: None }, Some(Reason::ArchiveLagging))]
    #[case::archiving_disabled(NodeVerdict::ArchivingDisabled, Some(Reason::ArchivingDisabled))]
    #[case::sync_commit_off(NodeVerdict::SyncCommitOff, Some(Reason::SyncCommitOff))]
    #[case::high_lag(NodeVerdict::HighLag { bytes: 100 }, Some(Reason::HighReplicationLag))]
    #[case::disk_io_errors(NodeVerdict::DiskIoErrors { io: 1, block: 0 }, Some(Reason::DiskIoErrors))]
    #[case::filesystem_errors(NodeVerdict::FilesystemErrors { count: 1 }, Some(Reason::FilesystemErrors))]
    #[case::chained_replication(NodeVerdict::ChainedReplication { upstream: "x".into() }, Some(Reason::ChainedReplica))]
    #[case::not_streaming(NodeVerdict::NotStreaming, Some(Reason::RebuildingReplica))]
    #[case::not_in_quorum(NodeVerdict::NotInQuorum, Some(Reason::NotInQuorum))]
    #[case::unreachable(NodeVerdict::Unreachable, Some(Reason::ReducedRedundancy))]
    #[case::is_failover_node(NodeVerdict::IsFailoverNode, None)]
    fn node_verdict_to_reason(#[case] input: NodeVerdict, #[case] expected: Option<Reason>) {
        assert_eq!(Option::<Reason>::from(&input), expected);
    }

    #[rstest]
    #[case::one_replica_down(Reason::ReducedRedundancy, Tier::Degraded)]
    #[case::high_replication_lag(Reason::HighReplicationLag, Tier::Degraded)]
    #[case::rebuilding_replica(Reason::RebuildingReplica, Tier::Degraded)]
    #[case::chained_replica(Reason::ChainedReplica, Tier::Degraded)]
    #[case::not_in_quorum(Reason::NotInQuorum, Tier::Degraded)]
    #[case::disk_io_errors(Reason::DiskIoErrors, Tier::Degraded)]
    #[case::no_primary(Reason::NoPrimary, Tier::Critical)]
    #[case::split_brain(Reason::SplitBrain, Tier::Critical)]
    #[case::writes_blocked(Reason::WritesBlocked, Tier::Critical)]
    #[case::writes_unprotected(Reason::WritesUnprotected, Tier::Critical)]
    #[case::archive_failure(Reason::ArchiveFailure, Tier::Critical)]
    #[case::archive_lagging(Reason::ArchiveLagging, Tier::Degraded)]
    #[case::archiving_disabled(Reason::ArchivingDisabled, Tier::Critical)]
    #[case::filesystem_errors(Reason::FilesystemErrors, Tier::Critical)]
    #[case::sync_commit_off(Reason::SyncCommitOff, Tier::Critical)]
    #[case::no_nodes_reachable(Reason::NoNodesReachable, Tier::Unknown)]
    #[case::unexpected_topology(Reason::UnexpectedTopology, Tier::Unknown)]
    fn reason_to_tier(#[case] input: Reason, #[case] expected: Tier) {
        assert_eq!(Tier::from(&input), expected);
    }

    #[test]
    fn reason_ordering_matches_severity_rank() {
        use strum::IntoEnumIterator as _;

        for a in Reason::iter() {
            for b in Reason::iter() {
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
}
