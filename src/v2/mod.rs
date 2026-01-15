pub mod analyze;
pub mod cluster;
pub mod db;
pub mod node;
#[cfg(feature = "prometheus")]
pub mod prometheus;
pub mod scan;
pub mod writer;

#[cfg(test)]
mod tests_common {

    use crate::v2::{
        cluster::Cluster,
        db::DbError,
        scan::{
            AnalyzedNode, Role,
            health_check_primary::{PrimaryHealthCheckResult, ReplicationConnection},
            health_check_replica::{LagInfo, ReplicaHealthCheckResult, WalReceiverInfo},
        },
    };

    // TODO: various scenarios where the replication info is misconfigured:
    // db002 is primary and follows itself (db001 and db003 follows db002)
    // db001 is primary, db002 follows db001, db003 follows db002 (replica follows replica)

    // pub enum FailureScenario {
    //     Healthy { failover: bool },
    //     OneReplicaDown,
    //     HighReplicationLag,
    //     StandalonePrimary,
    //     AllReplicasDown,
    //     TwoPrimaries { scenario: TwoPrimariesScenarios },
    //     UnreachableReplicas { reachable: usize },
    //     Unknown,
    // }

    // pub enum TwoPrimariesScenarios {
    //     /// Old primary is up on TL, new primary is up on TL + 1, replica follows new primary on TL + 1
    //     DegradedFailover,
    //     // Old primary is up on TL, new primary is up on TL + 1, replica follows old primary on TL
    //     IgnoredNewTl,
    //     // Old primary is up on TL, new primary is up on TL + 1, replica follows old primary on TL + 1
    //     FollowedOldTlPlus1,
    // }

    pub mod healthy {
        static HEALTHY_CLUSTER_JSON: &str =
            include_str!("../../tests/fixtures/healthy/NON_FAILOVER_CLUSTER.json");

        use crate::v2::cluster::Cluster;

        pub fn non_failover_cluster() -> Cluster {
            serde_json::from_str::<Cluster>(HEALTHY_CLUSTER_JSON).unwrap()
        }
    }

    pub mod unhealthy {
        static DB001_UNREACHABLE_FAILOVER_WITH_REPLICA_JSON: &str = include_str!(
            "../../tests/fixtures/unhealthy/DB001_UNREACHABLE_FAILOVER_WITH_REPLICA.json"
        );
        static DB001_REBUILDING_AFTER_FAILOVER_JSON: &str =
            include_str!("../../tests/fixtures/unhealthy/DB001_REBUILDING_AFTER_FAILOVER.json");
        static CHAINED_REPLICA_JSON: &str =
            include_str!("../../tests/fixtures/unhealthy/CHAINED_REPLICA.json");

        use crate::v2::cluster::Cluster;
        pub fn db001_unreachable_failover_with_replica() -> Cluster {
            serde_json::from_str::<Cluster>(DB001_UNREACHABLE_FAILOVER_WITH_REPLICA_JSON).unwrap()
        }

        pub fn db001_rebuilding_after_failover() -> Cluster {
            serde_json::from_str::<Cluster>(DB001_REBUILDING_AFTER_FAILOVER_JSON).unwrap()
        }

        pub fn chained_replica() -> Cluster {
            serde_json::from_str::<Cluster>(CHAINED_REPLICA_JSON).unwrap()
        }
    }

    impl Clone for Role {
        fn clone(&self) -> Self {
            match self {
                Role::Unknown => Role::Unknown,
                Role::UnknownPrimary => Role::UnknownPrimary,
                Role::UnknownReplica => Role::UnknownReplica,
                Role::Primary { health } => Role::Primary {
                    health: health.clone(),
                },
                Role::Replica { health } => Role::Replica {
                    health: health.clone(),
                },
            }
        }
    }

    impl Clone for DbError {
        fn clone(&self) -> Self {
            use DbError::*;
            match self {
                TlsConnector(e) => TlsConnector(e.clone()),
                Postgres(e) => Postgres(e.clone()),
                Io(e) => Io(e.clone()),
                SerdeJson(e) => SerdeJson(e.clone()),
            }
        }
    }

    impl Clone for Cluster {
        fn clone(&self) -> Self {
            Self {
                id: self.id,
                name: self.name.clone(),
                env: self.env.clone(),
                nodes: self.nodes.clone(),
            }
        }
    }

    impl Clone for PrimaryHealthCheckResult {
        fn clone(&self) -> Self {
            Self {
                timeline_id: self.timeline_id,
                uptime: self.uptime.clone(),
                current_wal_lsn: self.current_wal_lsn.clone(),
                configuration: self.configuration.clone(),
                replication: self.replication.clone(),
                total_db_size_bytes: self.total_db_size_bytes,
            }
        }
    }

    impl Clone for ReplicationConnection {
        fn clone(&self) -> Self {
            Self {
                pid: self.pid,
                usesysid: self.usesysid,
                usename: self.usename.clone(),
                application_name: self.application_name.clone(),
                client_addr: self.client_addr.clone(),
                client_hostname: self.client_hostname.clone(),
                client_port: self.client_port,
                backend_start: self.backend_start,
                backend_xmin: self.backend_xmin.clone(),
                state: self.state.clone(),
                sent_lsn: self.sent_lsn.clone(),
                write_lsn: self.write_lsn.clone(),
                flush_lsn: self.flush_lsn.clone(),
                replay_lsn: self.replay_lsn.clone(),
                write_lag: self.write_lag.clone(),
                flush_lag: self.flush_lag.clone(),
                replay_lag: self.replay_lag.clone(),
                sync_priority: self.sync_priority,
                sync_state: self.sync_state.clone(),
                reply_time: self.reply_time,
            }
        }
    }

    impl Clone for AnalyzedNode {
        fn clone(&self) -> Self {
            Self {
                id: self.id,
                cluster_id: self.cluster_id,
                node_name: self.node_name.clone(),
                pg_version: self.pg_version.clone(),
                ip_address: self.ip_address,
                role: self.role.clone(),
                errors: self.errors.clone(),
            }
        }
    }

    impl Clone for ReplicaHealthCheckResult {
        fn clone(&self) -> Self {
            Self {
                wal_receiver: self.wal_receiver.clone(),
                lag: self.lag.clone(),
                conflicts_by_db: self.conflicts_by_db.clone(),
                configuration: self.configuration.clone(),
            }
        }
    }

    impl Clone for WalReceiverInfo {
        fn clone(&self) -> Self {
            Self {
                pid: self.pid,
                status: self.status.clone(),
                receive_start_lsn: self.receive_start_lsn.clone(),
                receive_start_tli: self.receive_start_tli,
                written_lsn: self.written_lsn.clone(),
                flushed_lsn: self.flushed_lsn.clone(),
                received_tli: self.received_tli,
                last_msg_send_time: self.last_msg_send_time,
                last_msg_receipt_time: self.last_msg_receipt_time,
                latest_end_lsn: self.latest_end_lsn.clone(),
                latest_end_time: self.latest_end_time,
                slot_name: self.slot_name.clone(),
                sender_host: self.sender_host.clone(),
                sender_port: self.sender_port,
                conninfo: self.conninfo.clone(),
            }
        }
    }

    impl Clone for LagInfo {
        fn clone(&self) -> Self {
            Self {
                apply_lag_bytes: self.apply_lag_bytes,
                last_transaction_replay_at: self.last_transaction_replay_at,
            }
        }
    }
}
