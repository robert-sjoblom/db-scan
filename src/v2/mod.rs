pub mod analyze;
pub mod cluster;
pub mod db;
pub mod node;
pub mod scan;
pub mod writer;

#[cfg(test)]
pub(crate) mod tests_common {
    use std::{collections::HashMap, net::Ipv4Addr};

    use chrono::Utc;

    use crate::v2::{
        cluster::Cluster,
        scan::{
            AnalyzedNode, Role,
            health_check_primary::{
                ArchiverStats, PgSyncSettings, PrimaryHealthCheckResult, ReplicationConnection,
                ReplicationSlot,
            },
            health_check_replica::{LagInfo, ReplicaHealthCheckResult, WalReceiverInfo},
        },
    };

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

    // ==================== Test fixture builders ====================

    pub struct PrimaryHealthBuilder {
        replication_count: usize,
        replay_lag: Option<String>,
        configuration: HashMap<String, String>,
        timeline_id: i32,
        archiver: ArchiverStats,
        replication_slots: Vec<ReplicationSlot>,
    }

    impl PrimaryHealthBuilder {
        pub fn new() -> Self {
            Self {
                replication_count: 0,
                replay_lag: None,
                configuration: HashMap::new(),
                timeline_id: 11,
                archiver: ArchiverStats {
                    archived_count: 0,
                    failed_count: 0,
                    last_archived_wal: None,
                    last_archived_time: None,
                    last_failed_wal: None,
                    last_failed_time: None,
                },
                replication_slots: vec![],
            }
        }

        pub fn with_replication(mut self, count: usize) -> Self {
            self.replication_count = count;
            self
        }

        pub fn with_lag(mut self, lag: &str) -> Self {
            self.replay_lag = Some(lag.to_string());
            self
        }

        pub fn with_config(mut self, configuration: HashMap<String, String>) -> Self {
            self.configuration = configuration;
            self
        }

        pub fn with_timeline(mut self, id: i32) -> Self {
            self.timeline_id = id;
            self
        }

        pub fn with_archiver(mut self, archiver: ArchiverStats) -> Self {
            self.archiver = archiver;
            self
        }

        pub fn build(self) -> PrimaryHealthCheckResult {
            let base_lsn = "48F/6957B540";
            let has_high_lag = self.replay_lag.as_ref().is_some_and(|lag| {
                parse_lag_seconds(lag)
                    .map(|seconds| seconds >= 5)
                    .unwrap_or(false)
            });

            let lagging_lsn = if has_high_lag {
                "48F/6357B540" // ~100MB behind
            } else {
                base_lsn
            };

            let replication: Vec<ReplicationConnection> = (0..self.replication_count)
                .map(|i| ReplicationConnection {
                    pid: 1000 + i as i32,
                    usesysid: 16387,
                    usename: "replicator".to_string(),
                    application_name: format!("dev_pg_app001_db00{}", i + 2),
                    client_addr: Some(format!("10.8{}.12.151", i + 2)),
                    client_hostname: None,
                    client_port: Some(63512 + i as i32),
                    backend_start: Utc::now(),
                    backend_xmin: Some("621647066".to_string()),
                    state: "streaming".to_string(),
                    sent_lsn: Some(base_lsn.to_string()),
                    write_lsn: Some(lagging_lsn.to_string()),
                    flush_lsn: Some(lagging_lsn.to_string()),
                    replay_lsn: Some(lagging_lsn.to_string()),
                    write_lag: Some("00:00:00.000354".to_string()),
                    flush_lag: Some("00:00:00.000895".to_string()),
                    replay_lag: self.replay_lag.clone(),
                    sync_priority: 1,
                    sync_state: PgSyncSettings::Quorum,
                    reply_time: Some(Utc::now()),
                })
                .collect();

            PrimaryHealthCheckResult {
                timeline_id: self.timeline_id,
                uptime: "26 days 14:39:06.703824".to_string(),
                current_wal_lsn: base_lsn.to_string(),
                configuration: self.configuration,
                replication,
                archiver: self.archiver,
                replication_slots: self.replication_slots,
            }
        }
    }

    impl Default for PrimaryHealthBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

    fn parse_lag_seconds(lag: &str) -> Option<u64> {
        let parts: Vec<&str> = lag.split(':').collect();
        if parts.len() != 3 {
            return None;
        }
        let hours: u64 = parts[0].parse().ok()?;
        let minutes: u64 = parts[1].parse().ok()?;
        let seconds_parts: Vec<&str> = parts[2].split('.').collect();
        let seconds: u64 = seconds_parts[0].parse().ok()?;
        Some(hours * 3600 + minutes * 60 + seconds)
    }

    pub struct ReplicaHealthBuilder {
        timeline_id: i32,
        sender_host: String,
        has_wal_receiver: bool,
    }

    #[allow(dead_code)]
    impl ReplicaHealthBuilder {
        pub fn new() -> Self {
            Self {
                timeline_id: 11,
                sender_host: "127.1.12.151".to_string(),
                has_wal_receiver: true,
            }
        }

        pub fn with_timeline(mut self, id: i32) -> Self {
            self.timeline_id = id;
            self
        }

        pub fn with_sender_host(mut self, host: &str) -> Self {
            self.sender_host = host.to_string();
            self
        }

        pub fn without_wal_receiver(mut self) -> Self {
            self.has_wal_receiver = false;
            self
        }

        pub fn build(self) -> ReplicaHealthCheckResult {
            let wal_receiver = if self.has_wal_receiver {
                Some(WalReceiverInfo {
                    pid: 4053449,
                    status: "streaming".to_string(),
                    receive_start_lsn: "47F/67000000".to_string(),
                    receive_start_tli: self.timeline_id,
                    written_lsn: "48F/6957B540".to_string(),
                    flushed_lsn: "48F/6957B540".to_string(),
                    received_tli: self.timeline_id,
                    last_msg_send_time: Some(Utc::now()),
                    last_msg_receipt_time: Some(Utc::now()),
                    latest_end_lsn: "48F/6957B540".to_string(),
                    latest_end_time: Some(Utc::now()),
                    slot_name: None,
                    sender_host: self.sender_host,
                    sender_port: 5432,
                    conninfo: "user=replicator host=127.1.12.151".to_string(),
                })
            } else {
                None
            };

            ReplicaHealthCheckResult {
                timeline_id: self.timeline_id,
                wal_receiver,
                lag: LagInfo {
                    apply_lag_bytes: Some(0),
                    last_transaction_replay_at: Some(Utc::now()),
                },
                conflicts_by_db: HashMap::new(),
                configuration: HashMap::new(),
            }
        }
    }

    impl Default for ReplicaHealthBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

    pub struct NodeBuilder {
        id: u32,
        cluster_id: u32,
        node_name: String,
        ip_address: Ipv4Addr,
        role: Role,
    }

    #[allow(dead_code)]
    impl NodeBuilder {
        pub fn new(name: &str) -> Self {
            Self {
                id: 1,
                cluster_id: 33,
                node_name: name.to_string(),
                ip_address: Ipv4Addr::new(10, 81, 12, 151),
                role: Role::Unknown,
            }
        }

        pub fn with_id(mut self, id: u32) -> Self {
            self.id = id;
            self
        }

        pub fn with_ip(mut self, ip: Ipv4Addr) -> Self {
            self.ip_address = ip;
            self
        }

        pub fn with_primary(mut self, health: PrimaryHealthCheckResult) -> Self {
            self.role = Role::Primary {
                health: Box::new(health),
            };
            self
        }

        pub fn with_replica(mut self, health: ReplicaHealthCheckResult) -> Self {
            self.role = Role::Replica {
                health: Box::new(health),
            };
            self
        }

        pub fn with_unknown(mut self) -> Self {
            self.role = Role::Unknown;
            self
        }

        pub fn build(self) -> AnalyzedNode {
            AnalyzedNode {
                id: self.id,
                cluster_id: self.cluster_id,
                node_name: self.node_name,
                pg_version: "15.14".to_string(),
                ip_address: self.ip_address,
                role: self.role,
                errors: vec![],
                disk_check: None,
            }
        }

        pub fn build_with_role(mut self, role: Role) -> AnalyzedNode {
            self.role = role;
            self.build()
        }
    }

    pub struct ClusterBuilder {
        id: u32,
        name: String,
        env: String,
        nodes: Vec<AnalyzedNode>,
    }

    impl ClusterBuilder {
        pub fn new(name: &str) -> Self {
            let env = name.split('-').next().unwrap_or("dev").to_string();
            Self {
                id: 33,
                name: name.to_string(),
                env,
                nodes: vec![],
            }
        }

        pub fn with_nodes(mut self, nodes: Vec<AnalyzedNode>) -> Self {
            self.nodes = nodes;
            self
        }

        pub fn build(self) -> Cluster {
            Cluster {
                id: self.id,
                name: self.name,
                env: self.env,
                nodes: self.nodes,
            }
        }
    }
}
