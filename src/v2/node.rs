use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

const CERT_CLUSTERS_IN_DEV: [&str; 3] = ["dev-pg-app006", "dev-pg-app010", "dev-pg-app011"];

/// Node information from the database portal API.
/// This is the input to the v2 scanning pipeline.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Node {
    pub id: u32,
    pub cluster_id: u32,
    pub node_name: String,
    pub pg_version: String,
    pub ip_address: Ipv4Addr,
}

impl Node {
    pub fn env(&self) -> String {
        self.node_name.split('-').next().unwrap().to_owned()
    }

    pub fn cluster_name(&self) -> String {
        self.node_name
            .split('-')
            .take(3)
            .collect::<Vec<&str>>()
            .join("-")
    }

    pub fn requires_cert(&self) -> bool {
        if self.env() != "dev" {
            return true;
        }

        CERT_CLUSTERS_IN_DEV.contains(&self.cluster_name().as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Node {
        /// Creates a test fixture for a Node with realistic default values
        pub fn fixture() -> Self {
            Self {
                id: 1,
                cluster_id: 10,
                node_name: "prod-pg-app007-db001.sto1.example.com".to_string(),
                pg_version: "15.12".to_string(),
                ip_address: "127.1.17.7".parse().unwrap(),
            }
        }
    }

    #[test]
    fn test_env_from_node_name() {
        let mut node = Node::fixture();

        let actual = node.env();
        let expected = "prod";

        assert_eq!(actual, expected);

        node.node_name = "dev-pg-app001-db001.sto1.example.com".to_string();

        let actual = node.env();
        let expected = "dev";

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_cluster_name_from_node_name_pg() {
        let node = Node::fixture();

        let actual = node.cluster_name();
        let expected = "prod-pg-app007";

        assert_eq!(actual, expected);
    }

    #[test]
    fn cluster_name_from_node_name_ts() {
        let node = Node {
            node_name: "prod-ts-metrics001-db002.sto2.example.com".to_string(),
            ..Node::fixture()
        };

        let actual = node.cluster_name();
        let expected = "prod-ts-metrics001";

        assert_eq!(actual, expected);
    }

    #[test]
    fn test_node_requires_cert_dev_pg_app006() {
        let node = Node {
            node_name: "dev-pg-app006-db001.sto1.example.com".to_string(),
            ..Node::fixture()
        };

        assert!(node.requires_cert());
    }

    #[test]
    fn test_node_requires_cert_dev_pg_app010() {
        let node = Node {
            node_name: "dev-pg-app010-db001.sto2.example.com".to_string(),
            ..Node::fixture()
        };

        assert!(node.requires_cert());
    }

    #[test]
    fn test_node_requires_cert_dev_pg_app011() {
        let node = Node {
            node_name: "dev-pg-app011-db001.sto2.example.com".to_string(),
            ..Node::fixture()
        };

        assert!(node.requires_cert());
    }

    #[test]
    fn test_node_requires_cert_dev_other() {
        let node = Node {
            node_name: "dev-pg-app001-db001.sto1.example.com".to_string(),
            ..Node::fixture()
        };

        assert!(!node.requires_cert());
    }

    #[test]
    fn test_node_requires_cert_other() {
        let node = Node {
            node_name: "acce-pg-app001-db001.sto1.example.com".to_string(),
            ..Node::fixture()
        };

        assert!(node.requires_cert());
    }
}
