use std::net::Ipv4Addr;

use serde::{Deserialize, Serialize};

/// Node information from the database portal API.
/// This is the input to the v2 scanning pipeline.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub struct Node {
    pub id: u32,
    pub cluster_id: u32,
    #[serde(rename = "node_name")]
    pub name: String,
    pub pg_version: String,
    pub ip_address: Ipv4Addr,
}

impl Node {
    pub fn env(&self) -> String {
        self.name.split('-').next().unwrap().to_owned()
    }

    pub fn cluster_name(&self) -> String {
        self.name
            .split('-')
            .take(3)
            .collect::<Vec<&str>>()
            .join("-")
    }

    pub fn requires_cert(&self, dev_cert_clusters: &[String]) -> bool {
        if self.env() != "dev" {
            return true;
        }

        dev_cert_clusters.contains(&self.cluster_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    impl Node {
        /// Creates a test fixture for a Node with realistic default values.
        pub fn fixture() -> Self {
            Self {
                id: 1,
                cluster_id: 10,
                name: "prod-pg-app007-db001.sto1.example.com".to_owned(),
                pg_version: "15.12".to_owned(),
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

        node.name = "dev-pg-app001-db001.sto1.example.com".to_owned();

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
            name: "prod-ts-metrics001-db002.sto2.example.com".to_owned(),
            ..Node::fixture()
        };

        let actual = node.cluster_name();
        let expected = "prod-ts-metrics001";

        assert_eq!(actual, expected);
    }

    #[test]
    fn dev_node_in_listed_cluster_requires_cert() {
        let node = Node {
            name: "dev-pg-app002-db001.sto1.example.com".to_owned(),
            ..Node::fixture()
        };

        assert!(node.requires_cert(&["dev-pg-app002".to_owned()]));
    }

    #[test]
    fn dev_node_in_unlisted_cluster_does_not_require_cert() {
        let node = Node {
            name: "dev-pg-app001-db001.sto1.example.com".to_owned(),
            ..Node::fixture()
        };

        assert!(!node.requires_cert(&["dev-pg-app002".to_owned()]));
    }

    #[test]
    fn non_dev_node_requires_cert_with_empty_list() {
        let node = Node {
            name: "acce-pg-app001-db001.sto1.example.com".to_owned(),
            ..Node::fixture()
        };

        assert!(node.requires_cert(&[]));
    }
}
