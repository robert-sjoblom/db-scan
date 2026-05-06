use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::instrument;

use crate::v2::node::Node;

const ONE_DAY_IN_SECONDS: u64 = 86_400;
const CACHE_PATH: &str = "/tmp/nodes_response.json";
const DATABASE_PORTAL_URL: &str = match option_env!("DATABASE_PORTAL_URL") {
    Some(url) => url,
    None => "https://database.example.com/api/v1/nodes",
};

#[derive(Debug, Serialize, Deserialize)]
struct NodesResponse {
    pub items: Vec<serde_json::Value>,
    pub count: i32,
}

fn parse_nodes(items: Vec<serde_json::Value>) -> Vec<Node> {
    items
        .into_iter()
        .filter_map(|v| match serde_json::from_value::<Node>(v.clone()) {
            Ok(node) => Some(node),
            Err(e) => {
                tracing::warn!(error = %e, raw = %v, "skipping node that failed to deserialize");
                None
            }
        })
        .collect()
}

#[instrument(skip_all, level = "INFO")]
pub(crate) async fn nodes() -> Result<Vec<Node>, DbPortalErrors> {
    // Check if cache exists and is less than 1 day old
    let use_cache = std::fs::metadata(CACHE_PATH)
        .ok()
        .and_then(|m| m.modified().ok())
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|e| e.as_secs() < ONE_DAY_IN_SECONDS);

    if use_cache && let Ok(json) = std::fs::read_to_string(CACHE_PATH) {
        tracing::info!(source = "file", "reading nodes from cache");
        let nodes_response: NodesResponse = serde_json::from_str(&json)?;
        tracing::info!(
            node_count = nodes_response.count,
            source = "file",
            "loaded nodes from cache"
        );
        return Ok(parse_nodes(nodes_response.items));
    }

    tracing::info!(
        source = "api",
        url = DATABASE_PORTAL_URL,
        "fetching nodes from API"
    );
    let client = reqwest::Client::new();
    let response = client.get(DATABASE_PORTAL_URL).send().await?;

    let nodes_response: NodesResponse = response.json().await?;

    // Write the response to a json file in /tmp
    let json = serde_json::to_string(&nodes_response)?;
    std::fs::write(CACHE_PATH, &json)?;
    tracing::info!(file = CACHE_PATH, "cached nodes to file");

    tracing::info!(
        node_count = nodes_response.count,
        source = "api",
        "fetched nodes from API"
    );
    Ok(parse_nodes(nodes_response.items))
}

#[derive(Error, Debug)]
pub enum DbPortalErrors {
    #[error("HTTP request failed: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("JSON serialization/deserialization failed: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("IO operation failed: {0}")]
    Io(#[from] std::io::Error),
}
